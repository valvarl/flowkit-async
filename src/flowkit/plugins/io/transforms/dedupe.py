# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Duplicate suppression operator with time-bounded window.

Arguments:
  - key: str                      (dotted path, e.g., "meta.id" or "payload.id")
  - window: str = "10m"           (ttl; supports "ms","s","m","h")
  - max_keys: int = 100_000       (upper bound to avoid unbounded memory)

Semantics:
  - For each element, extract the key. If it was seen within the TTL window,
    drop the element (return None). Otherwise store the timestamp and pass it.

Notes:
  - The operator is approximate and in-memory. It uses LRU-style eviction when
    `max_keys` is exceeded.
  - When processing Batches, it deduplicates per-item and returns a filtered Batch.
"""

import time
from collections import OrderedDict
from collections.abc import Hashable
from typing import Any

from ....api.adapters import AdapterContext, Capabilities, TransformOp
from ....api.streams import Batch, ContentKind, Item


def _parse_ttl(text: str) -> float:
    text = text.strip().lower()
    if text.endswith("ms"):
        return max(0.0, float(text[:-2]) / 1000.0)
    if text.endswith("s"):
        return max(0.0, float(text[:-1]))
    if text.endswith("m"):
        return max(0.0, float(text[:-1]) * 60.0)
    if text.endswith("h"):
        return max(0.0, float(text[:-1]) * 3600.0)
    try:
        return max(0.0, float(text))
    except Exception:
        return 600.0  # default 10m


def _get_dotted(obj: Any, path: str) -> Any:
    cur: Any = obj
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return None
    return cur


class Dedupe(TransformOp):
    name = "dedupe"
    capabilities = Capabilities(
        accepts=frozenset({ContentKind.RECORD}),
        emits=frozenset({ContentKind.RECORD}),
        bounded_memory=False,  # bounded by max_keys but keeps state
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._key_path = str(kwargs.get("key", "meta.id"))
        self._ttl = _parse_ttl(str(kwargs.get("window", "10m")))
        self._max = int(kwargs.get("max_keys", 100_000))
        # OrderedDict acts as an LRU eviction list: key -> last_seen_ts
        self._seen: OrderedDict[Hashable, float] = OrderedDict()

    async def open(self, ctx: AdapterContext, **args: Any) -> None:
        return

    async def process(self, x: Item | Batch) -> Item | Batch | None:
        if isinstance(x, Batch):
            # Process per-item; keep only uniques
            out: list[Item] = []
            for it in x.items or []:
                key = self._extract_key(it)
                if key is None:
                    out.append(it)
                    continue
                if self._should_pass(key):
                    out.append(it)
            if not out:
                return None
            return Batch(items=out, frame=None)

        # Single item
        key = self._extract_key(x)
        if key is None:
            return x
        return x if self._should_pass(key) else None

    async def close(self) -> None:
        self._seen.clear()

    # ---- helpers -------------------------------------------------------------

    def _extract_key(self, it: Item) -> Hashable | None:
        """
        Resolve the dedupe key for an Item. Supports:
          - meta.* paths
          - payload.* paths
          - plain single-level key present in meta or payload
        """
        if self._key_path.startswith("meta."):
            return _get_dotted(it.meta, self._key_path.removeprefix("meta."))
        if self._key_path.startswith("payload."):
            # payload should be dict-like for dotted lookup; otherwise fallback
            if isinstance(it.payload, dict):
                return _get_dotted(it.payload, self._key_path.removeprefix("payload."))
            return None
        # Try meta first, then payload
        if isinstance(it.meta, dict) and self._key_path in it.meta:
            return it.meta.get(self._key_path)
        if isinstance(it.payload, dict) and self._key_path in it.payload:
            return it.payload.get(self._key_path)
        return None

    def _should_pass(self, key: Hashable) -> bool:
        """
        Return True if the element with `key` should be passed downstream
        (not seen within TTL window). Updates internal LRU state.
        """
        now = time.monotonic()
        # Prune stale entries first (cheap incremental prune)
        self._prune(now)

        last = self._seen.get(key)
        if last is not None and (now - last) <= self._ttl:
            # Seen recently → drop
            return False

        # Record as (re)seen and move to end (most recent)
        self._seen[key] = now
        self._seen.move_to_end(key, last=False)  # newest to the left (arbitrary)
        # Enforce capacity
        self._evict_if_needed()
        return True

    def _prune(self, now: float) -> None:
        if not self._seen:
            return
        ttl = self._ttl
        # Pop from the right while expired
        to_delete: list[Hashable] = []
        for k, ts in list(self._seen.items())[::-1]:
            if (now - ts) > ttl:
                to_delete.append(k)
            else:
                break
        for k in to_delete:
            try:
                self._seen.pop(k, None)
            except Exception:
                pass

    def _evict_if_needed(self) -> None:
        while len(self._seen) > self._max:
            try:
                self._seen.popitem(last=True)  # pop LRU (rightmost)
            except Exception:
                break
