# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Bounded in-memory sort.

This operator provides a pragmatic way to impose local ordering in streaming
pipelines. It should be used with care: it buffers elements up to a configured
limit to bound memory usage.

Arguments:
  - key: str = "meta.ts"          (dotted path; compared as-is)
  - max_buffer: int = 1000        (flush when this many are buffered)
  - ascending: bool = True
  - stable: bool = True           (keep original order among equal keys)
  - flush_on_batch: bool = True   (when input is a Batch → sort & flush)

Behavior:
  - Item stream: buffer incoming Items; when buffer reaches `max_buffer`,
    emit a Batch with items sorted by `key`. Returns None otherwise.
  - Batch: if `flush_on_batch=True`, sort per-batch and return a sorted Batch.
  - On `close()`, leftovers are dropped (documented limitation); choose a
    reasonable `max_buffer`/chunking upstream if you need exact delivery.
"""

from typing import Any

from ....api.adapters import AdapterContext, Capabilities, TransformOp
from ....api.streams import Batch, ContentKind, Item


def _get_dotted(obj: Any, path: str) -> Any:
    if not isinstance(obj, dict):
        return None
    cur: Any = obj
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return None
    return cur


class SortBounded(TransformOp):
    name = "sort.bounded"
    capabilities = Capabilities(
        accepts=frozenset({ContentKind.RECORD}),
        emits=frozenset({ContentKind.RECORD}),
        bounded_memory=False,  # bounded by max_buffer but keeps state
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._key = str(kwargs.get("key", "meta.ts"))
        self._max = max(1, int(kwargs.get("max_buffer", 1000)))
        self._ascending = bool(kwargs.get("ascending", True))
        self._stable = bool(kwargs.get("stable", True))
        self._flush_on_batch = bool(kwargs.get("flush_on_batch", True))
        self._buf: list[Item] = []

    async def open(self, ctx: AdapterContext, **args: Any) -> None:
        return

    async def process(self, x: Item | Batch):
        if isinstance(x, Batch) and x.items is not None:
            if not self._flush_on_batch:
                return x
            return self._sorted_batch(x.items)

        if isinstance(x, Item):
            self._buf.append(x)
            if len(self._buf) >= self._max:
                items = self._flush()
                return Batch(items=items, frame=None)
            return None

        return None

    async def close(self) -> None:
        # Cannot emit at close (engine does not consume close() outputs).
        self._buf.clear()

    # ---- helpers -------------------------------------------------------------

    def _key_of(self, it: Item, idx: int) -> tuple[Any, int]:
        # Return (key, idx) for stable sorting when requested.
        v = None
        if self._key.startswith("meta."):
            v = _get_dotted(it.meta, self._key.removeprefix("meta."))
        elif self._key.startswith("payload."):
            if isinstance(it.payload, dict):
                v = _get_dotted(it.payload, self._key.removeprefix("payload."))
            else:
                v = None
        else:
            # try meta first
            v = it.meta.get(self._key)
            if v is None and isinstance(it.payload, dict):
                v = it.payload.get(self._key)
        return (v, idx) if self._stable else (v, 0)

    def _sorted_batch(self, items: list[Item]) -> Batch:
        keyed = [(self._key_of(it, i), it) for i, it in enumerate(items)]
        keyed.sort(key=lambda kv: kv[0], reverse=not self._ascending)
        out = [it for _, it in keyed]
        return Batch(items=out, frame=None)

    def _flush(self) -> list[Item]:
        items = list(self._buf)
        self._buf.clear()
        return self._sorted_batch(items).items or []
