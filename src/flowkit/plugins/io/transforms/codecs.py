# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Best-effort JSON Lines codec implemented as transform operators.

We intentionally keep this simple and self-contained (no global codec registry):
  - ToJsonLines:  Item/Batch → framed Batch (blob bytes, frame.type="jsonl")
  - FromJsonLines: framed Batch (jsonl) → Batch of Items (records)

Typical uses:
  - To pre-encode records for object storage or file-like sinks.
  - To decode downloaded blobs back into streaming records.

Arguments (ToJsonLines):
  - chunk_size: int = 0        (0=encode per incoming element; >0 → accumulate)
  - ensure_ascii: bool = False
  - newline: str = "\n"

Arguments (FromJsonLines):
  - encoding: str = "utf-8"
  - skip_blank: bool = True
"""

import json
from collections.abc import Iterable
from typing import Any

from ....api.adapters import AdapterContext, Capabilities, TransformOp
from ....api.streams import Batch, ContentKind, FrameDescriptor, Item
from ....runtime.stream.frames import normalize_batch


def _json_dumps(obj: Any, ensure_ascii: bool) -> str:
    return json.dumps(obj, ensure_ascii=ensure_ascii)


class ToJsonLines(TransformOp):
    name = "codecs.to_jsonl"
    capabilities = Capabilities(
        accepts=frozenset({ContentKind.RECORD}),
        emits=frozenset({ContentKind.BYTES}),
        bounded_memory=True,
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._buf: list[str] = []
        self._emit_n = int(kwargs.get("chunk_size", 0))
        self._ensure_ascii = bool(kwargs.get("ensure_ascii", False))
        self._newline: str = str(kwargs.get("newline", "\n"))

    async def open(self, ctx: AdapterContext, **args: Any) -> None:
        return

    async def process(self, x: Item | Batch) -> Batch | Iterable[Batch] | None:
        # Batch with items → encode all at once
        if isinstance(x, Batch) and x.items is not None:
            lines = [
                _json_dumps(it.payload if isinstance(it, Item) else getattr(it, "payload", it), self._ensure_ascii)
                for it in x.items
            ]
            blob = (self._newline.join(lines) + (self._newline if lines else "")).encode("utf-8")
            return normalize_batch(
                Batch(
                    items=None,
                    frame=FrameDescriptor(kind=ContentKind.BYTES, type="jsonl", encoding="utf-8"),
                    blob=blob,
                ),
                frame=FrameDescriptor(kind=ContentKind.BYTES, type="jsonl", encoding="utf-8"),
            )

        # Single item
        if isinstance(x, Item):
            line = _json_dumps(x.payload, self._ensure_ascii)
            if self._emit_n <= 1:
                blob = (line + self._newline).encode("utf-8")
                return normalize_batch(
                    blob,
                    frame=FrameDescriptor(kind=ContentKind.BYTES, type="jsonl", encoding="utf-8"),
                )
            # accumulate
            self._buf.append(line)
            if len(self._buf) >= self._emit_n:
                blob = (self._newline.join(self._buf) + self._newline).encode("utf-8")
                self._buf.clear()
                return normalize_batch(
                    blob,
                    frame=FrameDescriptor(kind=ContentKind.BYTES, type="jsonl", encoding="utf-8"),
                )
            return None

        # Framed blob or ref is not handled here
        return None

    async def close(self) -> None:
        # Emit leftovers as a final batch if chunking is used.
        # Note: the engine doesn't pull outputs from `close()`, so we can't emit here.
        self._buf.clear()
        return


class FromJsonLines(TransformOp):
    name = "codecs.from_jsonl"
    capabilities = Capabilities(
        accepts=frozenset({ContentKind.BYTES}),
        emits=frozenset({ContentKind.RECORD}),
        bounded_memory=True,
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._encoding = str(kwargs.get("encoding", "utf-8"))
        self._skip_blank = bool(kwargs.get("skip_blank", True))

    async def open(self, ctx: AdapterContext, **args: Any) -> None:
        return

    async def process(self, x: Item | Batch):
        if isinstance(x, Batch) and x.blob is not None:
            text = bytes(x.blob).decode(self._encoding, errors="ignore")
            items: list[Item] = []
            for line in text.splitlines():
                if (not line) and self._skip_blank:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    # Skip malformed lines (best effort).
                    continue
                items.append(Item(payload=obj))
            if not items:
                return None
            return Batch(items=items, frame=None)
        # decoding from raw Item bytes is not handled (use a codec-aware input)
        return None

    async def close(self) -> None:
        return
