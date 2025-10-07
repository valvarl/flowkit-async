# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Simple artifacts sink.

This sink is intentionally minimal and avoids coupling to the internal storage
facade, so it can be used in unit/e2e tests without storage providers.

Behavior:
  - If `directory` is provided (or `local_dir`), files are written under that
    directory with a generated name (uses idempotency key when available).
  - Otherwise, if `ctx.externals["artifacts"]` provides a callable `put(blob, *,
    key, meta)` or `write(...)`, we attempt to call it best-effort.
  - Item/Batch handling:
      * Batch with blob → write as-is.
      * Batch with items → newline-delimited JSON (UTF-8).
      * Item: record → JSON; bytes → raw.

This is **not** meant for high-performance archival, but is good enough for
MVP/e2e pipelines and local development.
"""

import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any

from ....api.adapters import AdapterContext, Capabilities, SinkAdapter
from ....api.streams import Batch, ContentKind, FrameDescriptor, Item


class ArtifactsSink(SinkAdapter):
    name = "push.to_artifacts"
    capabilities = Capabilities(
        accepts=frozenset(
            {
                ContentKind.RECORD,
                ContentKind.BYTES,
                ContentKind.FILE,
                ContentKind.REF,
            }
        ),
        emits=frozenset(),
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._dir: Path | None = None
        self._ext = (ctx.externals or {}).get("artifacts")

    async def open(self, ctx: AdapterContext, **spec: Any) -> None:
        directory = spec.get("directory") or spec.get("local_dir")
        if directory:
            self._dir = Path(directory)
            self._dir.mkdir(parents=True, exist_ok=True)
        # keep externals as-is; we call them lazily in send()

    async def send(self, x: Item | Batch, *, idempotency_key: str | None = None) -> None:
        key = self._make_key(idempotency_key)
        blob, frame = self._to_blob(x)

        # Prefer local directory when provided
        if self._dir is not None:
            path = self._dir / key
            await _async_write(path, blob)
            return

        # Try externals facade (best-effort)
        if self._ext:
            try:
                put = getattr(self._ext, "put", None) or getattr(self._ext, "write", None)
                if callable(put):
                    res = put(blob, key=key, meta={"frame": frame.type or frame.kind.value})
                    if hasattr(res, "__await__"):
                        await res  # type: ignore[func-returns-value]
                    return
            except Exception:
                # fall through to logging; artifacts writing is best-effort here
                pass

        # Fallback: log a warning (to avoid silently dropping data)
        if self._ctx and getattr(self._ctx, "logger", None):
            self._ctx.logger.warning("artifacts.sink.no_target", key=key, size=len(blob))

    async def flush(self) -> None:
        """No buffered state; nothing to flush."""
        return

    async def close(self) -> None:
        """No dedicated resources; noop."""
        return

    # ---- helpers -------------------------------------------------------------

    def _make_key(self, idem: str | None) -> str:
        ts = int(time.time() * 1000)
        base = idem or str(uuid.uuid4())
        suffix = ".bin"
        fmt = self._args.get("filename_template") or "{ts}-{id}.bin"
        try:
            return fmt.format(ts=ts, id=base, node=self._ctx.node_id, task=self._ctx.task_id)
        except Exception:
            return f"{ts}-{base}{suffix}"

    def _to_blob(self, x: Item | Batch) -> tuple[bytes, FrameDescriptor]:
        if isinstance(x, Batch):
            if x.blob is not None:
                blob = bytes(x.blob)
                frame = x.frame or FrameDescriptor(kind=ContentKind.BYTES)
                return blob, frame
            if x.items is not None:
                # JSONL
                lines = []
                for it in x.items:
                    if isinstance(it, Item):
                        payload = it.payload
                    else:
                        payload = getattr(it, "payload", it)
                    lines.append(json.dumps(payload, ensure_ascii=False))
                blob = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
                return blob, FrameDescriptor(kind=ContentKind.BYTES, type="jsonl", encoding="utf-8")
        # Single item
        if isinstance(x, Item):
            if isinstance(x.payload, (bytes, bytearray, memoryview)):
                return bytes(x.payload), FrameDescriptor(kind=ContentKind.BYTES)
            return json.dumps(x.payload, ensure_ascii=False).encode("utf-8"), FrameDescriptor(
                kind=ContentKind.BYTES, type="json", encoding="utf-8"
            )
        raise TypeError("unsupported element for artifacts sink")


async def _async_write(path: Path, data: bytes) -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, path.write_bytes, data)
