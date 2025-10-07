# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Object storage sink (S3 for MVP).

This sink uploads each element as an object to S3 using aioboto3. It supports:
  - raw bytes (framed batch blob or bytes item)
  - JSON record (auto-serialized to UTF-8)
  - JSONL batch (when receiving Batch with items; see also codecs.to_jsonl)

Args:
  - bucket: str (required)
  - key_template: str, e.g. "flowkit/{node}/{task}/{ts}-{idem}.bin"
  - content_type: Optional[str]
  - jsonl_for_batches: bool = True  (encode Batch.items as JSONL when True)
  - s3_client_kwargs: dict = {}     (passed to aioboto3.client("s3", **kwargs))

Key template placeholders:
  - {node}, {task}, {ts} (ms), {idem}, {rand}
"""

import json
import time
import uuid
from typing import Any

try:
    import aioboto3 as _s3lib  # type: ignore
except Exception:  # pragma: no cover
    _s3lib = None

from ....api.adapters import AdapterContext, Capabilities, SinkAdapter
from ....api.streams import Batch, ContentKind, Item


class ObjectStorageSink(SinkAdapter):
    name = "emit.s3"
    capabilities = Capabilities(
        accepts=frozenset({ContentKind.RECORD, ContentKind.BYTES}),
        emits=frozenset(),
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._session = None
        self._client = None

    async def open(self, ctx: AdapterContext, **spec: Any) -> None:
        if _s3lib is None:
            raise RuntimeError("emit.s3 requires aioboto3; please install it")
        self._args = {**self._args, **spec}
        self._bucket: str = self._args.get("bucket")
        if not self._bucket:
            raise ValueError("emit.s3 requires 'bucket'")
        self._session = _s3lib.Session()
        self._client = await self._session.client("s3").__aenter__()  # type: ignore[assignment]

    async def send(self, x: Item | Batch, *, idempotency_key: str | None = None) -> None:
        if self._client is None:
            raise RuntimeError("emit.s3 is not opened")

        key = self._render_key(idempotency_key)
        body, content_type = self._to_blob(x)

        extra_args = {}
        if ct := (self._args.get("content_type") or content_type):
            extra_args["ContentType"] = ct

        await self._client.put_object(Bucket=self._bucket, Key=key, Body=body, **extra_args)

    async def flush(self) -> None:
        """No internal buffering; nothing to flush."""
        return

    async def close(self) -> None:
        if self._client:
            try:
                await self._client.__aexit__(None, None, None)
            finally:
                self._client = None
        self._session = None

    # ---- helpers -------------------------------------------------------------

    def _render_key(self, idem: str | None) -> str:
        tpl = self._args.get("key_template") or "flowkit/{node}/{task}/{ts}-{rand}.bin"
        ts = int(time.time() * 1000)
        return tpl.format(node=self._ctx.node_id, task=self._ctx.task_id, ts=ts, idem=idem or "", rand=uuid.uuid4())

    def _to_blob(self, x: Item | Batch) -> tuple[bytes, str | None]:
        # Returns (blob, content_type)
        if isinstance(x, Item):
            if isinstance(x.payload, (bytes, bytearray, memoryview)):
                return bytes(x.payload), None
            return json.dumps(x.payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8"

        if x.items is not None:
            if bool(self._args.get("jsonl_for_batches", True)):
                lines = []
                for it in x.items:
                    if isinstance(it, Item):
                        payload = it.payload
                    else:
                        payload = getattr(it, "payload", it)
                    lines.append(json.dumps(payload, ensure_ascii=False))
                blob = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
                return blob, "application/x-ndjson; charset=utf-8"
            # else: consolidate as JSON array
            arr = []
            for it in x.items:
                if isinstance(it, Item):
                    arr.append(it.payload)
                else:
                    arr.append(getattr(it, "payload", it))
            return json.dumps(arr, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8"

        if x.blob is not None:
            return bytes(x.blob), None

        raise TypeError("unsupported batch shape for emit.s3")
