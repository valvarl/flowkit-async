# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
S3-compatible object storage input (list + get).

Two modes:
  - "ref" (default): yield Batch with `ref="s3://bucket/key"` (no download).
  - "bytes": download objects and emit Item(BYTES) per object (use with care).

Soft dependencies:
  - aioboto3 (preferred) OR boto3 with asyncio.to_thread fallback.

Args:
    {
      "bucket": "my-bucket",
      "prefix": "path/to/dir/",
      "suffix": ".jsonl",
      "endpoint_url": "https://s3.amazonaws.com",   # optional
      "region_name": "us-east-1",                    # optional
      "mode": "ref" | "bytes",
      "json": false,                                 # if true + bytes mode → decode JSON into RECORD
      "max_keys": 1000
    }

Checkpoint:
  {"last_key": "path/to/dir/file-00042.json"}

This is a pragmatic MVP; production deployments may replace it with a more
feature-complete adapter (e.g., versioning, etag, range reads, streaming).
"""

import asyncio
import json
from collections.abc import AsyncIterator, Mapping
from typing import Any

try:
    import aioboto3 as _s3lib  # type: ignore

    _AIO = True
except Exception:  # pragma: no cover
    _s3lib = None
    _AIO = False

from ....api.adapters import AdapterContext, SourceAdapter
from ....api.streams import Batch, Checkpoint, ContentKind, FrameDescriptor, Item


class S3ListGetSource(SourceAdapter):
    """
    Minimal S3 list+get source.

    For large objects or heavy traffic, consider implementing streaming reads.
    """

    name = "pull.s3.list+get"
    capabilities = None

    def __init__(self) -> None:
        self._ctx: AdapterContext | None = None
        self._args: Mapping[str, Any] = {}
        self._session: Any = None  # aioboto3.Session if available

    async def open(self, ctx: AdapterContext, **spec: Any) -> None:
        if _s3lib is None:
            raise RuntimeError("S3ListGetSource requires aioboto3 installed")
        self._ctx = ctx
        self._args = spec or {}
        if not _AIO:
            # For MVP we avoid blocking fallbacks; require aioboto3 explicitly.
            raise RuntimeError("S3ListGetSource: install aioboto3 (async) to use this adapter")
        self._session = _s3lib.Session()

    def stream(self, ctx: AdapterContext, *, checkpoint: Checkpoint | None = None) -> AsyncIterator[Any]:
        return self._stream_impl(ctx, checkpoint=checkpoint)

    async def close(self) -> None:
        self._session = None
        self._ctx = None

    # ---- internals -----------------------------------------------------------

    async def _stream_impl(self, ctx: AdapterContext, *, checkpoint: Checkpoint | None) -> AsyncIterator[Any]:
        bucket = self._args.get("bucket")
        if not bucket:
            raise ValueError("S3ListGetSource requires 'bucket' argument")

        prefix = self._args.get("prefix") or ""
        suffix = self._args.get("suffix")
        endpoint_url = self._args.get("endpoint_url")
        region_name = self._args.get("region_name")
        mode = (self._args.get("mode") or "ref").lower()
        json_mode = bool(self._args.get("json", False))
        max_keys = int(self._args.get("max_keys", 1000))

        last_key: str | None = None
        if checkpoint and checkpoint.token:
            last_key = checkpoint.token.get("last_key")

        cancel_ev = getattr(ctx, "cancel_flag", None)

        if _AIO:
            async with self._session.client("s3", endpoint_url=endpoint_url, region_name=region_name) as s3:
                paginator = s3.get_paginator("list_objects_v2")
                async for page in paginator.paginate(
                    Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": max_keys}
                ):
                    if cancel_ev is not None and cancel_ev.is_set():  # type: ignore[attr-defined]
                        break
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        if suffix and not key.endswith(suffix):
                            continue
                        if last_key and key <= last_key:
                            continue  # resume after last checkpoint
                        # Emit according to mode
                        if mode == "bytes":
                            resp = await s3.get_object(Bucket=bucket, Key=key)
                            body = await resp["Body"].read()
                            item = _item_from_bytes(body, json_mode, bucket, key)
                            ckpt = Checkpoint(token={"last_key": key})
                            yield (item, ckpt, f"s3:{bucket}:{key}")
                        else:
                            batch = Batch(
                                items=None,
                                frame=FrameDescriptor(kind=ContentKind.REF, type="s3"),
                                ref=f"s3://{bucket}/{key}",
                                meta={"count": 1, "size_bytes": obj.get("Size")},
                            )
                            ckpt = Checkpoint(token={"last_key": key})
                            yield (batch, ckpt, f"s3:{bucket}:{key}")
        else:
            # boto3 sync fallback via to_thread
            client = _s3lib.client("s3", endpoint_url=endpoint_url, region_name=region_name)  # type: ignore[union-attr]
            paginator = client.get_paginator("list_objects_v2")
            for page in await asyncio.to_thread(
                lambda: paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": max_keys})
            ):
                if cancel_ev is not None and cancel_ev.is_set():  # type: ignore[attr-defined]
                    break
                # boto3 paginator returns a PageIterator; iterate materialized pages
                page_obj = await asyncio.to_thread(lambda p=page: next(iter([p]), {}))  # no-op to yield execution
                for obj in page_obj.get("Contents", []):
                    key = obj["Key"]
                    if suffix and not key.endswith(suffix):
                        continue
                    if last_key and key <= last_key:
                        continue
                    if mode == "bytes":
                        body = await asyncio.to_thread(lambda: client.get_object(Bucket=bucket, Key=key)["Body"].read())
                        item = _item_from_bytes(body, json_mode, bucket, key)
                        ckpt = Checkpoint(token={"last_key": key})
                        yield (item, ckpt, f"s3:{bucket}:{key}")
                    else:
                        batch = Batch(
                            items=None,
                            frame=FrameDescriptor(kind=ContentKind.REF, type="s3"),
                            ref=f"s3://{bucket}/{key}",
                            meta={"count": 1, "size_bytes": obj.get("Size")},
                        )
                        ckpt = Checkpoint(token={"last_key": key})
                        yield (batch, ckpt, f"s3:{bucket}:{key}")


def _item_from_bytes(body: bytes, json_mode: bool, bucket: str, key: str) -> Item:
    if json_mode:
        try:
            payload = json.loads(body.decode("utf-8"))
            frame = FrameDescriptor(kind=ContentKind.RECORD, type="json")
        except Exception:
            payload = body
            frame = FrameDescriptor(kind=ContentKind.BYTES, type="bytes")
    else:
        payload = body
        frame = FrameDescriptor(kind=ContentKind.BYTES, type="bytes")
    return Item(payload=payload, frame=frame, meta={"s3.bucket": bucket, "s3.key": key})
