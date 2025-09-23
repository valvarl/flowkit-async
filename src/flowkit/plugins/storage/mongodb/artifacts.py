# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
MongoDB implementation of ArtifactStore.

- Small payloads are stored inline in the "artifacts" collection.
- Large payloads (> threshold) are stored in GridFS under a dedicated bucket,
  and documents keep the GridFS file id (gridfs_id).
- "promote" is a metadata move (cheap) between buckets.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Mapping
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from typing import Any

from motor.motor_asyncio import (
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
    AsyncIOMotorGridFSBucket,
)
from pymongo import ReturnDocument

from flowkit.api.streams import Batch, FrameDescriptor
from flowkit.storage.artifacts import (
    BUCKET_FINAL,
    BUCKET_PARTIAL,
    ArtifactError,
    ArtifactRef,
    ArtifactStore,
)

from .tx import MongoUoW

INLINE_THRESHOLD = 8 * 1024 * 1024  # 8 MB safe margin under 16MB BSON


def _now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _to_plain(obj: Any) -> Any:
    """Make obj JSON/BSON-friendly."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_to_plain(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _to_plain(v) for k, v in obj.items()}
    # Pydantic
    dump = getattr(obj, "model_dump", None)
    if callable(dump):
        return dump()
    # Dataclasses
    if is_dataclass(obj):
        return asdict(obj)
    # Fallback
    return str(obj)


def _batch_to_bytes(batch: Batch) -> bytes:
    """Serialize batch to UTF-8 JSON bytes (best-effort)."""
    payload = {
        "payload": _to_plain(getattr(batch, "payload", None)),
        "frame": _to_plain(getattr(batch, "frame", None)),
        "meta": _to_plain(getattr(batch, "meta", {})),
    }
    s = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return s.encode("utf-8")


class MongoArtifacts(ArtifactStore):
    """
    Collection layout: "artifacts"
      _id: ObjectId
      task_id: str
      node_id: str
      bucket: str
      key: str
      size_bytes: int
      checksum: str|null
      frame: dict|null
      content_type: str|null
      created_ms: int
      meta: dict
      storage: {
        kind: "inline" | "gridfs",
        data: <binary>           # when inline
        gridfs_id: ObjectId|null # when gridfs
      }

    GridFS bucket: configurable (default "af").
    """

    def __init__(self, db: AsyncIOMotorDatabase, uow: MongoUoW, *, bucket_name: str = "af") -> None:
        self._db = db
        self._col: AsyncIOMotorCollection = db["artifacts"]
        self._uow = uow
        self._gfs = AsyncIOMotorGridFSBucket(db, bucket_name=bucket_name)

    # ---- Write small payloads ----

    async def put_bytes(
        self,
        *,
        task_id: str,
        node_id: str,
        bucket: str,
        data: bytes,
        frame: FrameDescriptor | None = None,
        content_type: str | None = None,
        idempotency_key: str | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> ArtifactRef:
        size = len(data)
        checksum = _sha256(data)
        created = _now_ms()

        storage: dict[str, Any]
        if size <= INLINE_THRESHOLD:
            storage = {"kind": "inline", "data": data}
        else:
            # Upload to GridFS
            fname = f"{task_id}/{node_id}/{bucket}/{checksum}"
            stream = await self._gfs.open_upload_stream(
                filename=fname,
                metadata={"task_id": task_id, "node_id": node_id, "bucket": bucket},
                chunk_size_bytes=1024 * 255,  # default-ish
                session=self._uow.session,
            )
            try:
                await stream.write(data)
            finally:
                await stream.close()
            storage = {"kind": "gridfs", "gridfs_id": stream._id}  # type: ignore[attr-defined]

        doc = {
            "task_id": task_id,
            "node_id": node_id,
            "bucket": bucket,
            "key": checksum if idempotency_key is None else idempotency_key,  # stable key if provided
            "size_bytes": size,
            "checksum": checksum,
            "frame": _to_plain(frame),
            "content_type": content_type,
            "created_ms": created,
            "meta": dict(meta or {}),
            "storage": storage,
            "idempotency_key": idempotency_key,
        }

        # If idempotency_key is provided and unique index exists, insert will dedupe automatically.
        res = await self._col.insert_one(doc, session=self._uow.session)
        key = doc["key"]
        return ArtifactRef(
            task_id=task_id,
            node_id=node_id,
            bucket=bucket,
            key=str(key),
            size_bytes=size,
            checksum=checksum,
            frame=frame,
            content_type=content_type,
            created_ms=created,
            meta=doc["meta"],
        )

    async def put_batch(
        self,
        *,
        task_id: str,
        node_id: str,
        bucket: str,
        batch: Batch,
        idempotency_key: str | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> ArtifactRef:
        # Serialize batch to bytes (UTF-8 JSON) and store via put_bytes.
        data = _batch_to_bytes(batch)
        # We can include a frame hint that it's JSON-serialized batch
        frame_hint = {"kind": "json", "note": "serialized Batch"}
        return await self.put_bytes(
            task_id=task_id,
            node_id=node_id,
            bucket=bucket,
            data=data,
            frame=frame_hint,  # type: ignore[arg-type]
            content_type="application/json",
            idempotency_key=idempotency_key,
            meta=meta,
        )

    # ---- Read payloads ----

    async def get_bytes(self, ref: ArtifactRef) -> bytes:
        doc = await self._find_doc(ref)
        storage = doc.get("storage", {})
        kind = storage.get("kind")
        if kind == "inline":
            return storage.get("data", b"")
        if kind == "gridfs":
            gridfs_id = storage.get("gridfs_id")
            if not gridfs_id:
                raise ArtifactError("gridfs artifact missing gridfs_id")
            stream = await self._gfs.open_download_stream(gridfs_id, session=self._uow.session)
            try:
                chunks: list[bytes] = []
                while True:
                    b = await stream.readchunk()
                    if not b:
                        break
                    chunks.append(bytes(b))
                return b"".join(chunks)
            finally:
                await stream.close()
        raise ArtifactError(f"unknown storage kind: {kind}")

    async def open_stream(self, ref: ArtifactRef, *, chunk_size: int = 1 << 20) -> AsyncIterator[bytes]:
        doc = await self._find_doc(ref)
        storage = doc.get("storage", {})
        kind = storage.get("kind")
        if kind == "inline":
            yield storage.get("data", b"")
            return
        if kind == "gridfs":
            gridfs_id = storage.get("gridfs_id")
            if not gridfs_id:
                raise ArtifactError("gridfs artifact missing gridfs_id")
            stream = await self._gfs.open_download_stream(gridfs_id, session=self._uow.session)
            try:
                while True:
                    b = await stream.read(chunk_size)
                    if not b:
                        break
                    yield bytes(b)
            finally:
                await stream.close()
            return
        raise ArtifactError(f"unknown storage kind: {kind}")

    # ---- Listing / deletion ----

    async def list(
        self,
        *,
        task_id: str,
        node_id: str,
        bucket: str,
        limit: int = 100,
        cursor: str | None = None,
        prefix: str | None = None,
    ) -> tuple[list[ArtifactRef], str | None]:
        q: dict[str, Any] = {"task_id": task_id, "node_id": node_id, "bucket": bucket}
        if prefix:
            q["key"] = {"$regex": f"^{prefix}"}
        if cursor:
            # cursor is last key (string ordering)
            q["key"] = {**(q.get("key") or {}), "$gt": cursor}

        cur = self._col.find(q, projection={"storage.data": 0}).sort([("key", 1)]).limit(int(limit))
        items: list[ArtifactRef] = []
        last_key: str | None = None
        async for d in cur:
            last_key = str(d["key"])
            items.append(
                ArtifactRef(
                    task_id=str(d["task_id"]),
                    node_id=str(d["node_id"]),
                    bucket=str(d["bucket"]),
                    key=str(d["key"]),
                    size_bytes=int(d.get("size_bytes") or 0),
                    checksum=d.get("checksum"),
                    frame=d.get("frame"),
                    content_type=d.get("content_type"),
                    created_ms=int(d.get("created_ms") or 0),
                    meta=d.get("meta") or {},
                )
            )

        next_cursor = last_key if items and len(items) == int(limit) else None
        return items, next_cursor

    async def delete(self, ref: ArtifactRef) -> None:
        doc = await self._find_doc(ref)
        storage = doc.get("storage", {})
        if storage.get("kind") == "gridfs" and storage.get("gridfs_id"):
            await self._gfs.delete(storage["gridfs_id"], session=self._uow.session)
        await self._col.delete_one(
            {"task_id": ref.task_id, "node_id": ref.node_id, "bucket": ref.bucket, "key": ref.key},
            session=self._uow.session,
        )

    # ---- Convenience operations ----

    async def promote(
        self,
        *,
        task_id: str,
        node_id: str,
        key: str,
        from_bucket: str = BUCKET_PARTIAL,
        to_bucket: str = BUCKET_FINAL,
        overwrite: bool = False,
    ) -> ArtifactRef:
        # Optional overwrite: delete destination if exists
        dst_q = {"task_id": task_id, "node_id": node_id, "bucket": to_bucket, "key": key}
        if not overwrite:
            exists = await self._col.find_one(dst_q, session=self._uow.session)
            if exists:
                raise ArtifactError("destination already exists")
        else:
            await self._col.delete_one(dst_q, session=self._uow.session)

        # Cheap move: update bucket on the source doc
        src_q = {"task_id": task_id, "node_id": node_id, "bucket": from_bucket, "key": key}
        d = await self._col.find_one_and_update(
            src_q,
            {"$set": {"bucket": to_bucket}},
            return_document=ReturnDocument.AFTER,
            session=self._uow.session,
        )
        if not d:
            raise ArtifactError("source artifact not found")
        return ArtifactRef(
            task_id=str(d["task_id"]),
            node_id=str(d["node_id"]),
            bucket=str(d["bucket"]),
            key=str(d["key"]),
            size_bytes=int(d.get("size_bytes") or 0),
            checksum=d.get("checksum"),
            frame=d.get("frame"),
            content_type=d.get("content_type"),
            created_ms=int(d.get("created_ms") or 0),
            meta=d.get("meta") or {},
        )

    # ---- helpers ----

    async def _find_doc(self, ref: ArtifactRef) -> Mapping[str, Any]:
        d = await self._col.find_one(
            {"task_id": ref.task_id, "node_id": ref.node_id, "bucket": ref.bucket, "key": ref.key},
            session=self._uow.session,
        )
        if not d:
            raise ArtifactError("artifact not found")
        return d
