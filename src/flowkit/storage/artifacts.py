# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Artifact store interface (partial/final/DLQ).

Workers and the coordinator use this interface to persist:
- partial/intermediate artifacts (per node),
- final artifacts (per node),
- DLQ entries (failed batches/items).

This API intentionally deals with *references* and small payloads; large data
streams should be handled by adapters and stored as external objects, returning
a reference (URI/object key). For convenience, small `bytes` writes are supported.
"""

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Mapping, MutableMapping, Optional, Protocol, runtime_checkable

from ..api.streams import Batch, FrameDescriptor


__all__ = [
    "ArtifactError",
    "ArtifactRef",
    "ArtifactStore",
    "BUCKET_PARTIAL",
    "BUCKET_FINAL",
    "BUCKET_DLQ",
]


class ArtifactError(RuntimeError):
    """Base error for artifact operations."""


BUCKET_PARTIAL: str = "partial"
BUCKET_FINAL: str = "final"
BUCKET_DLQ: str = "dlq"


@dataclass(frozen=True)
class ArtifactRef:
    """
    Opaque reference to a stored artifact blob/record.

    Attributes:
        task_id: Task identifier this artifact belongs to.
        node_id: Node identifier within the task.
        bucket: Logical bucket (partial|final|dlq|custom).
        key: Implementation-defined unique key within the bucket.
        size_bytes: Stored payload size (best-effort).
        checksum: Content checksum (e.g., sha256) if available.
        frame: Optional frame descriptor to interpret the blob.
        content_type: MIME type hint.
        created_ms: Epoch milliseconds when written.
        meta: Small metadata map (idempotency key, error info, tags).
    """
    task_id: str
    node_id: str
    bucket: str
    key: str
    size_bytes: int | None = None
    checksum: str | None = None
    frame: FrameDescriptor | None = None
    content_type: str | None = None
    created_ms: int = 0
    meta: Mapping[str, Any] = field(default_factory=dict)

    def uri(self) -> str:
        """Human-friendly, stable string form."""
        return f"artifacts://{self.task_id}/{self.node_id}/{self.bucket}/{self.key}"


@runtime_checkable
class ArtifactStore(Protocol):
    """
    DB-agnostic artifact store.

    Common patterns:
        - Store a framed block (parquet/csv/bytes) and return a reference.
        - Attach small metadata/tags and idempotency keys to deduplicate writes.
        - List artifacts per (task_id, node_id, bucket) with pagination.
    """

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
        """Write a small in-band payload as a new artifact and return its reference.""" ...

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
        """
        Persist a framed batch (blob/ref + frame) or a batch of items serialized by the implementation.
        Implementations MAY choose to reject unframed item batches if unsupported.
        """ ...

    # ---- Read payloads ----

    async def get_bytes(self, ref: ArtifactRef) -> bytes:
        """
        Read full bytes for small artifacts. Implementations MAY raise when the
        artifact is backed by a remote reference and streaming is required.
        """ ...

    async def open_stream(self, ref: ArtifactRef, *, chunk_size: int = 1 << 20) -> AsyncIterator[bytes]:
        """
        Open a byte stream for large artifacts. Implementations MAY choose not to support
        streaming and raise NotImplementedError.
        """ ...

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
        """
        List artifact refs in a bucket with optional paging.
        Return (items, next_cursor).
        """ ...

    async def delete(self, ref: ArtifactRef) -> None: ...

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
        """
        Move/copy an artifact between buckets (e.g., partial -> final) and return the new reference.
        Implementations MAY perform this as a cheap metadata move when possible.
        """ ...
