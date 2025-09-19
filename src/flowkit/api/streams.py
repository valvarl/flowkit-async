# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Stream model primitives exposed to extensions.

These types model the data plane: content descriptors, items, batches and
acknowledgement tokens. Adapters and transform operators should use them.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, MutableMapping, Optional, Sequence


class ContentKind(str, Enum):
    """
    High-level content kind carried by an item or a framed batch.

    Notes:
        - RECORD: a single structured record (e.g., JSON-like object).
        - BYTES: opaque bytes (e.g., encoded message).
        - FILE: reference to a local/ephemeral file payload (path/handle).
        - KV: key/value pair semantics (e.g., changelog streams).
        - REF: external reference (URI/object key) instead of in-band bytes.
        - CONTROL: markers/watermarks/barriers signaling control flow.

    A single stream (port) should be content-homogeneous to simplify validation.
    """

    RECORD = "record"
    BYTES = "bytes"
    FILE = "file"
    KV = "kv"
    REF = "ref"
    CONTROL = "control"


@dataclass(frozen=True)
class FrameDescriptor:
    """
    Opaque descriptor for the physical representation of the payload.

    Attributes:
        kind: Content kind (record/bytes/file/kv/ref/control).
        type: Optional format hint (json|avro|parquet|csv|bytes|...).
        schema_ref: Opaque schema id/version (validation is out of scope here).
        encoding: Optional transport encoding (gzip|snappy|zstd|...).
    """

    kind: ContentKind
    type: Optional[str] = None
    schema_ref: Optional[str] = None
    encoding: Optional[str] = None


@dataclass
class Checkpoint:
    """
    Adapter-defined progress token. Must be JSON-serializable.

    Examples:
        - Kafka: {"topic": "...", "partition": 5, "offset": 123}
        - HTTP pagination: {"next_url": "...", "etag": "..."}
        - Filesystem: {"path": "...", "cursor": "..."}
    """

    token: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class Item:
    """
    Single item flowing through a stream.

    Attributes:
        payload: The logical content (JSON-like object for RECORD; bytes for BYTES; etc).
        meta: Small metadata map (timestamps, ids, routing hints). Avoid high cardinality keys.
        frame: Optional frame descriptor describing how to interpret `payload`.
        checkpoint: Optional checkpoint emitted by the source.
        port: Optional logical input port name (when using ports mode).
        source_alias: Optional originating source alias within the node.
    """

    payload: Any
    meta: MutableMapping[str, Any] = field(default_factory=dict)
    frame: Optional[FrameDescriptor] = None
    checkpoint: Optional[Checkpoint] = None
    port: Optional[str] = None
    source_alias: Optional[str] = None


@dataclass
class Batch:
    """
    Batch of items or a single framed block.

    Two representations:
        - items != None: a list/sequence of `Item` (homogeneous by content).
        - items == None: a framed block (parquet/csv/bytes) described by `frame`
                         and backed by either `blob` (in-band bytes) or `ref`
                         (external reference like S3 key / URI).

    Attributes:
        items: Sequence of items (if not a framed block).
        frame: Frame descriptor (required for framed blocks; optional for item batches).
        blob: In-band byte buffer for a framed block.
        ref: External reference for a framed block (S3 key, file://, gs://, http://, etc).
        meta: Batch-level metadata (e.g., aggregation counters). Recommended keys:
              - size_bytes: int   total size for backpressure decisions
              - count: int        number of logical records/items within this batch
        checkpoint: A checkpoint acknowledging the *batch* (supersedes per-item ones).
        port: Optional logical input port name.
        source_alias: Optional originating source alias.
    """

    items: Sequence[Item] | None = None
    frame: Optional[FrameDescriptor] = None
    blob: Optional[bytes | bytearray | memoryview] = None
    ref: Optional[str | Mapping[str, Any]] = None
    meta: MutableMapping[str, Any] = field(default_factory=dict)
    checkpoint: Optional[Checkpoint] = None
    port: Optional[str] = None
    source_alias: Optional[str] = None


# Optional explicit Ack/Nack message objects for adapter APIs that prefer them.


@dataclass(frozen=True)
class Ack:
    """Explicit acknowledgement carrying an optional checkpoint."""

    checkpoint: Optional[Checkpoint] = None


@dataclass(frozen=True)
class Nack:
    """Negative acknowledgement pointing to an optional error."""

    error: Optional[BaseException] = None
