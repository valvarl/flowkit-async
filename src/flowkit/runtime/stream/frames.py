# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Fast-path helpers for frame normalization and lightweight content validation.

These utilities are intentionally strict-but-cheap:
- infer content kind from payload shape
- ensure Item/Batch are internally consistent
- derive minimal metadata (size_bytes, count) to aid QoS heuristics

Heavy/format-specific validation (e.g., schema checks, Parquet footer) is out
of scope and should be implemented by adapters/transforms/codecs.
"""

from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import replace
from typing import Any

from ...api.streams import Batch, ContentKind, FrameDescriptor, Item

__all__ = [
    "approx_size_bytes",
    "infer_content_kind",
    "normalize_batch",
    "normalize_item",
    "validate_frame_for_payload",
]


def infer_content_kind(payload: Any) -> ContentKind:
    """
    Infer ContentKind from payload shape.

    Rules:
        - bytes/bytearray/memoryview -> BYTES
        - mapping/list/tuple/str/bool/int/float/None -> RECORD (JSON-like)
        - everything else -> RECORD (best-effort; adapters may refine)
    """
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return ContentKind.BYTES
    # Common JSON-like primitives/containers are treated as RECORD
    if isinstance(payload, (Mapping, list, tuple, str, bool, int, float)) or payload is None:
        return ContentKind.RECORD
    # Fallback: treat as RECORD to avoid hard failures on custom objects
    return ContentKind.RECORD


def validate_frame_for_payload(frame: FrameDescriptor | None, *, payload: Any | None, blob: Any | None) -> None:
    """
    Cheap sanity checks for frame/payload compatibility.

    - If a framed block is provided (blob is not None), the frame must exist.
    - If frame.kind == BYTES, payload must be bytes-like when not using blob.
    - If frame.kind == RECORD, payload can be any JSON-like object.
    - REF/CONTROL kinds are permitted; runtime semantics are enforced elsewhere.
    """
    if blob is not None and frame is None:
        raise ValueError("framed block requires a FrameDescriptor")

    if frame is None:
        return

    k = frame.kind
    if payload is None:
        return

    if k == ContentKind.BYTES:
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ValueError("frame.kind=bytes requires bytes-like payload")
    elif k == ContentKind.RECORD:
        # Accept JSON-like; no heavy validation here.
        pass
    elif k in (ContentKind.FILE, ContentKind.KV, ContentKind.REF, ContentKind.CONTROL):
        # No hard checks here; adapters/ops define semantics.
        pass
    else:
        raise ValueError(f"unsupported frame.kind: {k}")


def approx_size_bytes(x: Any) -> int:
    """
    Conservative, cheap size estimator for backpressure heuristics.
    - bytes/memoryview: len()
    - str: len(utf-8)
    - mapping/list/tuple: sum of child estimates (shallow)
    - fallback: 0 (unknown)
    """
    try:
        if isinstance(x, (bytes, bytearray, memoryview)):
            return len(x)
        if isinstance(x, str):
            return len(x.encode("utf-8", errors="ignore"))
        if isinstance(x, Mapping):
            # shallow estimate: keys + values
            return sum(approx_size_bytes(k) + approx_size_bytes(v) for k, v in x.items())
        if isinstance(x, (list, tuple)):
            return sum(approx_size_bytes(v) for v in x)
    except Exception:
        return 0
    return 0


def normalize_item(
    payload: Any,
    *,
    meta: MutableMapping[str, Any] | None = None,
    frame: FrameDescriptor | None = None,
    port: str | None = None,
    source_alias: str | None = None,
) -> Item:
    """
    Build a consistent `Item` from arbitrary user/adaptor inputs.

    - Infers frame.kind when not provided.
    - Validates that frame/payload are not obviously contradictory.
    - Does not deep-copy payload/meta for performance reasons.
    """
    if frame is None:
        frame = FrameDescriptor(kind=infer_content_kind(payload))
    validate_frame_for_payload(frame, payload=payload, blob=None)
    it = Item(
        payload=payload,
        meta=meta or {},
        frame=frame,
        checkpoint=None,
        port=port,
        source_alias=source_alias,
    )
    return it


def _derive_batch_meta(
    meta: MutableMapping[str, Any] | None, *, size_bytes: int | None, count: int | None
) -> MutableMapping[str, Any]:
    out: MutableMapping[str, Any] = dict(meta or {})
    # Do not overwrite explicit values if present
    if "size_bytes" not in out and size_bytes is not None:
        out["size_bytes"] = int(size_bytes)
    if "count" not in out and count is not None:
        out["count"] = int(count)
    return out


def normalize_batch(
    x: Batch | Iterable[Item] | bytes | bytearray | memoryview,
    *,
    frame: FrameDescriptor | None = None,
    meta: MutableMapping[str, Any] | None = None,
    port: str | None = None,
    source_alias: str | None = None,
) -> Batch:
    """
    Normalize an arbitrary input into a `Batch`.

    Accepts:
        - existing `Batch`
        - iterable of `Item`s -> item batch
        - bytes-like -> framed block (requires/creates frame.kind=BYTES)

    Derives `meta.size_bytes` and `meta.count` when cheap to compute.
    """
    # Existing batch: ensure minimal invariants and fill meta/count/size
    if isinstance(x, Batch):
        b = x
        # attach port/source if caller provides (non-destructive)
        if port is not None and b.port is None:
            b = replace(b, port=port)
        if source_alias is not None and b.source_alias is None:
            b = replace(b, source_alias=source_alias)

        if b.items is not None:
            cnt = len(b.items)
            size = sum(approx_size_bytes(it.payload) for it in b.items if it is not None)
            b.meta = _derive_batch_meta(b.meta, size_bytes=size, count=cnt)
            return b

        # framed block must have a frame descriptor
        if b.blob is not None:
            if b.frame is None:
                if frame is None:
                    raise ValueError("framed block batch requires a FrameDescriptor")
                b.frame = frame
            validate_frame_for_payload(b.frame, payload=None, blob=b.blob)
            size = len(b.blob) if isinstance(b.blob, (bytes, bytearray, memoryview)) else 0
            b.meta = _derive_batch_meta(b.meta, size_bytes=size, count=None)
            return b

        # ref-only framed block
        if b.ref is not None:
            if b.frame is None:
                if frame is None:
                    raise ValueError("ref-only batch requires a FrameDescriptor")
                b.frame = frame
            validate_frame_for_payload(b.frame, payload=None, blob=b.ref)
            b.meta = _derive_batch_meta(b.meta, size_bytes=None, count=None)
            return b

        # nothing inside
        raise ValueError("empty Batch: either items, blob or ref must be set")

    # Iterable[Item] -> item batch
    if isinstance(x, (list, tuple)) or (hasattr(x, "__iter__") and not isinstance(x, (bytes, bytearray, memoryview))):
        items = list(x)  # materialize once (caller usually batches already)
        cnt = len(items)
        size = sum(approx_size_bytes(it.payload) for it in items)
        b = Batch(
            items=items, frame=None, blob=None, ref=None, meta=_derive_batch_meta(meta, size_bytes=size, count=cnt)
        )
        b.port = port
        b.source_alias = source_alias
        return b

    # bytes-like -> framed block with kind=BYTES if not provided
    if isinstance(x, (bytes, bytearray, memoryview)):
        f = frame or FrameDescriptor(kind=ContentKind.BYTES)
        validate_frame_for_payload(f, payload=None, blob=x)
        b = Batch(items=None, frame=f, blob=x, ref=None, meta=_derive_batch_meta(meta, size_bytes=len(x), count=None))
        b.port = port
        b.source_alias = source_alias
        return b

    raise TypeError("unsupported input for normalize_batch()")
