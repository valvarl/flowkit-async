# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Thin compatibility layer around the public streaming types.

We deliberately DO NOT redeclare ContentKind/FrameDescriptor here to avoid
drift with `flowkit.api.streams`. Instead, we re-export those types and provide
small helpers used by compile-time validation and logging.
"""

from collections.abc import Mapping
from typing import Any, overload

from ..api.streams import ContentKind, FrameDescriptor

__all__ = [
    "ContentKind",
    "FrameDescriptor",
    "describe_frame",
    "validate_frame_descriptor",
]


@overload
def validate_frame_descriptor(fd: None | Mapping[str, Any] | FrameDescriptor) -> None | FrameDescriptor: ...
def validate_frame_descriptor(fd: None | Mapping[str, Any] | FrameDescriptor) -> None | FrameDescriptor:
    """Normalize and validate a FrameDescriptor-like object.

    Accepts:
        - None  -> returns None
        - dict  -> validates keys and returns FrameDescriptor
        - FrameDescriptor -> returned as-is

    Raises:
        ValueError on invalid shape or types.
    """
    if fd is None:
        return None
    if isinstance(fd, FrameDescriptor):
        return fd
    if isinstance(fd, dict):
        # Only pass through supported keys; ignore unknown keys (non-fatal).
        kind = fd.get("kind")
        if kind is None:
            raise ValueError("frame.kind is required")
        # Allow both enum and string for kind
        if isinstance(kind, str):
            try:
                kind_enum = ContentKind(kind)
            except Exception as e:
                raise ValueError(f"unsupported frame.kind: {kind!r}") from e
        elif isinstance(kind, ContentKind):
            kind_enum = kind
        else:
            raise ValueError("frame.kind must be a string or ContentKind")

        type_ = fd.get("type")
        if type_ is not None and not isinstance(type_, str):
            raise ValueError("frame.type must be a string if provided")

        schema_ref = fd.get("schema_ref")
        if schema_ref is not None and not isinstance(schema_ref, str):
            raise ValueError("frame.schema_ref must be a string if provided")

        encoding = fd.get("encoding")
        if encoding is not None and not isinstance(encoding, str):
            raise ValueError("frame.encoding must be a string if provided")

        return FrameDescriptor(kind=kind_enum, type=type_, schema_ref=schema_ref, encoding=encoding)
    raise ValueError("frame descriptor must be a dict, FrameDescriptor or None")


def describe_frame(fd: FrameDescriptor | None) -> str:
    """Return a short human-readable descriptor for logging."""
    if fd is None:
        return "frame:None"
    parts = [f"kind={fd.kind.value}"]
    if fd.type:
        parts.append(f"type={fd.type}")
    if fd.schema_ref:
        parts.append(f"schema_ref={fd.schema_ref}")
    if fd.encoding:
        parts.append(f"encoding={fd.encoding}")
    return "frame:" + ",".join(parts)
