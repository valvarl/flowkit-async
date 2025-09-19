# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Public exports for the IO typing & validation package.
"""

from .capabilities import KNOWN_ADAPTER_CAPS, AdapterCaps, get_caps
from .content import ContentKind, FrameDescriptor, describe_frame, validate_frame_descriptor
from .jsonschemas import frame_descriptor_schema
from .validation import (
    ADAPTER_SPECS,
    ARG_ALIAS,
    adapter_aliases,
    check_select_supported,
    normalize_adapter_args,
    resolve_adapter_name,
    validate_input_adapter,
    validate_io_source,
    validate_output_adapter,
)

__all__ = [
    # content
    "ContentKind",
    "FrameDescriptor",
    "validate_frame_descriptor",
    "describe_frame",
    # capabilities
    "AdapterCaps",
    "KNOWN_ADAPTER_CAPS",
    "get_caps",
    # json schema
    "frame_descriptor_schema",
    # validation
    "ADAPTER_SPECS",
    "ARG_ALIAS",
    "resolve_adapter_name",
    "adapter_aliases",
    "normalize_adapter_args",
    "validate_input_adapter",
    "check_select_supported",
    "validate_io_source",
    "validate_output_adapter",
]
