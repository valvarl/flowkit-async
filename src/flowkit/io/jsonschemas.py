# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing import Any, Dict

"""
JSON Schemas for documentation and optional runtime validation where needed.
"""

__all__ = ["frame_descriptor_schema"]


def frame_descriptor_schema() -> Dict[str, Any]:
    """JSON Schema (draft-07) for FrameDescriptor-like objects."""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "FrameDescriptor",
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "kind": {"type": "string", "minLength": 1},
            "type": {"type": "string"},
            "schema_ref": {"type": "string"},
            "encoding": {"type": "string"},
        },
        "required": ["kind"],
    }
