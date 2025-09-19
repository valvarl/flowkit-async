# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any, Literal, TypedDict

"""
Static adapter capability descriptors used for compile-time checks.

Notes:
- Keep this intentionally small and stable. Real adapters may expose richer
  descriptors at runtime; this module only enables basic compatibility checks.
"""

__all__ = [
    "KNOWN_ADAPTER_CAPS",
    "AdapterCaps",
    "get_caps",
]

ContentKindT = Literal["BYTES", "RECORD", "BATCH", "REF", "CONTROL"]


class AdapterCaps(TypedDict, total=False):
    """Minimal static capabilities for adapters.

    Keys:
        supports_select: which input 'select' modes are supported.
        produces: for sources (input adapters), what content kinds are produced.
        consumes: for outputs (sinks), what content kinds are consumed.
    """

    supports_select: list[Literal["batches", "final"]]
    produces: dict[str, Any]  # e.g. {"content": ["BATCH"], "frames": ["jsonl","csv"]}
    consumes: dict[str, Any]  # e.g. {"content": ["BATCH","BYTES"], "frames": ["parquet"]}


# Extend as your adapters grow.
KNOWN_ADAPTER_CAPS: dict[str, AdapterCaps] = {
    # ---- input adapters
    "pull.from_artifacts": {
        "supports_select": ["batches", "final"],
        "produces": {"content": ["BATCH", "RECORD"]},
    },
    "pull.from_artifacts.rechunk:size": {
        "supports_select": ["final"],
        "produces": {"content": ["BATCH"]},
    },
    "pull.kafka.subscribe": {
        "supports_select": ["batches"],
        "produces": {"content": ["BATCH", "BYTES"]},
    },
    # ---- sinks / outputs
    "push.to_artifacts": {
        "consumes": {"content": ["BATCH", "RECORD", "BYTES", "REF"]},
    },
    "emit.kafka": {
        "consumes": {"content": ["BATCH", "RECORD", "BYTES"]},
    },
    "emit.s3": {
        "consumes": {"content": ["BATCH", "BYTES", "REF"]},
    },
}


def get_caps(name: str) -> AdapterCaps | None:
    """Return capability descriptor for a known adapter or None if unknown."""
    return KNOWN_ADAPTER_CAPS.get(name)
