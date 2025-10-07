# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Transform operators bundle.

Registered transforms:
  - "json.map"         → JsonMap
  - "filter"           → FilterExpr
  - "dedupe"           → Dedupe
  - "sort.bounded"     → SortBounded
  - "codecs.to_jsonl"  → ToJsonLines
  - "codecs.from_jsonl"→ FromJsonLines
"""

from collections.abc import Mapping

from ....api.adapters import Plugin, TransformOp
from .codecs import FromJsonLines, ToJsonLines
from .dedupe import Dedupe
from .filter_expr import FilterExpr
from .json_map import JsonMap
from .sort_bounded import SortBounded


class IOTransformsPlugin(Plugin):
    """Plugin bundle for IO transform operators."""

    def sources(self) -> Mapping[str, type]:
        return {}

    def sinks(self) -> Mapping[str, type]:
        return {}

    def transforms(self) -> Mapping[str, type[TransformOp]]:
        return {
            "json.map": JsonMap,
            "filter": FilterExpr,
            "dedupe": Dedupe,
            "sort.bounded": SortBounded,
            "codecs.to_jsonl": ToJsonLines,
            "codecs.from_jsonl": FromJsonLines,
        }


__all__ = [
    "Dedupe",
    "FilterExpr",
    "FromJsonLines",
    "IOTransformsPlugin",
    "JsonMap",
    "SortBounded",
    "ToJsonLines",
]
