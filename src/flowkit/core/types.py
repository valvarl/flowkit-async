from __future__ import annotations

"""
flowkit.core.types
==================

Shared type aliases and small constants used across the codebase.
Keep this module **tiny** and dependency-free.

Guidelines:
- Prefer narrow aliases for clarity (e.g., TimestampMs vs generic int).
- Avoid importing application-level models here.
"""

import os
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any, Final, Union

# ---- JSON-like value aliases -------------------------------------------------

JSONScalar = Union[str, int, float, bool, None]
JSONValue = Union[JSONScalar, "JSONArray", "JSONDict"]
JSONArray = list[JSONValue]
JSONDict = dict[str, JSONValue]

# Read-only views
ROJSONDict = Mapping[str, JSONValue]
ROMappingStrAny = Mapping[str, Any]
MutableJSONDict = MutableMapping[str, JSONValue]

# Paths
StrPath = Union[str, os.PathLike[str], Path]

# ---- Time & IDs --------------------------------------------------------------

Millis = int
Seconds = float
TimestampMs = int  # wall-clock epoch timestamp (ms)
MonotonicMs = int  # process-local monotonic time (ms)

# Various identifiers (semantic sugar over str)
TopicName = str
WorkerId = str
TaskId = str
NodeId = str
AdapterName = str
HookName = str
ExternalName = str

# ---- Constants ---------------------------------------------------------------

# Default digest size used by stable_hash (BLAKE2b).
DEFAULT_BLAKE2_DIGEST_SIZE: Final[int] = 20

# NanoID defaults (URL-safe alphabet).
DEFAULT_NANOID_ALPHABET: Final[str] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz-"
DEFAULT_NANOID_SIZE: Final[int] = 21


__all__ = [
    # JSON aliases
    "JSONScalar",
    "JSONValue",
    "JSONArray",
    "JSONDict",
    "ROJSONDict",
    "ROMappingStrAny",
    "MutableJSONDict",
    # path/time/ids
    "StrPath",
    "Millis",
    "Seconds",
    "TimestampMs",
    "MonotonicMs",
    "TopicName",
    "WorkerId",
    "TaskId",
    "NodeId",
    "AdapterName",
    "HookName",
    "ExternalName",
    # constants
    "DEFAULT_BLAKE2_DIGEST_SIZE",
    "DEFAULT_NANOID_ALPHABET",
    "DEFAULT_NANOID_SIZE",
]
