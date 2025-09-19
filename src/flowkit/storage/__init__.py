# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
DB-agnostic storage interfaces used by coordinator and workers.
"""

from .artifacts import (
    BUCKET_DLQ,
    BUCKET_FINAL,
    BUCKET_PARTIAL,
    ArtifactError,
    ArtifactRef,
    ArtifactStore,
)
from .kv import KVError, KVStore, LockError, LockInfo
from .offsets import CheckpointStore, OffsetError, OffsetKey, OffsetStore
from .tasks import LeaseInfo, TaskDoc, TaskStore, TaskStoreError

__all__ = [
    # kv
    "KVError",
    "LockError",
    "LockInfo",
    "KVStore",
    # offsets
    "OffsetError",
    "CheckpointStore",
    "OffsetKey",
    "OffsetStore",
    # artifacts
    "ArtifactError",
    "ArtifactRef",
    "ArtifactStore",
    "BUCKET_PARTIAL",
    "BUCKET_FINAL",
    "BUCKET_DLQ",
    # tasks
    "TaskStoreError",
    "LeaseInfo",
    "TaskDoc",
    "TaskStore",
]
