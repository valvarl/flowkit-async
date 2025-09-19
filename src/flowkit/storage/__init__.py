# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
DB-agnostic storage interfaces used by coordinator and workers.
"""

from .kv import KVError, LockError, LockInfo, KVStore
from .offsets import OffsetError, CheckpointStore, OffsetKey, OffsetStore
from .artifacts import (
    ArtifactError,
    ArtifactRef,
    ArtifactStore,
    BUCKET_PARTIAL,
    BUCKET_FINAL,
    BUCKET_DLQ,
)
from .tasks import TaskStoreError, LeaseInfo, TaskDoc, TaskStore

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
