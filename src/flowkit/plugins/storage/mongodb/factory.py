# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Factory for MongoDB-based StorageFacade.
"""

from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient

from flowkit.storage.artifacts import ArtifactStore
from flowkit.storage.factory import StorageFacade
from flowkit.storage.kv import KVStore
from flowkit.storage.offsets import CheckpointStore, OffsetStore
from flowkit.storage.outbox import OutboxStore
from flowkit.storage.tasks import TaskStore

from .artifacts import MongoArtifacts
from .kv import MongoKV
from .mapping import ensure_indexes
from .offsets import MongoCheckpointStore, MongoOffsetStore
from .outbox import MongoOutbox
from .tasks import MongoTasks
from .tx import MongoUoW


async def mongodb_factory(*, dsn: str, **kwargs) -> StorageFacade:
    """
    Create a Mongo-backed StorageFacade.

    Args:
        dsn: mongodb DSN (replica set recommended).
        kwargs:
            db: database name (default: "flowkit")
            client_args: dict of extra kwargs for AsyncIOMotorClient
            gridfs_bucket: artifacts GridFS bucket name (default: "af")
            outbox_lock_ms: reservation lock window (default: 30000)
    """
    db_name: str = kwargs.get("db", "flowkit")
    client_args: dict[str, Any] = kwargs.get("client_args", {})
    gridfs_bucket: str = kwargs.get("gridfs_bucket", "af")
    outbox_lock_ms: int = int(kwargs.get("outbox_lock_ms", 30_000))

    client = AsyncIOMotorClient(dsn, **client_args)
    db = client[db_name]

    # Ensure indexes lazily (idempotent)
    await ensure_indexes(db)

    uow = MongoUoW(client, db)

    kv: KVStore = MongoKV(db, uow)
    checkpoints: CheckpointStore = MongoCheckpointStore(db, uow)
    offsets: OffsetStore = MongoOffsetStore(checkpoints)
    tasks: TaskStore = MongoTasks(db, uow)
    artifacts: ArtifactStore = MongoArtifacts(db, uow, bucket_name=gridfs_bucket)
    outbox: OutboxStore = MongoOutbox(db, uow, lock_ms=outbox_lock_ms)

    return StorageFacade(
        uow=uow,
        kv=kv,
        checkpoints=checkpoints,
        offsets=offsets,
        outbox=outbox,
        artifacts=artifacts,
        tasks=tasks,
    )
