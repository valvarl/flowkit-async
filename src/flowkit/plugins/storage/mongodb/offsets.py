# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
MongoDB implementations of CheckpointStore and OffsetStore.
"""

from collections.abc import Iterable
from typing import Any

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import UpdateOne

from flowkit.api.streams import Checkpoint
from flowkit.storage.offsets import CheckpointStore, OffsetKey, OffsetStore

from .tx import MongoUoW


class MongoCheckpointStore(CheckpointStore):
    """
    Collection layout: "checkpoints"
      _id: str            # "{scope}|{key}"
      scope: str
      key: str
      cp: any (JSON-like)
    """

    def __init__(self, db: AsyncIOMotorDatabase, uow: MongoUoW) -> None:
        self._db = db
        self._col: AsyncIOMotorCollection = db["checkpoints"]
        self._uow = uow

    @staticmethod
    def _id(scope: str, key: str) -> str:
        return f"{scope}|{key}"

    async def get(self, scope: str, key: str) -> Checkpoint | None:
        doc = await self._col.find_one({"_id": self._id(scope, key)}, session=self._uow.session)
        return doc.get("cp") if doc else None

    async def put(self, scope: str, key: str, cp: Checkpoint) -> None:
        await self._col.replace_one(
            {"_id": self._id(scope, key)},
            {"_id": self._id(scope, key), "scope": scope, "key": key, "cp": cp},
            upsert=True,
            session=self._uow.session,
        )

    async def delete(self, scope: str, key: str) -> None:
        await self._col.delete_one({"_id": self._id(scope, key)}, session=self._uow.session)


class MongoOffsetStore(OffsetStore):
    """
    Thin wrapper on top of MongoCheckpointStore with canonical (scope, key) mapping.
    """

    def __init__(self, checkpoints: MongoCheckpointStore) -> None:
        self._cp = checkpoints

    def _scope_key(self, k: OffsetKey) -> tuple[str, str]:
        scope = f"task:{k.task_id}|node:{k.node_id}|src:{k.source_alias}"
        key = f"p:{k.partition}"
        return scope, key

    async def read(self, key: OffsetKey) -> Checkpoint | None:
        s, k = self._scope_key(key)
        return await self._cp.get(s, k)

    async def commit(self, key: OffsetKey, cp: Checkpoint) -> None:
        s, k = self._scope_key(key)
        await self._cp.put(s, k, cp)

    async def delete(self, key: OffsetKey) -> None:
        s, k = self._scope_key(key)
        await self._cp.delete(s, k)

    async def read_many(self, keys: Iterable[OffsetKey]) -> list[Checkpoint | None]:
        pairs = [self._scope_key(k) for k in keys]
        # Batch query via _id IN to preserve order
        ids = [f"{s}|{k}" for s, k in pairs]
        col = self._cp._col  # type: ignore[attr-defined]
        uow = self._cp._uow  # type: ignore[attr-defined]
        cur = col.find({"_id": {"$in": ids}}, session=uow.session)
        found: dict[str, Any] = {}
        async for d in cur:
            found[d["_id"]] = d.get("cp")
        return [found.get(i) for i in ids]

    async def commit_many(self, pairs: Iterable[tuple[OffsetKey, Checkpoint]]) -> None:
        ops: list[UpdateOne] = []
        for key, cp in pairs:
            s, k = self._scope_key(key)
            _id = f"{s}|{k}"
            ops.append(
                UpdateOne(
                    {"_id": _id},
                    {"$set": {"scope": s, "key": k, "cp": cp}},
                    upsert=True,
                )
            )
        if not ops:
            return
        col = self._cp._col  # type: ignore[attr-defined]
        uow = self._cp._uow  # type: ignore[attr-defined]
        await col.bulk_write(ops, ordered=False, session=uow.session)
