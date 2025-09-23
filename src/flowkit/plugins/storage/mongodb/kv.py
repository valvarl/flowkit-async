# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
MongoDB implementation of KVStore and distributed locks.
"""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from flowkit.storage.kv import KVStore, LockInfo

from .tx import MongoUoW


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _ms_from_dt(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class MongoKV(KVStore):
    """
    Collection layout:

    kv:
      _id: str (key)
      v: any (JSON-like)
      expires_at: datetime (optional)  # TTL index
    locks:
      _id: str (lock name)
      owner: str
      token: str
      deadline_ms: int
      meta: dict (optional)
    """

    def __init__(self, db: AsyncIOMotorDatabase, uow: MongoUoW) -> None:
        self._db = db
        self._col: AsyncIOMotorCollection = db["kv"]
        self._locks: AsyncIOMotorCollection = db["locks"]
        self._uow = uow

    # ---- Plain KV ----

    async def get(self, key: str) -> Any | None:
        doc = await self._col.find_one({"_id": key}, session=self._uow.session)
        if not doc:
            return None
        # Soft check for expiration if TTL daemon hasn't kicked in yet
        exp: datetime | None = doc.get("expires_at")
        if exp and exp <= _now_utc():
            return None
        return doc.get("v")

    async def mget(self, keys: list[str]) -> list[Any | None]:
        if not keys:
            return []
        cur = self._col.find({"_id": {"$in": keys}}, session=self._uow.session)
        found: dict[str, Any] = {}
        async for d in cur:
            exp = d.get("expires_at")
            if exp and exp <= _now_utc():
                continue
            found[d["_id"]] = d.get("v")
        return [found.get(k) for k in keys]

    async def set(self, key: str, value: Any, *, ttl_ms: int | None = None) -> None:
        update: dict[str, Any] = {"v": value}
        if ttl_ms is not None:
            update["expires_at"] = _now_utc() + timedelta(milliseconds=ttl_ms)
        await self._col.replace_one({"_id": key}, {"_id": key, **update}, upsert=True, session=self._uow.session)

    async def delete(self, key: str) -> None:
        await self._col.delete_one({"_id": key}, session=self._uow.session)

    async def incr(self, key: str, *, by: int = 1, ttl_ms: int | None = None) -> int:
        update: dict[str, Any] = {"$inc": {"v": int(by)}}
        if ttl_ms is not None:
            update["$set"] = {"expires_at": _now_utc() + timedelta(milliseconds=ttl_ms)}
        doc = await self._col.find_one_and_update(
            {"_id": key},
            update,
            upsert=True,
            return_document=ReturnDocument.AFTER,
            session=self._uow.session,
        )
        return int(doc.get("v", 0))

    async def expire(self, key: str, ttl_ms: int) -> bool:
        res = await self._col.update_one(
            {"_id": key},
            {"$set": {"expires_at": _now_utc() + timedelta(milliseconds=ttl_ms)}},
            session=self._uow.session,
        )
        return res.matched_count > 0

    async def ttl(self, key: str) -> int | None:
        """
        Return remaining TTL (ms).
        - None → key exists but has no TTL
        - -1   → key not found
        """
        doc = await self._col.find_one({"_id": key}, projection={"expires_at": 1}, session=self._uow.session)
        if not doc:
            return -1
        exp: datetime | None = doc.get("expires_at")
        if not exp:
            return None
        delta = exp - _now_utc()
        return max(0, int(delta.total_seconds() * 1000))

    async def cas(self, key: str, expect: Any, update: Any, *, ttl_ms: int | None = None) -> bool:
        new_doc: dict[str, Any] = {"v": update}
        if ttl_ms is not None:
            new_doc["expires_at"] = _now_utc() + timedelta(milliseconds=ttl_ms)

        res = await self._col.update_one(
            {"_id": key, "v": expect},
            {"$set": new_doc},
            upsert=False,
            session=self._uow.session,
        )
        return res.modified_count == 1

    # ---- Locks ----

    async def acquire_lock(
        self,
        name: str,
        *,
        owner: str,
        ttl_ms: int,
        meta: Mapping[str, Any] | None = None,
    ) -> LockInfo | None:
        import secrets

        now_ms = _ms_from_dt(_now_utc())
        token = secrets.token_urlsafe(16)
        doc = await self._locks.find_one_and_update(
            {
                "_id": name,
                "$or": [
                    {"deadline_ms": {"$lte": now_ms}},
                    {"owner": owner},
                ],
            },
            {
                "$set": {
                    "owner": owner,
                    "token": token,
                    "deadline_ms": now_ms + int(ttl_ms),
                    "meta": dict(meta or {}),
                },
                "$setOnInsert": {"_id": name},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
            session=self._uow.session,
        )

        # If someone else holds a non-expired lock, filter won't match and doc is None
        if not doc or doc.get("owner") != owner or doc.get("token") != token:
            return None

        return LockInfo(
            name=name,
            owner=owner,
            token=token,
            deadline_ms=int(doc["deadline_ms"]),
            meta=doc.get("meta"),
        )

    async def renew_lock(self, name: str, *, owner: str, token: str, ttl_ms: int) -> bool:
        now_ms = _ms_from_dt(_now_utc())
        res = await self._locks.update_one(
            {"_id": name, "owner": owner, "token": token},
            {"$set": {"deadline_ms": now_ms + int(ttl_ms)}},
            session=self._uow.session,
        )
        return res.modified_count == 1

    async def release_lock(self, name: str, *, owner: str, token: str) -> bool:
        res = await self._locks.delete_one({"_id": name, "owner": owner, "token": token}, session=self._uow.session)
        return res.deleted_count == 1

    async def get_lock(self, name: str) -> LockInfo | None:
        doc = await self._locks.find_one({"_id": name}, session=self._uow.session)
        if not doc:
            return None
        return LockInfo(
            name=name,
            owner=str(doc.get("owner")),
            token=str(doc.get("token")),
            deadline_ms=int(doc.get("deadline_ms", 0)),
            meta=doc.get("meta"),
        )
