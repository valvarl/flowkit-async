# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
MongoDB implementation of OutboxStore.

- enqueue(): idempotent by fingerprint (fp) when provided (unique, partial index)
- next_batch(): atomically reserves records via findOneAndUpdate with a short lock window
- mark_sent(), mark_failed(), schedule_retry(): state transitions
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from flowkit.storage.outbox import OutboxError, OutboxRecord, OutboxStore

from .tx import MongoUoW


class MongoOutbox(OutboxStore):
    """
    Collection layout: "outbox"
      _id: ObjectId
      topic: str
      key: str|null
      envelope: dict
      attempts: int
      status: "pending"|"retry"|"inflight"|"sent"|"failed"
      next_attempt_at_ms: int
      locked_until_ms: int|null
      fp: str|null                # idempotency fingerprint
      error: str|null
      created_ms: int
      sent_ms: int|null
      failed_ms: int|null
    """

    def __init__(self, db: AsyncIOMotorDatabase, uow: MongoUoW, *, lock_ms: int = 30_000) -> None:
        self._db = db
        self._col: AsyncIOMotorCollection = db["outbox"]
        self._uow = uow
        self._lock_ms = int(lock_ms)

    async def enqueue(self, *, topic: str, key: str | None, envelope: Mapping[str, Any], fp: str | None) -> None:
        import time

        now_ms = int(time.time() * 1000)
        doc = {
            "topic": topic,
            "key": key,
            "envelope": dict(envelope),
            "attempts": 0,
            "status": "pending",
            "next_attempt_at_ms": now_ms,
            "locked_until_ms": None,
            "fp": fp,
            "error": None,
            "created_ms": now_ms,
            "sent_ms": None,
            "failed_ms": None,
        }
        if fp:
            # Idempotent insert by fp (unique partial index)
            # If duplicate, do nothing silently (idempotency).
            try:
                await self._col.insert_one(doc, session=self._uow.session)
            except Exception:
                # duplicate key on fp → treat as success
                return
        else:
            await self._col.insert_one(doc, session=self._uow.session)

    async def next_batch(self, *, now_ms: int, limit: int) -> AsyncIterator[OutboxRecord]:  # type: ignore[override]
        """
        Atomically reserve up to `limit` records in stable order:
          status in {pending,retry}
          next_attempt_at_ms <= now_ms
          (not locked or lock expired)
        """
        # Pull one-by-one to honor sort and avoid large transactions.
        # Each reservation sets locked_until_ms to now_ms + lock_ms.
        for _ in range(int(limit)):
            d = await self._col.find_one_and_update(
                {
                    "status": {"$in": ["pending", "retry"]},
                    "next_attempt_at_ms": {"$lte": int(now_ms)},
                    "$or": [
                        {"locked_until_ms": None},
                        {"locked_until_ms": {"$lte": int(now_ms)}},
                    ],
                },
                {
                    "$set": {
                        "locked_until_ms": int(now_ms) + self._lock_ms,
                        "status": "inflight",
                    }
                },
                sort=[("next_attempt_at_ms", 1), ("_id", 1)],
                return_document=ReturnDocument.AFTER,
                session=self._uow.session,
            )
            if not d:
                break
            yield OutboxRecord(
                rec_id=d["_id"],
                topic=str(d["topic"]),
                key=d.get("key"),
                envelope=d.get("envelope") or {},
                attempts=int(d.get("attempts") or 0),
            )

    async def mark_sent(self, rec_id: Any) -> None:
        import time

        now_ms = int(time.time() * 1000)
        res = await self._col.update_one(
            {"_id": rec_id},
            {"$set": {"status": "sent", "sent_ms": now_ms, "locked_until_ms": None}},
            session=self._uow.session,
        )
        if res.matched_count != 1:
            raise OutboxError("record not found")

    async def mark_failed(self, rec_id: Any, *, attempts: int, error: str) -> None:
        import time

        now_ms = int(time.time() * 1000)
        res = await self._col.update_one(
            {"_id": rec_id},
            {
                "$set": {
                    "status": "failed",
                    "attempts": int(attempts),
                    "error": str(error),
                    "failed_ms": now_ms,
                    "locked_until_ms": None,
                }
            },
            session=self._uow.session,
        )
        if res.matched_count != 1:
            raise OutboxError("record not found")

    async def schedule_retry(self, rec_id: Any, *, attempts: int, next_attempt_at_ms: int, error: str) -> None:
        res = await self._col.update_one(
            {"_id": rec_id},
            {
                "$set": {
                    "status": "retry",
                    "attempts": int(attempts),
                    "next_attempt_at_ms": int(next_attempt_at_ms),
                    "error": str(error),
                    "locked_until_ms": None,
                }
            },
            session=self._uow.session,
        )
        if res.matched_count != 1:
            raise OutboxError("record not found")
