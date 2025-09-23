# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
MongoDB implementation of TaskStore.
"""

from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from flowkit.protocol.messages import RunState
from flowkit.storage.tasks import LeaseInfo, TaskDoc, TaskStore

from .tx import MongoUoW


def _flatten(prefix: str, obj: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in obj.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, Mapping):
            out.update(_flatten(key, v))
        else:
            out[key] = v
    return out


class MongoTasks(TaskStore):
    """
    Collection layout: "tasks"
      _id: str (task_id)
      created_ms: int
      state: str
      attempt_epoch: int
      worker_id: str|null
      lease_id: str|null
      lease_deadline_ms: int|null
      params: dict
      vars: dict
      execution_plan: dict|null
      runtime_graph: dict|null
      progress: dict|null
      artifacts: dict|null
      meta: dict
      state_reason: str|null
    """

    def __init__(self, db: AsyncIOMotorDatabase, uow: MongoUoW) -> None:
        self._db = db
        self._col: AsyncIOMotorCollection = db["tasks"]
        self._uow = uow

    # ---- CRUD ----

    async def create(self, doc: TaskDoc) -> None:
        payload = {
            "_id": doc.task_id,
            "created_ms": int(doc.created_ms),
            "state": doc.state.value if isinstance(doc.state, RunState) else str(doc.state),
            "attempt_epoch": int(doc.attempt_epoch),
            "worker_id": doc.worker_id,
            "lease_id": doc.lease_id,
            "lease_deadline_ms": doc.lease_deadline_ms,
            "params": dict(doc.params),
            "vars": dict(doc.vars),
            "execution_plan": doc.execution_plan,
            "runtime_graph": doc.runtime_graph,
            "progress": doc.progress,
            "artifacts": doc.artifacts,
            "meta": dict(doc.meta),
        }
        # Idempotent "create": setOnInsert to avoid overwriting existing tasks
        await self._col.update_one(
            {"_id": doc.task_id},
            {"$setOnInsert": payload},
            upsert=True,
            session=self._uow.session,
        )

    async def get(self, task_id: str) -> TaskDoc | None:
        d = await self._col.find_one({"_id": task_id}, session=self._uow.session)
        return self._to_doc(d) if d else None

    async def update_fields(self, task_id: str, patch: Mapping[str, Any]) -> TaskDoc:
        # Shallow patch at top-level
        # Convert RunState enum if present
        patch2 = {k: (v.value if isinstance(v, RunState) else v) for k, v in patch.items()}
        d = await self._col.find_one_and_update(
            {"_id": task_id},
            {"$set": patch2},
            return_document=ReturnDocument.AFTER,
            session=self._uow.session,
        )
        if not d:
            raise KeyError(f"task not found: {task_id}")
        return self._to_doc(d)

    async def delete(self, task_id: str) -> None:
        await self._col.delete_one({"_id": task_id}, session=self._uow.session)

    # ---- Vars ----

    async def update_vars(self, task_id: str, ops: list[Mapping[str, Any]]) -> Mapping[str, Any]:
        set_ops: dict[str, Any] = {}
        inc_ops: dict[str, Any] = {}

        for op in ops or []:
            kind = op.get("op")
            key = op.get("key")
            if not isinstance(key, str) or not key:
                continue

            if kind == "set":
                set_ops[f"vars.{key}"] = op.get("value")
            elif kind == "merge":
                value = op.get("value") or {}
                if not isinstance(value, Mapping):
                    continue
                flat = _flatten(f"vars.{key}", value)  # deep merge by $set of subkeys
                set_ops.update(flat)
            elif kind == "incr":
                by = int(op.get("by", 1))
                inc_ops[f"vars.{key}"] = by

        update: dict[str, Any] = {}
        if set_ops:
            update["$set"] = set_ops
        if inc_ops:
            update["$inc"] = inc_ops

        if not update:
            d = await self._col.find_one({"_id": task_id}, session=self._uow.session)
        else:
            d = await self._col.find_one_and_update(
                {"_id": task_id},
                update,
                return_document=ReturnDocument.AFTER,
                session=self._uow.session,
            )

        if not d:
            raise KeyError(f"task not found: {task_id}")
        return d.get("vars", {})

    # ---- State / progress ----

    async def set_state(self, task_id: str, state: RunState, *, reason: str | None = None) -> None:
        update = {"state": state.value if isinstance(state, RunState) else str(state)}
        if reason is not None:
            update["state_reason"] = reason
        await self._col.update_one({"_id": task_id}, {"$set": update}, session=self._uow.session)

    async def update_progress(self, task_id: str, progress: Mapping[str, Any]) -> None:
        await self._col.update_one(
            {"_id": task_id},
            {"$set": {"progress": dict(progress)}},
            session=self._uow.session,
        )

    # ---- Leasing ----

    async def acquire_lease(self, task_id: str, *, worker_id: str, ttl_ms: int) -> LeaseInfo | None:
        deadline = await self._new_deadline(ttl_ms)
        lease_id = str(uuid4())

        d = await self._col.find_one_and_update(
            {
                "_id": task_id,
                "$or": [
                    {"lease_deadline_ms": {"$lte": deadline - 1}},  # expired or unset (None compares false)
                    {"lease_id": None},
                    {"worker_id": worker_id},
                ],
            },
            {
                "$set": {
                    "worker_id": worker_id,
                    "lease_id": lease_id,
                    "lease_deadline_ms": deadline,
                }
            },
            return_document=ReturnDocument.AFTER,
            session=self._uow.session,
        )
        if not d or d.get("worker_id") != worker_id or d.get("lease_id") != lease_id:
            return None
        return LeaseInfo(worker_id=worker_id, lease_id=lease_id, deadline_ms=deadline)

    async def renew_lease(self, task_id: str, *, worker_id: str, lease_id: str, ttl_ms: int) -> LeaseInfo | None:
        deadline = await self._new_deadline(ttl_ms)
        d = await self._col.find_one_and_update(
            {"_id": task_id, "worker_id": worker_id, "lease_id": lease_id},
            {"$set": {"lease_deadline_ms": deadline}},
            return_document=ReturnDocument.AFTER,
            session=self._uow.session,
        )
        if not d:
            return None
        return LeaseInfo(worker_id=worker_id, lease_id=lease_id, deadline_ms=deadline)

    async def release_lease(self, task_id: str, *, worker_id: str, lease_id: str) -> bool:
        res = await self._col.update_one(
            {"_id": task_id, "worker_id": worker_id, "lease_id": lease_id},
            {"$set": {"worker_id": None, "lease_id": None, "lease_deadline_ms": None}},
            session=self._uow.session,
        )
        return res.modified_count == 1

    # ---- Find with cursor ----

    async def find(
        self,
        *,
        state: RunState | None = None,
        owner: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[list[TaskDoc], str | None]:
        q: dict[str, Any] = {}
        if state is not None:
            q["state"] = state.value if isinstance(state, RunState) else str(state)
        if owner is not None:
            q["meta.owner"] = owner
        if cursor:
            q["_id"] = {"$gt": cursor}

        cur = self._col.find(q, session=self._uow.session).sort("_id", 1).limit(int(limit))
        items: list[TaskDoc] = []
        last_id: str | None = None
        async for d in cur:
            items.append(self._to_doc(d))
            last_id = d["_id"]
        next_cursor = last_id if (items and len(items) == int(limit)) else None
        return items, next_cursor

    # ---- helpers ----

    async def _new_deadline(self, ttl_ms: int) -> int:
        # Using server time where possible would be ideal; motor/pymongo simplifies to client time.
        import time

        return int(time.time() * 1000) + int(ttl_ms)

    def _to_doc(self, d: Mapping[str, Any]) -> TaskDoc:
        return TaskDoc(
            task_id=str(d["_id"]),
            created_ms=int(d.get("created_ms", 0)),
            state=RunState(d.get("state", RunState.queued)),
            attempt_epoch=int(d.get("attempt_epoch", 0)),
            worker_id=d.get("worker_id"),
            lease_id=d.get("lease_id"),
            lease_deadline_ms=d.get("lease_deadline_ms"),
            params=dict(d.get("params", {})),
            vars=dict(d.get("vars", {})),
            execution_plan=d.get("execution_plan"),
            runtime_graph=d.get("runtime_graph"),
            progress=d.get("progress"),
            artifacts=d.get("artifacts"),
            meta=dict(d.get("meta", {})),
        )
