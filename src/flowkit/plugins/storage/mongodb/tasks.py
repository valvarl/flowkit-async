# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
MongoDB implementation of TaskStore with per-node collection.

Collections:

  tasks:
    _id: str (task_id)
    schema_version: str
    params: dict
    plan: dict
    runtime: dict
    status: str ("queued" | "running" | "deferred" | "cancelling" | "finished" | "failed")
    created_ms: int
    last_event_ms: int
    meta: dict
    reason: str|null

  task_nodes:
    _id: str (f"{task_id}:{node_id}")
    task_id: str
    node_id: str
    type: str
    parents: [str]
    io: dict|null
    concurrency: dict|null
    status: str (same domain as tasks.status)
    attempt_epoch: int
    lease: {worker_id:str, lease_id:str, deadline_ms:int}|null
    reason: str|null
    updated_ms: int

Design notes:
- Node documents are created on task creation from runtime/plan slices.
- All state transitions are done with findOneAndUpdate filters to ensure CAS.
- 'iter_active' and 'is_task_complete' treat 'finished' and 'failed' as terminal.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from flowkit.protocol.messages import RunState
from flowkit.storage.tasks import LeaseInfo, NodeSnapshot, TaskDoc, TaskStore

from .tx import MongoUoW


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


class MongoTasks(TaskStore):

    def __init__(self, db: AsyncIOMotorDatabase, uow: MongoUoW) -> None:
        self._db = db
        self._col_tasks: AsyncIOMotorCollection = db["tasks"]
        self._col_nodes: AsyncIOMotorCollection = db["task_nodes"]
        self._uow = uow

    # --------------------------------------------------------------------- #
    # Task CRUD
    # --------------------------------------------------------------------- #

    async def create(self, doc: TaskDoc) -> None:
        """
        Idempotent task creation:
        - upsert task doc (setOnInsert)
        - initialize task_nodes from runtime + plan (concurrency if available)
        """
        payload = {
            "_id": doc.task_id,
            "schema_version": doc.schema_version,
            "params": dict(doc.params or {}),
            "plan": dict(doc.plan or {}),
            "runtime": dict(doc.runtime or {}),
            "status": str(doc.status),
            "created_ms": int(doc.created_ms or _now_ms()),
            "last_event_ms": int(doc.last_event_ms or 0),
            "meta": dict(doc.meta or {}),
        }
        await self._col_tasks.update_one(
            {"_id": doc.task_id},
            {"$setOnInsert": payload},
            upsert=True,
            session=self._uow.session,
        )

        # Initialize per-node docs (idempotent upserts)
        runtime_nodes = (doc.runtime or {}).get("nodes", [])
        pol = (doc.plan or {}).get("policies", {})
        for n in runtime_nodes:
            nid = str(n.get("node_id"))
            if not nid:
                continue
            node_payload = {
                "_id": f"{doc.task_id}:{nid}",
                "task_id": doc.task_id,
                "node_id": nid,
                "type": str(n.get("type") or ""),
                "parents": list(n.get("parents") or []),
                "io": n.get("io") or {},
                "concurrency": (pol.get(nid) or {}).get("concurrency"),
                "status": RunState.queued.value,
                "attempt_epoch": 0,
                "lease": None,
                "reason": None,
                "updated_ms": _now_ms(),
            }
            await self._col_nodes.update_one(
                {"_id": node_payload["_id"]},
                {"$setOnInsert": node_payload},
                upsert=True,
                session=self._uow.session,
            )

    async def get(self, task_id: str) -> TaskDoc | None:
        d = await self._col_tasks.find_one({"_id": task_id}, session=self._uow.session)
        return self._to_doc(d) if d else None

    async def update_fields(self, task_id: str, patch: Mapping[str, Any]) -> TaskDoc:
        # Shallow patch for top-level fields
        d = await self._col_tasks.find_one_and_update(
            {"_id": task_id},
            {"$set": dict(patch)},
            return_document=ReturnDocument.AFTER,
            session=self._uow.session,
        )
        if not d:
            raise KeyError(f"task not found: {task_id}")
        return self._to_doc(d)

    async def delete(self, task_id: str) -> None:
        await self._col_tasks.delete_one({"_id": task_id}, session=self._uow.session)
        await self._col_nodes.delete_many({"task_id": task_id}, session=self._uow.session)

    # --------------------------------------------------------------------- #
    # Task scanning / liveness
    # --------------------------------------------------------------------- #

    async def iter_active(self) -> AsyncIterator[TaskDoc]:
        cur = self._col_tasks.find(
            {"status": {"$nin": [RunState.finished.value, RunState.failed.value]}},
            session=self._uow.session,
        ).sort([("_id", 1)])
        async for d in cur:
            yield self._to_doc(d)

    async def touch_event(self, task_id: str) -> None:
        await self._col_tasks.update_one(
            {"_id": task_id},
            {"$set": {"last_event_ms": _now_ms()}},
            session=self._uow.session,
        )

    async def ms_since_last_event(self, task_id: str) -> int:
        import math

        d = await self._col_tasks.find_one({"_id": task_id}, projection={"last_event_ms": 1}, session=self._uow.session)
        if not d:
            # Treat as "very stale"
            return math.inf  # type: ignore[return-value]
        last = int(d.get("last_event_ms") or 0)
        if last <= 0:
            return 1 << 31  # effectively "infinite" for timeout checks
        return max(0, _now_ms() - last)

    # --------------------------------------------------------------------- #
    # High-level task transitions
    # --------------------------------------------------------------------- #

    async def bump_running(self, task_id: str) -> None:
        await self._col_tasks.update_one(
            {"_id": task_id, "status": RunState.queued.value},
            {"$set": {"status": RunState.running.value}},
            session=self._uow.session,
        )

    async def is_task_complete(self, task_id: str) -> bool:
        active = await self._col_nodes.count_documents(
            {
                "task_id": task_id,
                "status": {"$nin": [RunState.finished.value, RunState.failed.value]},
            },
            session=self._uow.session,
        )
        return int(active) == 0

    async def finish_task(self, task_id: str) -> None:
        await self._col_tasks.update_one(
            {"_id": task_id},
            {"$set": {"status": RunState.finished.value, "reason": None}},
            session=self._uow.session,
        )

    async def fail_task(self, task_id: str, *, reason: str) -> None:
        await self._col_tasks.update_one(
            {"_id": task_id},
            {"$set": {"status": RunState.failed.value, "reason": str(reason)}},
            session=self._uow.session,
        )

    async def defer_task(self, task_id: str, *, reason: str) -> None:
        await self._col_tasks.update_one(
            {"_id": task_id},
            {"$set": {"status": RunState.deferred.value, "reason": str(reason)}},
            session=self._uow.session,
        )

    # --------------------------------------------------------------------- #
    # Node listing / lookup
    # --------------------------------------------------------------------- #

    async def iter_task_nodes(self, task_id: str) -> AsyncIterator[NodeSnapshot]:
        cur = self._col_nodes.find({"task_id": task_id}, session=self._uow.session).sort([("node_id", 1)])
        async for d in cur:
            yield self._to_node(d)

    async def get_node(self, task_id: str, node_id: str) -> NodeSnapshot | None:
        d = await self._col_nodes.find_one(
            {"_id": f"{task_id}:{node_id}"},
            session=self._uow.session,
        )
        return self._to_node(d) if d else None

    # --------------------------------------------------------------------- #
    # Node lifecycle
    # --------------------------------------------------------------------- #

    async def begin_node(self, task_id: str, node_id: str) -> tuple[bool, int, LeaseInfo | None]:
        """
        Attempt to transition a node from queued/deferred -> running.
        Increments attempt_epoch atomically and clears any previous lease.
        """
        d = await self._col_nodes.find_one_and_update(
            {
                "_id": f"{task_id}:{node_id}",
                "status": {"$in": [RunState.queued.value, RunState.deferred.value]},
            },
            {
                "$inc": {"attempt_epoch": 1},
                "$set": {"status": RunState.running.value, "lease": None, "reason": None, "updated_ms": _now_ms()},
            },
            return_document=ReturnDocument.AFTER,
            session=self._uow.session,
        )
        if not d:
            # Could not transition due to state mismatch (someone else started it)
            cur = await self._col_nodes.find_one({"_id": f"{task_id}:{node_id}"}, session=self._uow.session)
            new_epoch = int((cur or {}).get("attempt_epoch") or 0)
            return False, new_epoch, None
        return True, int(d.get("attempt_epoch") or 0), None

    async def finish_node(self, task_id: str, node_id: str) -> None:
        await self._col_nodes.update_one(
            {"_id": f"{task_id}:{node_id}"},
            {"$set": {"status": RunState.finished.value, "lease": None, "updated_ms": _now_ms()}},
            session=self._uow.session,
        )

    async def defer_node(self, task_id: str, node_id: str, *, reason: str) -> None:
        await self._col_nodes.update_one(
            {"_id": f"{task_id}:{node_id}"},
            {"$set": {"status": RunState.deferred.value, "reason": str(reason), "updated_ms": _now_ms()}},
            session=self._uow.session,
        )

    async def fail_node(self, task_id: str, node_id: str, *, reason: str) -> None:
        await self._col_nodes.update_one(
            {"_id": f"{task_id}:{node_id}"},
            {"$set": {"status": RunState.failed.value, "reason": str(reason), "updated_ms": _now_ms()}},
            session=self._uow.session,
        )

    async def mark_cancelling(self, task_id: str, node_id: str, *, reason: str) -> None:
        await self._col_nodes.update_one(
            {"_id": f"{task_id}:{node_id}"},
            {"$set": {"status": RunState.cancelling.value, "reason": str(reason), "updated_ms": _now_ms()}},
            session=self._uow.session,
        )

    async def update_lease(
        self,
        *,
        task_id: str,
        node_id: str,
        worker_id: str,
        lease_id: str,
        deadline_ts_ms: int,
    ) -> None:
        await self._col_nodes.update_one(
            {"_id": f"{task_id}:{node_id}"},
            {
                "$set": {
                    "lease": {
                        "worker_id": worker_id,
                        "lease_id": lease_id,
                        "deadline_ms": int(deadline_ts_ms),
                    },
                    "updated_ms": _now_ms(),
                }
            },
            session=self._uow.session,
        )

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #

    def _to_doc(self, d: Mapping[str, Any]) -> TaskDoc:
        return TaskDoc(
            task_id=str(d["_id"]),
            schema_version=str(d.get("schema_version", "")),
            params=dict(d.get("params") or {}),
            plan=dict(d.get("plan") or {}),
            runtime=dict(d.get("runtime") or {}),
            status=str(d.get("status") or RunState.queued.value),
            created_ms=int(d.get("created_ms") or 0),
            last_event_ms=int(d.get("last_event_ms") or 0),
            meta=dict(d.get("meta") or {}),
        )

    def _to_node(self, d: Mapping[str, Any]) -> NodeSnapshot:
        lease_doc = d.get("lease") or None
        lease = (
            LeaseInfo(
                worker_id=str(lease_doc["worker_id"]),
                lease_id=str(lease_doc["lease_id"]),
                deadline_ms=int(lease_doc["deadline_ms"]),
            )
            if lease_doc
            else None
        )
        return NodeSnapshot(
            task_id=str(d.get("task_id") or ""),
            node_id=str(d.get("node_id") or ""),
            type=str(d.get("type") or ""),
            parents=list(d.get("parents") or []),
            io=dict(d.get("io") or {}),
            concurrency=(d.get("concurrency") or None),
            status=str(d.get("status") or RunState.queued.value),
            attempt_epoch=int(d.get("attempt_epoch") or 0),
            lease=lease,
        )
