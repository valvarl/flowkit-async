from __future__ import annotations

from typing import Any

from ..core.time import Clock


class ArtifactsWriter:
    def __init__(self, *, db, clock: Clock, task_id: str, node_id: str, attempt_epoch: int, worker_id: str):
        self.db = db
        self.clock = clock
        self.task_id = task_id
        self.node_id = node_id
        self.attempt_epoch = attempt_epoch
        self.worker_id = worker_id

    async def upsert_partial(self, batch_uid: str, meta: dict[str, Any]) -> dict[str, Any]:
        if not batch_uid:
            raise ValueError("batch_uid is required for partial artifact")
        filt = {"task_id": self.task_id, "node_id": self.node_id, "batch_uid": batch_uid}
        await self.db.artifacts.update_one(
            filt,
            {
                "$setOnInsert": {
                    "task_id": self.task_id,
                    "node_id": self.node_id,
                    "attempt_epoch": self.attempt_epoch,
                    "status": "partial",
                    "worker_id": self.worker_id,
                    "payload": None,
                    "created_at": self.clock.now_dt(),
                },
                "$set": {
                    "status": "partial",
                    "meta": meta or {},
                    "batch_uid": batch_uid,
                    "updated_at": self.clock.now_dt(),
                },
            },
            upsert=True,
        )
        return {"task_id": self.task_id, "node_id": self.node_id, "batch_uid": batch_uid}

    async def mark_complete(self, meta: dict[str, Any], artifacts_ref: dict[str, Any] | None = None) -> dict[str, Any]:
        await self.db.artifacts.update_one(
            {"task_id": self.task_id, "node_id": self.node_id},
            {
                "$set": {"status": "complete", "meta": meta, "updated_at": self.clock.now_dt()},
                "$setOnInsert": {
                    "attempt_epoch": self.attempt_epoch,
                    "worker_id": self.worker_id,
                    "created_at": self.clock.now_dt(),
                },
            },
            upsert=True,
        )
        return artifacts_ref or {"task_id": self.task_id, "node_id": self.node_id}
