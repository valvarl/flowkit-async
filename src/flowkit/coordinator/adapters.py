from __future__ import annotations
from typing import Any, Dict, List, Mapping, Callable

from ..core.time import SystemClock
from ..protocol.messages import RunState


class AdapterError(Exception):
    pass


class CoordinatorAdapters:
    """
    Generic functions the coordinator can call (in coordinator_fn nodes).
    They operate via injected `db`. Keep side-effects idempotent.
    """

    def __init__(self, *, db, clock: SystemClock | None = None) -> None:
        self.db = db
        self.clock = clock or SystemClock()

    async def merge_generic(self, task_id: str, from_nodes: List[str], target: Dict[str, Any]) -> Dict[str, Any]:
        if not target or not isinstance(target, dict):
            raise AdapterError("merge_generic: 'target' must be a dict with at least node_id")
        target_node = target.get("node_id") or "coordinator"

        partial_batches = 0
        complete_nodes = set()
        batch_uids = set()

        cur = self.db.artifacts.find({"task_id": task_id, "node_id": {"$in": from_nodes}})
        async for a in cur:
            st = a.get("status")
            if st == "complete":
                complete_nodes.add(a.get("node_id"))
            elif st == "partial":
                uid = a.get("batch_uid")
                if uid:
                    batch_uids.add(uid)
                partial_batches += 1

        meta = {
            "merged_from": from_nodes,
            "complete_nodes": sorted(list(complete_nodes)),
            "partial_batches": partial_batches,
            "distinct_batch_uids": len(batch_uids),
            "merged_at": self.clock.now_dt().isoformat(),
        }

        await self.db.artifacts.update_one(
            {"task_id": task_id, "node_id": target_node},
            {"$set": {"status": "complete", "meta": meta, "updated_at": self.clock.now_dt()},
             "$setOnInsert": {"task_id": task_id, "node_id": target_node, "attempt_epoch": 0, "created_at": self.clock.now_dt()}},
            upsert=True
        )
        return {"ok": True, "meta": meta}

    async def metrics_aggregate(self, task_id: str, node_id: str, *, mode: str = "sum") -> Dict[str, Any]:
        cur = self.db.metrics_raw.find({"task_id": task_id, "node_id": node_id, "failed": {"$ne": True}})
        acc: Dict[str, float] = {}
        cnt: Dict[str, int] = {}
        async for m in cur:
            for k, v in (m.get("metrics") or {}).items():
                try:
                    x = float(v)
                except Exception:
                    continue
                acc[k] = acc.get(k, 0.0) + x
                cnt[k] = cnt.get(k, 0) + 1

        out = {k: (acc[k] / max(1, cnt[k])) for k in acc} if mode == "mean" else {k: acc[k] for k in acc}

        await self.db.tasks.update_one(
            {"id": task_id, "graph.nodes.node_id": node_id},
            {"$set": {"graph.nodes.$.stats": out, "graph.nodes.$.stats_cached_at": self.clock.now_dt()}}
        )
        return {"ok": True, "mode": mode, "stats": out}

    async def noop(self, _: str, **__) -> Dict[str, Any]:
        return {"ok": True}


# default registry factory
def default_adapters(*, db, clock: SystemClock | None = None) -> Mapping[str, Callable[..., object]]:
    impl = CoordinatorAdapters(db=db, clock=clock)
    return {
        "merge.generic": impl.merge_generic,
        "metrics.aggregate": impl.metrics_aggregate,
        "noop": impl.noop,
    }
