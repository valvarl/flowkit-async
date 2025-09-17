"""
XFails (placeholder): conditional routing by coordinator.vars is not implemented yet.

Intended behavior (when implemented):
  - n1 sets coordinator.vars.routing.sla = "gold".
  - Downstream 'gold_only' should run when sla == "gold".
  - Downstream 'silver_only' should NOT run in this case.

Current status:
  - Marked xfail until coordinator supports variable-driven conditional routing.
"""

from __future__ import annotations

import pytest
from tests.helpers.graph import wait_task_finished

pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.vars]


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Conditional routing by coordinator.vars is not implemented yet", strict=False)
async def test_conditional_routing_by_vars(env_and_imports, coord, inmemory_db):
    # Tiny adapter to mark execution through an artifact (so we can assert which branch ran).
    async def mark_adapter(task_id: str, target: dict | None = None, tag: str | None = None, **_):
        node_id = (target or {}).get("node_id") or tag or "mark"
        await inmemory_db.artifacts.update_one(
            {"task_id": task_id, "node_id": node_id},
            {
                "$set": {"status": "complete", "meta": {"mark": tag or node_id}, "updated_at": coord.clock.now_dt()},
                "$setOnInsert": {
                    "task_id": task_id,
                    "node_id": node_id,
                    "attempt_epoch": 0,
                    "created_at": coord.clock.now_dt(),
                },
            },
            upsert=True,
        )
        return {"ok": True}

    coord.adapters["vars.mark"] = mark_adapter

    g = {
        "nodes": [
            {
                "node_id": "set_vars",
                "type": "coordinator_fn",
                "io": {"fn": "vars.set", "fn_args": {"kv": {"routing.sla": "gold"}}},
            },
            # The two branches below are conceptual; coordinator should choose based on vars.
            {
                "node_id": "gold_only",
                "type": "coordinator_fn",
                "io": {"fn": "vars.mark", "fn_args": {"target": {}, "tag": "gold"}},
                "depends_on": ["set_vars"],
            },
            {
                "node_id": "silver_only",
                "type": "coordinator_fn",
                "io": {"fn": "vars.mark", "fn_args": {"target": {}, "tag": "silver"}},
                "depends_on": ["set_vars"],
            },
        ],
    }

    tid = await coord.create_task(params={}, graph=g)
    _ = await wait_task_finished(inmemory_db, tid, timeout=8.0)

    # Expected (once implemented): only 'gold_only' runs.
    art_gold = await inmemory_db.artifacts.find_one({"task_id": tid, "node_id": "gold_only"})
    art_silver = await inmemory_db.artifacts.find_one({"task_id": tid, "node_id": "silver_only"})
    assert art_gold is not None
    assert art_silver is None
