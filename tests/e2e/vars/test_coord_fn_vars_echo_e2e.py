"""
E2E (slow): scheduler-driven coordinator_fn → coordinator_fn chain.

Graph:
  n1: coordinator_fn (vars.set)     → routing.sla="gold", limits.max_batches=5
  n2: coordinator_fn (vars.echo)    → reads coordinator.vars and writes an artifact echo

Checks:
  - scheduler starts nodes automatically (no manual _run_coordinator_fn)
  - both nodes finish
  - echo artifact exists for n2 and contains an exact copy of coordinator.vars
  - updated_at present (and sane)
"""

from __future__ import annotations

import pytest
from tests.helpers.graph import wait_task_finished

from flowkit.protocol.messages import RunState

pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.vars]


@pytest.mark.asyncio
async def test_scheduler_runs_coordfn_chain_and_child_sees_vars(env_and_imports, coord, inmemory_db, tlog):

    async def vars_echo_adapter(task_id: str, *, target: dict | None = None, **_):
        tdoc = await inmemory_db.tasks.find_one({"id": task_id}, {"coordinator": 1})
        vars_copy = ((tdoc or {}).get("coordinator") or {}).get("vars", {})
        node_id = (target or {}).get("node_id") or "coordinator"
        await inmemory_db.artifacts.update_one(
            {"task_id": task_id, "node_id": node_id},
            {
                "$set": {
                    "status": "complete",
                    "meta": {"vars": vars_copy},
                    "updated_at": coord.clock.now_dt(),
                },
                "$setOnInsert": {
                    "task_id": task_id,
                    "node_id": node_id,
                    "attempt_epoch": 0,
                    "created_at": coord.clock.now_dt(),
                },
            },
            upsert=True,
        )
        return {"ok": True, "node_id": node_id, "size": len(vars_copy)}

    coord.adapters["vars.echo"] = vars_echo_adapter

    g = {
        "nodes": [
            {
                "node_id": "n1",
                "type": "coordinator_fn",
                "io": {"fn": "vars.set", "fn_args": {"kv": {"routing.sla": "gold", "limits.max_batches": 5}}},
            },
            {
                "node_id": "n2",
                "type": "coordinator_fn",
                "io": {"fn": "vars.echo", "fn_args": {"target": {}}},
                "depends_on": ["n1"],
            },
        ],
    }

    task_id = await coord.create_task(params={}, graph=g)
    final_doc = await wait_task_finished(inmemory_db, task_id, timeout=8.0)

    statuses = {n["node_id"]: n["status"] for n in final_doc["graph"]["nodes"]}
    assert statuses["n1"] == RunState.finished
    assert statuses["n2"] == RunState.finished

    art = await inmemory_db.artifacts.find_one({"task_id": task_id, "node_id": "n2"})
    assert art is not None, "echo artifact must exist for n2"
    meta_vars = (art.get("meta") or {}).get("vars") or {}
    assert meta_vars.get("routing", {}).get("sla") == "gold"
    assert meta_vars.get("limits", {}).get("max_batches") == 5
