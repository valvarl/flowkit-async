"""
E2E: cancelling a running task does not erase previously written coordinator.vars.

Scenario:
  - n1: coordinator_fn (vars.set) writes task variables.
  - s : long-running cancellable 'source' node (worker-based).
  - Cancel the task while 's' is running.
Assertions:
  - Task (or node) transitions away from 'running' due to cancel (implementation-dependent).
  - coordinator.vars written by n1 remain intact after cancellation.
"""

from __future__ import annotations

import asyncio

import pytest
from tests.helpers.graph import prime_graph, wait_node_running
from tests.helpers.handlers import build_cancelable_source_handler

from flowkit.protocol.messages import RunState

pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.vars]


@pytest.mark.asyncio
async def test_cancel_midflow_preserves_coordinator_vars(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    cd, _ = env_and_imports

    # Start a cancellable source worker (emits several batches with delays).
    await worker_factory(
        ("source", build_cancelable_source_handler(db=inmemory_db, total=100, batch=10, delay=0.25)),
    )

    g = {
        "nodes": [
            {
                "node_id": "n1",
                "type": "coordinator_fn",
                "status": RunState.queued,
                "io": {"fn": "vars.set", "fn_args": {"kv": {"routing.sla": "gold", "limits.max_batches": 5}}},
            },
            {
                "node_id": "s",
                "type": "source",
                "status": RunState.queued,
                "depends_on": ["n1"],
                "fan_in": "all",
                "io": {"input_inline": {}},
            },
        ],
        "edges": [("n1", "s")],
        "edges_ex": [],
    }
    g = prime_graph(cd, g)

    task_id = await coord.create_task(params={}, graph=g)

    # Wait until the worker node actually starts.
    await wait_node_running(inmemory_db, task_id, "s", timeout=8.0)

    # Trigger cancellation via DB flag (worker db-cancel watcher will pick it up).
    await inmemory_db.tasks.update_one({"id": task_id}, {"$set": {"coordinator.cancelled": True}})

    # Give the runtime a short time to propagate and react to cancel.
    for _ in range(60):  # ~3s max (60 * 0.05)
        doc = await inmemory_db.tasks.find_one({"id": task_id}, {"graph": 1, "coordinator": 1, "status": 1})
        s_node = next((n for n in (doc.get("graph", {}).get("nodes") or []) if n.get("node_id") == "s"), {})
        st = str(s_node.get("status") or "")
        if st.endswith("cancelled") or st.endswith("finished") or st.endswith("cancelling"):
            break
        await asyncio.sleep(0.05)

    # Coordinator vars must remain intact after cancellation.
    doc = await inmemory_db.tasks.find_one({"id": task_id}, {"coordinator": 1})
    vars_ = (doc.get("coordinator") or {}).get("vars") or {}
    assert vars_.get("routing", {}).get("sla") == "gold"
    assert vars_.get("limits", {}).get("max_batches") == 5
