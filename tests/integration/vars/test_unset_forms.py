from __future__ import annotations

import pytest

from flowkit.coordinator.runner import Coordinator
from flowkit.core.time import ManualClock
from flowkit.protocol.messages import RunState

pytestmark = [pytest.mark.integration, pytest.mark.vars]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "initial_kv,keys_to_unset,expected_remaining",
    [
        ({"a.b": 1, "c.d": 2, "e.f": 3}, ["a.b", "c.d"], {"e": {"f": 3}}),
        ({"x.y": 10, "z.w": 20}, ["x.y"], {"z": {"w": 20}}),
    ],
)
async def test_vars_unset_kwargs_list_in_coord_fn(inmemory_db, initial_kv, keys_to_unset, expected_remaining):
    """
    Coordinator function 'vars.unset' must accept kwargs form `keys=[...]`.
    """
    clock = ManualClock(start_ms=0)
    coord = Coordinator(db=inmemory_db, clock=clock)

    g = {
        "nodes": [
            {
                "node_id": "set",
                "type": "coordinator_fn",
                "status": RunState.queued,
                "io": {"fn": "vars.set", "fn_args": {"kv": initial_kv}},
            },
            {
                "node_id": "unset",
                "type": "coordinator_fn",
                "status": RunState.queued,
                "io": {"fn": "vars.unset", "fn_args": {"keys": list(keys_to_unset)}},
                "depends_on": ["set"],
            },
        ],
        "edges": [("set", "unset")],
        "edges_ex": [],
    }

    tid = await coord.create_task(params={}, graph=g)

    # Run sequentially without the scheduler.
    await coord._run_coordinator_fn({"id": tid, "graph": g}, g["nodes"][0])
    await coord._run_coordinator_fn({"id": tid, "graph": g}, g["nodes"][1])

    # Verify both nodes finished.
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"graph": 1, "coordinator": 1})
    assert all(n["status"] == RunState.finished for n in doc["graph"]["nodes"])

    # Verify remaining vars match expectation (keys were removed).
    v = (doc.get("coordinator") or {}).get("vars") or {}
    assert v == expected_remaining
