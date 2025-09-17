from __future__ import annotations

import itertools

import pytest

from flowkit.coordinator.runner import Coordinator
from flowkit.core.time import ManualClock
from flowkit.protocol.messages import RunState

pytestmark = [pytest.mark.integration, pytest.mark.vars]


async def _updated_at(db, tid: str):
    doc = await db.tasks.find_one({"id": tid}, {"updated_at": 1})
    return (doc or {}).get("updated_at")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sla,max_batches,flags,incr_by",
    [
        ("gold", 5, {"ab": False, "beta": True}, 3),
        ("silver", 9, {"ab": True, "beta": False}, 7),
    ],
)
async def test_coord_fn_chain_set_merge_incr_unset(inmemory_db, sla, max_batches, flags, incr_by):
    """
    Integration scenario:
      n1: vars.set   → routing.sla=<param>, limits.max_batches=<param>
      n2: vars.merge → flags=<param>
      n3: vars.incr  → counters.pages += <param>
      n4: vars.unset → limits.max_batches

    Verifies:
      - each node finishes (status=finished)
      - final coordinator.vars matches expected
      - updated_at increases across steps (if tracked by inmemory_db)
    """
    clock = ManualClock(start_ms=0)
    coord = Coordinator(db=inmemory_db, clock=clock)

    g = {
        "nodes": [
            {
                "node_id": "n1",
                "type": "coordinator_fn",
                "io": {"fn": "vars.set", "fn_args": {"kv": {"routing.sla": sla, "limits.max_batches": max_batches}}},
            },
            {
                "node_id": "n2",
                "depends_on": ["n1"],
                "type": "coordinator_fn",
                "io": {"fn": "vars.merge", "fn_args": {"data": {"flags": flags}}},
            },
            {
                "node_id": "n3",
                "depends_on": ["n2"],
                "type": "coordinator_fn",
                "io": {"fn": "vars.incr", "fn_args": {"key": "counters.pages", "by": incr_by}},
            },
            {
                "node_id": "n4",
                "depends_on": ["n3"],
                "type": "coordinator_fn",
                "io": {"fn": "vars.unset", "fn_args": {"keys": ["limits.max_batches"]}},
            },
        ],
    }

    tid = await coord.create_task(params={}, graph=g)

    async def _node(nid: str):
        doc = await inmemory_db.tasks.find_one({"id": tid}, {"graph": 1})
        return next(n for n in (doc["graph"]["nodes"] or []) if n["node_id"] == nid)

    updated = []

    node = await _node("n1")
    await coord._run_coordinator_fn({"id": tid, "graph": g}, node)
    updated.append(await _updated_at(inmemory_db, tid))
    await clock.sleep_ms(1)

    node = await _node("n2")
    await coord._run_coordinator_fn({"id": tid, "graph": g}, node)
    updated.append(await _updated_at(inmemory_db, tid))
    await clock.sleep_ms(1)

    node = await _node("n3")
    await coord._run_coordinator_fn({"id": tid, "graph": g}, node)
    updated.append(await _updated_at(inmemory_db, tid))
    await clock.sleep_ms(1)

    node = await _node("n4")
    await coord._run_coordinator_fn({"id": tid, "graph": g}, node)
    updated.append(await _updated_at(inmemory_db, tid))

    final_doc = await inmemory_db.tasks.find_one({"id": tid}, {"graph": 1, "coordinator": 1})
    assert all(n["status"] == RunState.finished for n in final_doc["graph"]["nodes"])

    vars_ = (final_doc.get("coordinator") or {}).get("vars") or {}
    assert vars_.get("routing", {}).get("sla") == sla
    assert vars_.get("flags", {}).get("ab") == flags["ab"]
    assert vars_.get("flags", {}).get("beta") == flags["beta"]
    assert vars_.get("counters", {}).get("pages") == incr_by
    assert "max_batches" not in (vars_.get("limits") or {})

    ups = [u for u in updated if u is not None]
    if len(ups) >= 2:
        for prev, cur in itertools.pairwise(ups):
            assert cur > prev, f"updated_at must increase: {prev!r} → {cur!r}"
