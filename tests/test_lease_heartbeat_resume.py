"""
Heartbeat/grace-window/resume tests.

Covers:
- Soft heartbeat -> deferred -> recovery
- Hard heartbeat -> task failed
- Resume with local worker state (no new epoch)
- Discover w/ complete artifacts -> skip start
- Grace-gate delay, and that deferred retry ignores the gate
- Heartbeat extends lease deadline
"""

from __future__ import annotations

import asyncio

import pytest

from tests.helpers import wait_task_finished
from tests.helpers.graph import make_graph, node_by_id, prime_graph
from tests.helpers.handlers import (
    build_flaky_once_handler,
    build_noop_query_only_role,
    build_sleepy_handler,
)

# Restrict available roles for this module
pytestmark = pytest.mark.worker_types("sleepy,noop,flaky")


@pytest.mark.cfg(coord={"heartbeat_soft_sec": 0.4, "heartbeat_hard_sec": 5.0}, worker={"hb_interval_sec": 1.0})
@pytest.mark.asyncio
async def test_heartbeat_soft_deferred_then_recovers(env_and_imports, inmemory_db, coord, worker_factory):
    """
    With a short soft heartbeat window the task should become DEFERRED, then recover and finish.
    """
    cd, _ = env_and_imports

    # Sleepy role: long processing so heartbeats lag behind soft window.
    await worker_factory(("sleepy", build_sleepy_handler(db=inmemory_db, role="sleepy", batches=1, sleep_s=1.6)))

    graph = prime_graph(
        cd,
        make_graph(
            nodes=[{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    async def saw_deferred(timeout=3.0) -> bool:  # noqa: ASYNC109
        from time import time

        t0 = time()
        while time() - t0 < timeout:
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t and str(t.get("status")) == str(cd.RunState.deferred):
                return True
            await asyncio.sleep(0.03)
        return False

    assert await saw_deferred(), "expected task to become deferred on SOFT heartbeat"
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=8.0)
    assert str(tdoc.get("status")) == str(cd.RunState.finished)
    node = node_by_id(tdoc, "s")
    assert str(node.get("status")) == str(cd.RunState.finished)


@pytest.mark.cfg(coord={"heartbeat_soft_sec": 0.2, "heartbeat_hard_sec": 0.5}, worker={"hb_interval_sec": 10.0})
@pytest.mark.asyncio
async def test_heartbeat_hard_marks_task_failed(env_and_imports, inmemory_db, coord, worker_factory):
    """
    If hard heartbeat window is exceeded, the task should be marked FAILED.
    """
    cd, _ = env_and_imports

    await worker_factory(("sleepy", build_sleepy_handler(db=inmemory_db, role="sleepy", batches=1, sleep_s=1.2)))

    graph = prime_graph(
        cd,
        make_graph(
            nodes=[{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    # Wait for FAILED due to hard window
    from time import time

    t0 = time()
    while time() - t0 < 4.0:
        t = await inmemory_db.tasks.find_one({"id": task_id})
        if t and str(t.get("status")) == str(cd.RunState.failed):
            break
        await asyncio.sleep(0.03)
    else:
        raise AssertionError("expected HARD heartbeat to mark task failed")

    t = await inmemory_db.tasks.find_one({"id": task_id})
    node = node_by_id(t, "s")
    assert str(node.get("status")) != str(cd.RunState.finished)


@pytest.mark.cfg(coord={"heartbeat_soft_sec": 30, "heartbeat_hard_sec": 60}, worker={"hb_interval_sec": 100})
@pytest.mark.asyncio
async def test_resume_inflight_worker_restarts_with_local_state(
    env_and_imports, inmemory_db, coord, worker_cfg, monkeypatch, tmp_path
):
    """
    When a worker restarts with the same WORKER_ID and persisted local state,
    the coordinator should adopt the inflight work without bumping attempt_epoch.
    """
    cd, wu = env_and_imports

    worker_id = "w-resume"
    monkeypatch.setenv("WORKER_ID", worker_id)
    monkeypatch.setenv("WORKER_STATE_DIR", str(tmp_path))

    w1 = wu.Worker(
        db=inmemory_db,
        cfg=worker_cfg,
        roles=["sleepy"],
        handlers={"sleepy": build_sleepy_handler(db=inmemory_db, role="sleepy", batches=1, sleep_s=2.0)},
    )
    await w1.start()

    graph = prime_graph(
        cd,
        make_graph(
            nodes=[{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    async def wait_running() -> int:
        from time import time

        t0 = time()
        while time() - t0 < 2.5:
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t:
                n = node_by_id(t, "s")
                if str(n.get("status")) == str(cd.RunState.running):
                    return int(n.get("attempt_epoch", 0))
            await asyncio.sleep(0.03)
        raise AssertionError("node did not reach running in time")

    epoch_before = await wait_running()

    await w1.stop()

    w2 = wu.Worker(
        db=inmemory_db,
        cfg=worker_cfg,
        roles=["sleepy"],
        handlers={"sleepy": build_sleepy_handler(db=inmemory_db, role="sleepy", batches=1, sleep_s=0.1)},
    )
    await w2.start()

    await asyncio.sleep(0.5)
    t = await inmemory_db.tasks.find_one({"id": task_id})
    n = node_by_id(t, "s")
    epoch_after = int(n.get("attempt_epoch", 0))
    assert epoch_after == epoch_before, "coordinator should adopt inflight without starting a new attempt"
    await w2.stop()


@pytest.mark.asyncio
async def test_task_discover_complete_artifacts_skips_node_start(env_and_imports, inmemory_db, coord, worker_factory):
    """
    If artifacts for a node are already marked 'complete' during discovery,
    the node should be auto-finished without starting its handler.
    """
    cd, _ = env_and_imports

    await worker_factory(("noop", build_noop_query_only_role(db=inmemory_db, role="noop")))

    graph = prime_graph(
        cd,
        make_graph(
            nodes=[{"node_id": "x", "type": "noop", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    # Mark artifacts as 'complete' before scheduler picks it up
    await inmemory_db.artifacts.update_one(
        {"task_id": task_id, "node_id": "x"}, {"$set": {"status": "complete"}}, upsert=True
    )

    await asyncio.sleep(0.3)

    t = await inmemory_db.tasks.find_one({"id": task_id})
    node = node_by_id(t, "x")
    assert str(node.get("status")) == str(cd.RunState.finished), "node should finish without start"


@pytest.mark.cfg(
    coord={"discovery_window_sec": 1.0, "scheduler_tick_sec": 0.05, "heartbeat_soft_sec": 30, "heartbeat_hard_sec": 60}
)
@pytest.mark.asyncio
async def test_grace_gate_blocks_then_allows_after_window(env_and_imports, inmemory_db, coord, worker_factory):
    """
    Discovery grace window should delay start initially and allow it after the window elapses.
    """
    cd, _ = env_and_imports

    await worker_factory(("noop", build_noop_query_only_role(db=inmemory_db, role="noop")))

    graph = prime_graph(
        cd,
        make_graph(
            nodes=[{"node_id": "x", "type": "noop", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    # Shortly after creation the node should not be running due to grace-gate
    await asyncio.sleep(0.2)
    t = await inmemory_db.tasks.find_one({"id": task_id})
    node = node_by_id(t, "x")
    assert str(node.get("status")) != str(cd.RunState.running), "expected grace-gate to delay start"

    # After the window, it should start (or even finish)
    from time import time

    t0 = time()
    started = False
    while time() - t0 < 2.0:
        t = await inmemory_db.tasks.find_one({"id": task_id})
        node = node_by_id(t, "x")
        if str(node.get("status")) in (str(cd.RunState.running), str(cd.RunState.finished)):
            started = True
            break
        await asyncio.sleep(0.05)
    assert started, "expected coordinator to start after window"


@pytest.mark.cfg(
    coord={
        "discovery_window_sec": 5.0,
        "scheduler_tick_sec": 0.05,
        "finalizer_tick_sec": 0.05,
        "heartbeat_soft_sec": 30,
        "heartbeat_hard_sec": 60,
    }
)
@pytest.mark.asyncio
async def test_deferred_retry_ignores_grace_gate(env_and_imports, inmemory_db, coord, worker_factory):
    """
    A DEFERRED retry should not be throttled by discovery grace window.
    """
    cd, _ = env_and_imports

    await worker_factory(("flaky", build_flaky_once_handler(db=inmemory_db)))

    graph = prime_graph(
        cd,
        make_graph(
            nodes=[
                {
                    "node_id": "f",
                    "type": "flaky",
                    "depends_on": [],
                    "fan_in": "all",
                    "retry_policy": {"max": 2, "backoff_sec": 0, "permanent_on": []},
                    "io": {"input_inline": {}},
                }
            ],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=6.0)
    assert str(tdoc.get("status")) == str(cd.RunState.finished)
    node = node_by_id(tdoc, "f")
    assert str(node.get("status")) == str(cd.RunState.finished)


@pytest.mark.cfg(coord={"heartbeat_soft_sec": 30, "heartbeat_hard_sec": 60})
@pytest.mark.asyncio
async def test_no_task_resumed_on_worker_restart(
    env_and_imports, inmemory_db, coord, worker_cfg, monkeypatch, tmp_path
):
    """
    On a cold worker restart there should be no TASK_RESUMED event emitted by the worker.
    """
    cd, wu = env_and_imports

    worker_id = "w-nores"
    monkeypatch.setenv("WORKER_ID", worker_id)
    monkeypatch.setenv("WORKER_STATE_DIR", str(tmp_path))

    w1 = wu.Worker(
        db=inmemory_db,
        cfg=worker_cfg,
        roles=["sleepy"],
        handlers={"sleepy": build_sleepy_handler(db=inmemory_db, role="sleepy", batches=1, sleep_s=1.0)},
    )
    await w1.start()

    graph = prime_graph(
        cd,
        make_graph(
            nodes=[{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    # Wait until running
    from time import time

    t0 = time()
    while time() - t0 < 2.5:
        t = await inmemory_db.tasks.find_one({"id": task_id})
        if t and str(node_by_id(t, "s").get("status")) == str(cd.RunState.running):
            break
        await asyncio.sleep(0.03)

    await w1.stop()

    w2 = wu.Worker(
        db=inmemory_db,
        cfg=worker_cfg,
        roles=["sleepy"],
        handlers={"sleepy": build_sleepy_handler(db=inmemory_db, role="sleepy", batches=1, sleep_s=0.2)},
    )
    await w2.start()
    await asyncio.sleep(0.4)

    found = False
    cur = inmemory_db.worker_events.find({})
    async for e in cur:
        if (e.get("payload") or {}).get("kind") == "TASK_RESUMED":
            found = True
            break
    assert not found, "TASK_RESUMED must not be published on worker restart"
    await w2.stop()


@pytest.mark.cfg(worker={"hb_interval_sec": 0.05}, coord={"heartbeat_soft_sec": 5, "heartbeat_hard_sec": 60})
@pytest.mark.asyncio
async def test_heartbeat_updates_lease_deadline_simple(env_and_imports, inmemory_db, coord, worker_factory):
    """
    Heartbeats should move the lease.deadline_ts_ms forward while the node runs.
    """
    cd, _ = env_and_imports

    await worker_factory(("sleepy", build_sleepy_handler(db=inmemory_db, role="sleepy", batches=1, sleep_s=0.8)))

    graph = prime_graph(
        cd,
        make_graph(
            nodes=[{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    # First observed lease
    first: int | None = None
    for _ in range(200):
        t = await inmemory_db.tasks.find_one({"id": task_id})
        if t:
            lease = node_by_id(t, "s").get("lease") or {}
            if lease.get("deadline_ts_ms"):
                first = int(lease["deadline_ts_ms"])
                break
        await asyncio.sleep(0.02)
    assert first is not None, "expected first lease.deadline_ts_ms"

    await asyncio.sleep(1.1)
    t = await inmemory_db.tasks.find_one({"id": task_id})
    lease2 = node_by_id(t, "s").get("lease") or {}
    assert int(lease2.get("deadline_ts_ms", 0)) > int(first), "heartbeat must move lease forward"
