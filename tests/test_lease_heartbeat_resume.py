"""
Heartbeat / grace-window / resume tests.

Covers:
- Soft heartbeat -> deferred -> recovery
- Hard heartbeat -> task failed
- Resume with local worker state (no new epoch)
- Discover w/ complete artifacts -> skip start
- Grace-gate delay, and that deferred retry ignores the gate
- Heartbeat extends lease deadline
- Restart with a new worker_id bumps attempt_epoch and fences stale heartbeats
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from time import time

import pytest

from tests.helpers import wait_task_finished
from tests.helpers.graph import make_graph, node_by_id, prime_graph
from tests.helpers.handlers import (
    build_flaky_once_handler,
    build_noop_query_only_role,
    build_sleepy_handler,
)
from tests.helpers.kafka import BROKER

# Restrict available roles for this module
pytestmark = pytest.mark.worker_types("sleepy,noop,flaky")


@pytest.mark.cfg(coord={"heartbeat_soft_sec": 0.4, "heartbeat_hard_sec": 5.0}, worker={"hb_interval_sec": 1.0})
@pytest.mark.asyncio
async def test_heartbeat_soft_deferred_then_recovers(env_and_imports, inmemory_db, coord, worker_factory):
    """With a short soft heartbeat window the task becomes DEFERRED, then recovers and finishes."""
    cd, _ = env_and_imports

    # Sleepy role: slow processing so heartbeats lag behind soft window.
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
    """If hard heartbeat window is exceeded, the task should be marked FAILED."""
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
async def test_resume_inflight_worker_restarts_with_local_state(env_and_imports, inmemory_db, coord, worker_cfg):
    """Restart with the same worker_id: coordinator should adopt inflight work without new epoch."""
    cd, wu = env_and_imports

    cfg_same = replace(worker_cfg, worker_id="w-resume")

    w1 = wu.Worker(
        db=inmemory_db,
        cfg=cfg_same,
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

    # Same worker_id -> adoption, no epoch bump
    w2 = wu.Worker(
        db=inmemory_db,
        cfg=cfg_same,
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
    """If artifacts are 'complete' during discovery, node should auto-finish without starting its handler."""
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
    """Grace window should delay start initially and allow it after the window elapses."""
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
    """A DEFERRED retry must not be throttled by discovery grace window."""
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
async def test_no_task_resumed_on_worker_restart(env_and_imports, inmemory_db, coord, worker_cfg):
    """On a cold worker restart there must be no TASK_RESUMED event emitted by the worker."""
    cd, wu = env_and_imports

    cfg = replace(worker_cfg, worker_id="w-nores")

    w1 = wu.Worker(
        db=inmemory_db,
        cfg=cfg,
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
    t0 = time()
    while time() - t0 < 2.5:
        t = await inmemory_db.tasks.find_one({"id": task_id})
        if t and str(node_by_id(t, "s").get("status")) == str(cd.RunState.running):
            break
        await asyncio.sleep(0.03)

    await w1.stop()

    w2 = wu.Worker(
        db=inmemory_db,
        cfg=cfg,  # same worker_id, cold start
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
    """Heartbeats should move the lease.deadline_ts_ms forward while the node runs."""
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


@pytest.mark.asyncio
@pytest.mark.cfg(
    coord={
        "discovery_window_sec": 0.05,
        "scheduler_tick_sec": 0.02,
        "hb_monitor_tick_sec": 0.05,
        "heartbeat_soft_sec": 0.2,
        "heartbeat_hard_sec": 5.0,
    },
    worker={"hb_interval_sec": 0.2, "lease_ttl_sec": 2},
)
async def test_worker_restart_with_new_id_bumps_epoch(env_and_imports, inmemory_db, coord, worker_cfg):
    """Restart with a new worker_id must bump attempt_epoch; stale heartbeats from the old epoch are ignored."""
    cd, wu = env_and_imports

    # --- print coordinator config
    try:
        print("COORD.CFG:", getattr(coord, "cfg", None))
    except Exception:
        pass

    # First worker
    cfg1 = replace(worker_cfg, worker_id="w-epoch-1")
    print("WORKER.CFG1:", cfg1)
    w1 = wu.Worker(
        db=inmemory_db,
        cfg=cfg1,
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

    # Wait for running and capture epoch_before
    t0 = time()
    while True:
        if time() - t0 > 3.0:
            raise AssertionError("node did not reach running in time (first run)")
        doc = await inmemory_db.tasks.find_one({"id": task_id})
        if doc:
            n = node_by_id(doc, "s")
            if str(n.get("status")) == str(cd.RunState.running):
                epoch_before = int(n.get("attempt_epoch", 0))
                print(f"[PHASE1] running with epoch_before={epoch_before}")
                break
        await asyncio.sleep(0.03)

    await w1.stop()

    # Second worker with a different id
    cfg2 = replace(worker_cfg, worker_id="w-epoch-2")
    print("WORKER.CFG2:", cfg2)
    w2 = wu.Worker(
        db=inmemory_db,
        cfg=cfg2,
        roles=["sleepy"],
        handlers={"sleepy": build_sleepy_handler(db=inmemory_db, role="sleepy", batches=1, sleep_s=0.5)},
    )
    await w2.start()

    # Requeue the node to force a new attempt
    await inmemory_db.tasks.update_one(
        {"id": task_id, "graph.nodes.node_id": "s"},
        {"$set": {"graph.nodes.$.status": cd.RunState.queued, "graph.nodes.$.last_event_recv_ms": 0}},
    )
    print("[PHASE2] requeued node 's'")

    # Poll for epoch bump and new lease owner; tolerate quick finish
    t1 = time()
    epoch_after: int | None = None
    lease_worker: str | None = None
    last_status: str | None = None

    # For diagnostics
    observations: list[tuple[float, str, int, str | None]] = []

    while True:
        if time() - t1 > 4.0:
            # dump diagnostics
            cur_doc = await inmemory_db.tasks.find_one({"id": task_id})
            cur_node = node_by_id(cur_doc or {}, "s") if cur_doc else {}
            print(
                "[TIMEOUT] last node snapshot:",
                {
                    "status": str(cur_node.get("status")),
                    "attempt_epoch": cur_node.get("attempt_epoch"),
                    "lease": cur_node.get("lease"),
                },
            )
            # dump worker events
            evs = []
            cur = inmemory_db.worker_events.find({"task_id": task_id, "node_id": "s"})
            async for e in cur:
                p = e.get("payload") or {}
                evs.append(
                    {"kind": p.get("kind"), "worker_id": p.get("worker_id"), "attempt_epoch": e.get("attempt_epoch")}
                )
            print("[TIMEOUT] worker_events:", evs)
            raise AssertionError("node did not restart with a new epoch in time")

        d2 = await inmemory_db.tasks.find_one({"id": task_id})
        if d2:
            n2 = node_by_id(d2, "s")
            st = str(n2.get("status"))
            last_status = st
            epoch_after = int(n2.get("attempt_epoch", 0))
            lease_worker = (n2.get("lease") or {}).get("worker_id")
            observations.append((time() - t1, st, epoch_after, lease_worker))
            # live trace
            print(
                f"[TRACE] dt={observations[-1][0]:.3f}s status={st} epoch={epoch_after} lease.worker_id={lease_worker}"
            )

            # success condition while running
            if st == str(cd.RunState.running) and epoch_after == epoch_before + 1 and lease_worker == "w-epoch-2":
                print("[SUCCESS] running with bumped epoch and new lease owner")
                break

            # if finished quickly, we'll assert epoch bump and verify ACCEPTED from w-epoch-2 via worker_events
            if st == str(cd.RunState.finished) and epoch_after == epoch_before + 1:
                print("[INFO] finished with bumped epoch; will verify ACCEPTED owner via worker_events")
                break

        await asyncio.sleep(0.03)

    assert epoch_after == epoch_before + 1, (epoch_before, epoch_after)

    if last_status == str(cd.RunState.running):
        # already validated lease owner in-loop
        pass
    else:
        # Verify that new attempt was actually owned by w-epoch-2 at some point
        saw_accept_from_w2 = False
        cur = inmemory_db.worker_events.find({"task_id": task_id, "node_id": "s"})
        async for e in cur:
            if (
                (e.get("payload") or {}).get("kind") == "TASK_ACCEPTED"
                and (e.get("payload") or {}).get("worker_id") == "w-epoch-2"
                and int(e.get("attempt_epoch", -1)) == epoch_before + 1
            ):
                saw_accept_from_w2 = True
                break
        assert saw_accept_from_w2, "expected TASK_ACCEPTED for new epoch from w-epoch-2"

    # Fence old-epoch heartbeat
    stale_env = cd.Envelope(
        msg_type=cd.MsgType.event,
        role=cd.Role.worker,
        dedup_id="hb-stale",
        task_id=task_id,
        node_id="s",
        step_type="sleepy",
        attempt_epoch=epoch_before,  # old epoch
        ts_ms=coord.clock.now_ms(),
        payload=cd.EvHeartbeat(
            kind=cd.EventKind.TASK_HEARTBEAT,
            worker_id="w-epoch-1",
            lease_id="stale-lease",
            lease_deadline_ts_ms=coord.clock.now_ms() + 60_000,
        ).model_dump(),
    )
    await BROKER.produce(coord.bus.topic_status("sleepy"), stale_env.model_dump(mode="json"))
    await asyncio.sleep(0.2)

    d3 = await inmemory_db.tasks.find_one({"id": task_id})
    n3 = node_by_id(d3, "s")
    assert ((n3.get("lease") or {}).get("worker_id")) in (None, "w-epoch-2"), "stale heartbeat must not take over"

    await w2.stop()


@pytest.mark.asyncio
@pytest.mark.cfg(coord={"hb_monitor_tick_sec": 0.05, "heartbeat_soft_sec": 5, "heartbeat_hard_sec": 60})
async def test_heartbeat_tolerates_clock_skew(env_and_imports, inmemory_db, coord):
    """Worker clock skew should not cause a hard timeout; lease deadlines must be non-decreasing."""
    cd, _ = env_and_imports

    # Single-node task; manually put node into running/epoch=1
    graph = prime_graph(
        cd,
        make_graph(
            nodes=[{"node_id": "x", "type": "noop", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)
    await inmemory_db.tasks.update_one(
        {"id": task_id, "graph.nodes.node_id": "x"},
        {
            "$set": {
                "status": cd.RunState.running,
                "graph.nodes.$.status": cd.RunState.running,
                "graph.nodes.$.attempt_epoch": 1,
                "graph.nodes.$.started_at": coord.clock.now_dt(),
                "last_event_recv_ms": coord.clock.now_ms(),
            }
        },
    )

    topic = coord.bus.topic_status("noop")

    def _hb(deadline: int):
        return cd.Envelope(
            msg_type=cd.MsgType.event,
            role=cd.Role.worker,
            dedup_id=f"hb-{deadline}",
            task_id=task_id,
            node_id="x",
            step_type="noop",
            attempt_epoch=1,
            ts_ms=coord.clock.now_ms(),
            payload=cd.EvHeartbeat(
                kind=cd.EventKind.TASK_HEARTBEAT,
                worker_id="w-skew",
                lease_id="L",
                lease_deadline_ts_ms=deadline,
            ).model_dump(),
        )

    base = coord.clock.now_ms() + 2_000
    series = [base, base - 500, base + 1_500]  # jitter
    observed: list[int] = []

    for dl in series:
        await BROKER.produce(topic, _hb(dl).model_dump(mode="json"))
        await asyncio.sleep(0.15)
        tdoc = await inmemory_db.tasks.find_one({"id": task_id})
        lease = node_by_id(tdoc, "x").get("lease") or {}
        observed.append(int(lease.get("deadline_ts_ms", 0)))

    tdoc2 = await inmemory_db.tasks.find_one({"id": task_id})
    assert str(tdoc2.get("status")) != str(cd.RunState.failed)
    assert observed[0] <= observed[1] <= observed[2], f"non-monotonic lease deadlines: {observed}"


@pytest.mark.asyncio
@pytest.mark.cfg(coord={"cancel_grace_sec": 0.05, "finalizer_tick_sec": 0.05, "hb_monitor_tick_sec": 0.05})
async def test_lease_expiry_cascades_cancel(env_and_imports, inmemory_db, coord):
    """Permanent fail upstream should cascade-cancel downstream nodes."""
    cd, _ = env_and_imports

    # up -> down
    graph = prime_graph(
        cd,
        make_graph(
            nodes=[
                {"node_id": "up", "type": "noop", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}},
                {"node_id": "down", "type": "noop", "depends_on": ["up"], "fan_in": "all", "io": {"input_inline": {}}},
            ],
            edges=[("up", "down")],
        ),
    )
    task_id = await coord.create_task(params={}, graph=graph)

    # Wait until task is running (scheduler first iteration)
    t0 = time()
    while True:
        if time() - t0 > 1.5:
            break
        t = await inmemory_db.tasks.find_one({"id": task_id})
        if t and str(t.get("status")) == str(cd.RunState.running):
            break
        await asyncio.sleep(0.03)

    # Emulate expired lease on upstream via permanent TASK_FAILED
    env_fail = cd.Envelope(
        msg_type=cd.MsgType.event,
        role=cd.Role.worker,
        dedup_id="up-fail",
        task_id=task_id,
        node_id="up",
        step_type="noop",
        attempt_epoch=0,
        ts_ms=coord.clock.now_ms(),
        payload=cd.EvTaskFailed(
            kind=cd.EventKind.TASK_FAILED,
            worker_id="w-up",
            reason_code="lease_expired",
            permanent=True,
            error=None,
        ).model_dump(),
    )
    await BROKER.produce(coord.bus.topic_status("noop"), env_fail.model_dump(mode="json"))

    # Wait for downstream to become cancelling
    t1 = time()
    while True:
        if time() - t1 > 2.0:
            break
        doc = await inmemory_db.tasks.find_one({"id": task_id})
        dn = node_by_id(doc, "down")
        if dn and str(dn.get("status")) == str(cd.RunState.cancelling):
            break
        await asyncio.sleep(0.05)

    doc2 = await inmemory_db.tasks.find_one({"id": task_id})
    assert str(node_by_id(doc2, "down").get("status")) == str(cd.RunState.cancelling)
