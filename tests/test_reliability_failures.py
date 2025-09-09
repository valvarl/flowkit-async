"""
Tests around source-like roles: idempotent metrics, retries, fencing, coordinator
restart adoption, cascade cancel, and heartbeat/lease updates.

Design:
- All runtime knobs come via coord_cfg/worker_cfg fixtures (see conftest.py).
- For one-off tweaks, use @pytest.mark.cfg(coord={...}, worker={...}).
- Workers are started with in-memory DB and handlers built from helper builders.
"""

from __future__ import annotations

import asyncio

import pytest

from flowkit.core.log import log_context
from flowkit.protocol.messages import Envelope, EventKind, MsgType, Role
from tests.helpers import status_topic
from tests.helpers.graph import make_graph, node_by_id, prime_graph, wait_task_finished, wait_task_status
from tests.helpers.handlers import (
    build_cancelable_source_handler,
    build_counting_source_handler,
    build_flaky_once_handler,
    build_noop_handler,
    build_permanent_fail_handler,
    build_slow_source_handler,
)
from tests.helpers.kafka import BROKER, AIOKafkaProducerMock

# Limit roles available in this module. conftest will pass these to CoordinatorConfig.
pytestmark = pytest.mark.worker_types("source,flaky,a,b,c")


@pytest.mark.asyncio
async def test_idempotent_metrics_on_duplicate_events(
    env_and_imports, inmemory_db, coord, worker_factory, monkeypatch, tlog
):
    """
    Duplicated STATUS events (BATCH_OK/TASK_DONE) must not double-count metrics.
    We duplicate STATUS envelopes at the Kafka level; aggregator must remain stable.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="idempotent_metrics_on_duplicate_events")

    # Start a single source worker that emits 9 items in batches of 3.
    await worker_factory(("source", build_counting_source_handler(db=inmemory_db, total=9, batch=3)))

    # Duplicate only STATUS events coming from the worker.
    orig_send = AIOKafkaProducerMock.send_and_wait

    async def dup_status(self, topic, value, key=None):
        await orig_send(self, topic, value, key)
        if topic.startswith("status.") and (value or {}).get("msg_type") == "event":
            kind = (value.get("payload") or {}).get("kind") or ""
            if kind in ("BATCH_OK", "TASK_DONE"):
                tlog.debug("test.kafka.dup.inject", event="test.kafka.dup.inject", topic=topic, kind=kind)
                await BROKER.produce(topic, value)

    monkeypatch.setattr("tests.helpers.kafka.AIOKafkaProducerMock.send_and_wait", dup_status, raising=True)

    graph = make_graph(
        nodes=[
            {"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}},
        ],
        edges=[],
        agg={"after": "s", "node_id": "agg", "mode": "sum"},
    )
    graph = prime_graph(cd, graph)
    task_id = await coord.create_task(params={}, graph=graph)
    with log_context(task_id=task_id):
        tlog.debug("test.task.created", event="test.task.created")

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=10.0)
    s = node_by_id(tdoc, "s")
    got = int((s.get("stats") or {}).get("count") or 0)
    tlog.debug("test.idempotent.final", event="test.idempotent.final", count=got)
    assert got == 9
    assert str(node_by_id(tdoc, "agg").get("status")).endswith("finished")


@pytest.mark.asyncio
async def test_transient_failure_deferred_then_retry(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    A transient error should defer the node and succeed on retry according to retry_policy (max>=2).
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="transient_failure_deferred_then_retry")

    # Flaky role: first batch fails transiently, then succeeds.
    await worker_factory(("flaky", build_flaky_once_handler(db=inmemory_db)))

    graph = make_graph(
        nodes=[
            {
                "node_id": "f",
                "type": "flaky",
                "depends_on": [],
                "fan_in": "all",
                "retry_policy": {"max": 2, "backoff_sec": 1, "permanent_on": []},
                "io": {"input_inline": {}},
            }
        ],
        edges=[],
    )
    graph = prime_graph(cd, graph)
    task_id = await coord.create_task(params={}, graph=graph)
    with log_context(task_id=task_id):
        tlog.debug("test.task.created", event="test.task.created")

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    f = node_by_id(tdoc, "f")
    tlog.debug(
        "test.retry.final",
        event="test.retry.final",
        status=str(f.get("status")),
        attempt_epoch=int(f.get("attempt_epoch", 0)),
    )
    assert str(f.get("status")).endswith("finished")
    assert int(f.get("attempt_epoch", 0)) >= 2


@pytest.mark.cfg(coord={"hb_monitor_tick_sec": 0.1})
@pytest.mark.asyncio
async def test_permanent_fail_cascades_cancel_and_task_failed(
    env_and_imports, inmemory_db, coord, worker_factory, tlog
):
    """
    Permanent failure in an upstream node should cause the task to fail,
    while dependents get cancelled/deferred/queued depending on race windows.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="permanent_fail_cascades_cancel_and_task_failed")

    await worker_factory(
        ("a", build_permanent_fail_handler(db=inmemory_db, role="a")),
        ("b", build_noop_handler(db=inmemory_db, role="b")),
        ("c", build_noop_handler(db=inmemory_db, role="c")),
    )

    graph = make_graph(
        nodes=[
            {"node_id": "a", "type": "a", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}},
            {"node_id": "b", "type": "b", "depends_on": ["a"], "fan_in": "all", "io": {"input_inline": {}}},
            {"node_id": "c", "type": "c", "depends_on": ["a"], "fan_in": "all", "io": {"input_inline": {}}},
        ],
        edges=[("a", "b"), ("a", "c")],
    )
    graph = prime_graph(cd, graph)
    task_id = await coord.create_task(params={}, graph=graph)
    with log_context(task_id=task_id):
        tlog.debug("test.task.created", event="test.task.created")

    tdoc = await wait_task_status(inmemory_db, task_id, want=str(cd.RunState.failed), timeout=8.0)

    b = node_by_id(tdoc, "b")
    c = node_by_id(tdoc, "c")
    bs = str(b.get("status"))
    cs = str(c.get("status"))
    tlog.debug("test.cascade.final", event="test.cascade.final", task=str(tdoc.get("status")), b=bs, c=cs)

    assert str(tdoc.get("status")) == str(cd.RunState.failed)
    allowed = {str(cd.RunState.cancelling), str(cd.RunState.deferred), str(cd.RunState.queued)}
    assert bs in allowed
    assert cs in allowed


@pytest.mark.asyncio
async def test_status_fencing_ignores_stale_epoch(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Status fencing must ignore events from a stale attempt_epoch.
    We finish the task, then send a forged event with attempt_epoch=0; stats must remain unchanged.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="status_fencing_ignores_stale_epoch")

    await worker_factory(("source", build_counting_source_handler(db=inmemory_db, total=9, batch=3)))

    graph = make_graph(
        nodes=[{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
        edges=[],
    )
    graph = prime_graph(cd, graph)
    task_id = await coord.create_task(params={}, graph=graph)
    with log_context(task_id=task_id):
        tlog.debug("test.task.created", event="test.task.created")

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=10.0)
    base = int((node_by_id(tdoc, "s").get("stats") or {}).get("count") or 0)

    env = Envelope(
        msg_type=MsgType.event,
        role=Role.worker,
        dedup_id="stale1",
        task_id=task_id,
        node_id="s",
        step_type="source",
        attempt_epoch=0,  # stale epoch
        ts_ms=coord.clock.now_ms(),
        payload={
            "kind": EventKind.BATCH_OK,
            "worker_id": "WZ",
            "metrics": {"count": 999},
            "artifacts_ref": {"batch_uid": "zzz"},
        },
    )
    await BROKER.produce(status_topic("source"), env.model_dump(mode="json"))
    tlog.debug("test.fencing.injected", event="test.fencing.injected", attempt_epoch=0)
    await asyncio.sleep(0.2)

    t2 = await inmemory_db.tasks.find_one({"id": task_id})
    got = int((node_by_id(t2, "s").get("stats") or {}).get("count") or 0)
    tlog.debug("test.fencing.final", event="test.fencing.final", base=base, got=got)
    assert got == base, "stale event must be ignored by fencing"


@pytest.mark.asyncio
async def test_coordinator_restart_adopts_inflight_without_new_epoch(
    env_and_imports, inmemory_db, coord_cfg, worker_cfg, worker_factory, tlog
):
    """
    When the coordinator restarts, it should adopt in-flight work without incrementing
    the worker's attempt_epoch unnecessarily (i.e., source keeps epoch=1).
    """
    cd, wu = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="coordinator_restart_adopts_inflight_without_new_epoch")

    # Slow streaming source to ensure long-running inflight.
    await worker_factory(("source", build_slow_source_handler(db=inmemory_db, total=60, batch=5, delay=0.08)))

    coord1 = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
    await coord1.start()

    task_id: str | None = None
    coord2 = None
    try:
        graph = make_graph(
            nodes=[{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
            edges=[],
        )
        graph = prime_graph(cd, graph)
        task_id = await coord1.create_task(params={}, graph=graph)
        with log_context(task_id=task_id):
            tlog.debug("test.task.created", event="test.task.created")

        # Wait until 's' is running.
        for _ in range(60):
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t and str(node_by_id(t, "s").get("status")).endswith("running"):
                tlog.debug("test.adopt.running", event="test.adopt.running")
                break
            await asyncio.sleep(0.05)

        # Restart coordinator.
        await coord1.stop()
        tlog.debug("test.adopt.restart", event="test.adopt.restart", phase="stopped.coord1")
        coord2 = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
        await coord2.start()
        tlog.debug("test.adopt.restart", event="test.adopt.restart", phase="started.coord2")

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        s = node_by_id(tdoc, "s")
        epoch = int(s.get("attempt_epoch", 0))
        tlog.debug("test.adopt.final", event="test.adopt.final", attempt_epoch=epoch)
        assert epoch == 1, "new coordinator must adopt inflight instead of restarting"
    finally:
        if coord2:
            await coord2.stop()


@pytest.mark.cfg(coord={"cancel_grace_sec": 0.05})
@pytest.mark.asyncio
async def test_explicit_cascade_cancel_moves_node_to_deferred(
    env_and_imports, inmemory_db, coord, worker_factory, tlog
):
    """
    Explicit cascade cancel should move a running node to a cancelling/deferred/queued state
    within the configured cancel_grace window.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="explicit_cascade_cancel_moves_node_to_deferred")

    await worker_factory(("source", build_cancelable_source_handler(db=inmemory_db, total=100, batch=10, delay=0.3)))

    graph = make_graph(
        nodes=[{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
        edges=[],
    )
    graph = prime_graph(cd, graph)
    task_id = await coord.create_task(params={}, graph=graph)
    with log_context(task_id=task_id):
        tlog.debug("test.task.created", event="test.task.created")

    # Wait until node is running.
    for _ in range(120):
        t = await inmemory_db.tasks.find_one({"id": task_id})
        if t and str(node_by_id(t, "s").get("status")).endswith("running"):
            tlog.debug("test.cancel.running", event="test.cancel.running")
            break
        await asyncio.sleep(0.03)

    await coord._cascade_cancel(task_id, reason="test_cancel")
    tlog.debug("test.cancel.sent", event="test.cancel.sent")

    target = {str(cd.RunState.cancelling), str(cd.RunState.deferred), str(cd.RunState.queued)}
    status = None
    # Allow a bit more than cancel_grace to see the transition.
    deadline = asyncio.get_running_loop().time() + 1.2
    while asyncio.get_running_loop().time() < deadline:
        t2 = await inmemory_db.tasks.find_one({"id": task_id})
        status = str(node_by_id(t2, "s").get("status"))
        if status in target:
            break
        await asyncio.sleep(0.05)

    tlog.debug("test.cancel.final", event="test.cancel.final", status=status, target=list(target))
    assert status in target, f"expected node status in {target}, got {status}"


@pytest.mark.cfg(worker={"hb_interval_sec": 0.05})
@pytest.mark.asyncio
async def test_heartbeat_updates_lease_deadline(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Heartbeats from a worker must extend the lease deadline in the task document.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="heartbeat_updates_lease_deadline")

    await worker_factory(("source", build_slow_source_handler(db=inmemory_db, total=40, batch=4, delay=0.12)))

    graph = make_graph(
        nodes=[{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
        edges=[],
    )
    graph = prime_graph(cd, graph)
    task_id = await coord.create_task(params={}, graph=graph)
    with log_context(task_id=task_id):
        tlog.debug("test.task.created", event="test.task.created")

    # Capture first observed lease deadline.
    first: int | None = None
    for _ in range(120):
        t = await inmemory_db.tasks.find_one({"id": task_id})
        if t:
            lease = node_by_id(t, "s").get("lease") or {}
            if lease.get("deadline_ts_ms"):
                first = int(lease["deadline_ts_ms"])
                tlog.debug("test.hb.first_deadline", event="test.hb.first_deadline", first=first)
                break
        await asyncio.sleep(0.03)
    assert first is not None

    await asyncio.sleep(1.1)

    second = first
    for _ in range(40):
        t2 = await inmemory_db.tasks.find_one({"id": task_id})
        second = int((node_by_id(t2, "s").get("lease") or {}).get("deadline_ts_ms") or 0)
        if second > first:
            break
        await asyncio.sleep(0.05)

    tlog.debug("test.hb.second_deadline", event="test.hb.second_deadline", first=first, second=second)
    assert second > first, f"heartbeat should extend lease (first={first}, second={second})"
    await wait_task_finished(inmemory_db, task_id, timeout=12.0)
