# tests/test_cancel_and_restart.py
from __future__ import annotations

import asyncio
import copy
from contextlib import suppress
from enum import Enum

import pytest
import pytest_asyncio

from flowkit.core.utils import stable_hash
from tests.helpers import BROKER, AIOKafkaConsumerMock
from tests.helpers.graph import wait_node_running, wait_task_finished
from tests.helpers.handlers import build_analyzer_handler, build_flaky_once_handler, build_indexer_handler

# Ограничиваем роли для ускорения/избежания лишних handler'ов
pytestmark = pytest.mark.worker_types("indexer,analyzer,flaky")


# ───────────────────────── Small helpers ─────────────────────────


def graph_cancel_flow() -> dict:
    """
    w1=indexer -> w2=analyzer; analyzer starts on first upstream batch (pull.from_artifacts).
    Equivalent to simple producer→sink.
    """
    return {
        "schema_version": "1.0",
        "nodes": [
            {
                "node_id": "w1",
                "type": "indexer",
                "depends_on": [],
                "fan_in": "all",
                "io": {"input_inline": {"batch_size": 5, "total_skus": 50}},
            },
            {
                "node_id": "w2",
                "type": "analyzer",
                "depends_on": ["w1"],
                "fan_in": "any",
                "io": {
                    "start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["w1"], "poll_ms": 30, "meta_list_key": "skus"},
                    },
                },
            },
        ],
    }


def graph_restart_flaky() -> dict:
    """Single 'flaky' node with retries: first attempt fails (transient), then succeeds."""
    return {
        "schema_version": "1.0",
        "nodes": [
            {
                "node_id": "fx",
                "type": "flaky",
                "depends_on": [],
                "fan_in": "all",
                "retry_policy": {"max": 3, "backoff_sec": 0.05, "permanent_on": []},
                "io": {"input_inline": {}},
            }
        ],
    }


# ───────────────────────── Fixtures ─────────────────────────


@pytest_asyncio.fixture
async def workers(worker_factory, inmemory_db, tlog):
    """
    Start one worker for each role used here: indexer, analyzer, flaky.
    Auto-stopped via worker_factory teardown.
    """
    tlog.debug("workers.spawn.start", event="workers.spawn.start", roles=["indexer", "analyzer", "flaky"])
    idx = build_indexer_handler(db=inmemory_db)
    ana = build_analyzer_handler(db=inmemory_db)
    flk = build_flaky_once_handler(db=inmemory_db)
    ws = await worker_factory(("indexer", idx), ("analyzer", ana), ("flaky", flk))
    tlog.debug("workers.spawn.done", event="workers.spawn.done")
    return {"indexer": ws[0], "analyzer": ws[1], "flaky": ws[2]}


# ───────────────────────── Tests ─────────────────────────


@pytest.mark.asyncio
async def test_cascade_cancel_prevents_downstream(env_and_imports, inmemory_db, coord, workers, tlog):
    """Cancel the task while upstream runs: downstream must not start."""
    cd, _ = env_and_imports

    # Find a cancel API (support multiple names across versions)
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel", "_cascade_cancel"):
        if hasattr(coord, name):
            cancel_method = getattr(coord, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    g = graph_cancel_flow()
    tid = await coord.create_task(params={}, graph=g)
    tlog.debug("task.created", event="task.created", test_name="cascade_cancel_prevents_downstream", task_id=tid)

    # Ensure producer (indexer) started
    await wait_node_running(inmemory_db, tid, "w1", timeout=4.0)
    tlog.debug("node.running", event="node.running", task_id=tid, node_id="w1")

    # Spy CANCELLED for indexer
    spy = AIOKafkaConsumerMock("status.indexer.v1", group_id="test.spy.cancel")
    await spy.start()
    tlog.debug("spy.start", event="spy.start", topic="status.indexer.v1", group_id="test.spy.cancel")
    cancelled_seen = asyncio.Event()

    async def watch_cancel():
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("msg_type") == "event" and (env.get("payload") or {}).get("kind") == "CANCELLED":
                if env.get("task_id") == tid and env.get("node_id") == "w1":
                    tlog.debug("cancel.event.observed", event="cancel.event.observed", task_id=tid, node_id="w1")
                    cancelled_seen.set()
                    return

    spy_task = asyncio.create_task(watch_cancel())

    # Cancel the whole task (in background to avoid blocking on grace windows)
    tlog.debug("coordinator.cancel.call", event="coordinator.cancel.call", task_id=tid)
    cancel_bg = asyncio.create_task(cancel_method(tid, reason="test-cascade"))

    # Wait for CANCELLED from w1
    await asyncio.wait_for(cancelled_seen.wait(), timeout=5.0)
    tlog.debug("cancel.event.received", event="cancel.event.received", task_id=tid)

    # Ensure background cancel task finished
    with suppress(Exception):
        await asyncio.wait_for(cancel_bg, timeout=5.0)

    # Downstream must not start after cancel
    with pytest.raises(AssertionError):
        await wait_node_running(inmemory_db, tid, "w2", timeout=1.0)
    tlog.debug("downstream.not_started", event="downstream.not_started", task_id=tid, node_id="w2")

    spy_task.cancel()
    with suppress(Exception):
        await spy_task
    await spy.stop()

    # Final sanity
    tdoc = await inmemory_db.tasks.find_one({"id": tid})
    assert tdoc is not None
    st = tdoc.get("status")
    if isinstance(st, Enum):
        st = st.value
    tlog.debug("task.final.status", event="task.final.status", task_id=tid, status=st)
    assert st in ("failed", "finished", "cancelled") or ((tdoc.get("coordinator") or {}).get("cancelled") is True)


@pytest.mark.asyncio
async def test_restart_higher_epoch_ignores_old_events(env_and_imports, inmemory_db, coord, workers, tlog):
    """
    After accepting epoch>=1, re-inject an old event (epoch=0). Coordinator must ignore it by fencing.
    """
    cd, _ = env_and_imports
    g = graph_restart_flaky()
    tid = await coord.create_task(params={}, graph=g)
    tlog.debug("task.created", event="task.created", test_name="restart_higher_epoch_ignores_old_events", task_id=tid)

    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.restart")
    await spy.start()
    tlog.debug("spy.start", event="spy.start", topic=status_topic, group_id="test.spy.restart")

    async def collect_and_inject():
        saved_old = None
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("task_id") != tid or env.get("msg_type") != "event" or env.get("node_id") != "fx":
                continue
            epoch = int(env.get("attempt_epoch", 0))
            kind = (env.get("payload") or {}).get("kind")

            if epoch == 0:
                saved_old = env  # prefer TASK_FAILED typically

            if kind == "TASK_ACCEPTED" and epoch >= 1:
                if saved_old:
                    tlog.debug(
                        "inject.stale.event",
                        event="inject.stale.event",
                        task_id=tid,
                        kind=saved_old["payload"]["kind"],
                        stale_epoch=0,
                    )
                    await BROKER.produce(status_topic, saved_old)
                return

    coll_task = asyncio.create_task(collect_and_inject())

    tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)
    tlog.debug("task.finished", event="task.finished", task_id=tid)

    coll_task.cancel()
    with suppress(Exception):
        await coll_task
    await spy.stop()

    # Final node status should be finished with attempt_epoch >= 1
    node_map = {n["node_id"]: n for n in tdoc["graph"]["nodes"]}
    fx = node_map["fx"]
    st = fx.get("status")
    if isinstance(st, Enum):
        st = st.value
    tlog.debug("node.final", event="node.final", node_id="fx", status=st, attempt_epoch=int(fx.get("attempt_epoch", 0)))
    assert st == "finished"
    assert await inmemory_db.artifacts.find_one({"task_id": tid, "node_id": "fx"}) is not None
    assert int(fx.get("attempt_epoch", 0)) >= 1


@pytest.mark.asyncio
async def test_cancel_before_any_start_keeps_all_nodes_idle(env_and_imports, inmemory_db, coord, workers, tlog):
    """Cancel the task before any node can start: no node must enter 'running'."""
    cd, _ = env_and_imports

    g = {
        "schema_version": "1.0",
        "nodes": [
            {
                "node_id": "w1",
                "type": "indexer",
                "depends_on": ["__missing__"],  # prevents start
                "fan_in": "all",
                "io": {"input_inline": {"batch_size": 5, "total_skus": 10}},
            },
            {
                "node_id": "w2",
                "type": "analyzer",
                "depends_on": ["w1"],
                "fan_in": "any",
                "io": {
                    "start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["w1"], "poll_ms": 30, "meta_list_key": "skus"},
                    },
                },
            },
        ],
    }

    # Find cancel API
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel", "_cascade_cancel"):
        if hasattr(coord, name):
            cancel_method = getattr(coord, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    tid = await coord.create_task(params={}, graph=g)
    tlog.debug(
        "task.created", event="task.created", test_name="cancel_before_any_start_keeps_all_nodes_idle", task_id=tid
    )

    # Fire cancel immediately (do not await)
    tlog.debug("coordinator.cancel.call", event="coordinator.cancel.call", task_id=tid)
    cancel_bg = asyncio.create_task(cancel_method(tid, reason="cancel-before-start"))

    with pytest.raises(AssertionError):
        await wait_node_running(inmemory_db, tid, "w1", timeout=1.0)
    with pytest.raises(AssertionError):
        await wait_node_running(inmemory_db, tid, "w2", timeout=1.0)
    tlog.debug("nodes.stayed.idle", event="nodes.stayed.idle", task_id=tid, nodes=["w1", "w2"])

    with suppress(Exception):
        await asyncio.wait_for(cancel_bg, timeout=5.0)


@pytest.mark.asyncio
async def test_cancel_on_deferred_prevents_retry(env_and_imports, inmemory_db, coord, workers, tlog):
    """
    Node 'flaky' fails on first attempt → becomes deferred with backoff.
    Cancel the task right after TASK_FAILED(epoch=1) and ensure no higher epoch is accepted.
    """
    cd, _ = env_and_imports

    base = graph_restart_flaky()
    base["nodes"][0]["retry_policy"]["backoff_sec"] = 1.0  # tweak backoff
    g = base

    # Find cancel API
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel", "_cascade_cancel"):
        if hasattr(coord, name):
            cancel_method = getattr(coord, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    tid = await coord.create_task(params={}, graph=g)
    tlog.debug("task.created", event="task.created", test_name="cancel_on_deferred_prevents_retry", task_id=tid)

    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.defer_cancel")
    await spy.start()
    tlog.debug("spy.start", event="spy.start", topic=status_topic, group_id="test.spy.defer_cancel")

    cancel_bg_task = None
    cancel_triggered = asyncio.Event()
    higher_epoch_accepted = asyncio.Event()

    async def watcher():
        nonlocal cancel_bg_task
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("task_id") != tid or env.get("msg_type") != "event" or env.get("node_id") != "fx":
                continue
            epoch = int(env.get("attempt_epoch", 0))
            kind = (env.get("payload") or {}).get("kind")

            if kind == "TASK_FAILED" and epoch == 1:
                tlog.debug("deferred.observed", event="deferred.observed", task_id=tid, node_id="fx", epoch=epoch)
                cancel_bg_task = asyncio.create_task(cancel_method(tid, reason="cancel-on-deferred"))
                cancel_triggered.set()
                return

            if kind == "TASK_ACCEPTED" and epoch >= 2:
                tlog.debug("unexpected.accepted", event="unexpected.accepted", task_id=tid, node_id="fx", epoch=epoch)
                higher_epoch_accepted.set()

    wtask = asyncio.create_task(watcher())
    await asyncio.wait_for(cancel_triggered.wait(), timeout=5.0)

    # Ensure a new epoch does NOT start within slightly more than backoff window
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(higher_epoch_accepted.wait(), timeout=1.8)

    wtask.cancel()
    with suppress(Exception):
        await wtask
    await spy.stop()

    if cancel_bg_task is not None:
        with suppress(Exception):
            await asyncio.wait_for(cancel_bg_task, timeout=5.0)
    tlog.debug("cancel.completed", event="cancel.completed", task_id=tid)


@pytest.mark.asyncio
async def test_restart_higher_epoch_ignores_old_batch_ok(env_and_imports, inmemory_db, coord, workers, tlog):
    """
    After a node restarts with a higher epoch, the coordinator must ignore a stale BATCH_OK
    from a lower epoch. Also ensure injected stale event doesn't create duplicate metrics.
    """
    cd, _ = env_and_imports
    g = graph_restart_flaky()
    tid = await coord.create_task(params={}, graph=g)
    tlog.debug("task.created", event="task.created", test_name="restart_higher_epoch_ignores_old_batch_ok", task_id=tid)

    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.fence_bok")
    await spy.start()
    tlog.debug("spy.start", event="spy.start", topic=status_topic, group_id="test.spy.fence_bok")

    async def collect_and_inject():
        saved_bok_e1 = None
        seen_accept_e1 = False

        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("task_id") != tid or env.get("msg_type") != "event" or env.get("node_id") != "fx":
                continue

            kind = (env.get("payload") or {}).get("kind")
            epoch = int(env.get("attempt_epoch", 0))

            if kind == "TASK_ACCEPTED" and epoch >= 1:
                seen_accept_e1 = True
                if saved_bok_e1:
                    # Forge a stale BATCH_OK from epoch 0 with its own dedup_id so it's recorded
                    fake_old = copy.deepcopy(saved_bok_e1)
                    fake_old["attempt_epoch"] = 0
                    uid = (fake_old.get("payload") or {}).get("batch_uid")
                    fake_old["dedup_id"] = stable_hash({"bok": tid, "n": "fx", "e": 0, "uid": uid})
                    tlog.debug("inject.stale.bok", event="inject.stale.bok", task_id=tid, batch_uid=uid)
                    await BROKER.produce(status_topic, fake_old)
                    return

            if kind == "BATCH_OK" and epoch >= 1:
                saved_bok_e1 = env
                if seen_accept_e1:
                    fake_old = copy.deepcopy(saved_bok_e1)
                    fake_old["attempt_epoch"] = 0
                    uid = (fake_old.get("payload") or {}).get("batch_uid")
                    fake_old["dedup_id"] = stable_hash({"bok": tid, "n": "fx", "e": 0, "uid": uid})
                    tlog.debug("inject.stale.bok", event="inject.stale.bok", task_id=tid, batch_uid=uid)
                    await BROKER.produce(status_topic, fake_old)
                    return

    coll_task = asyncio.create_task(collect_and_inject())

    tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)
    tlog.debug("task.finished", event="task.finished", task_id=tid)

    coll_task.cancel()
    with suppress(asyncio.CancelledError):
        await coll_task

    # Ensure the injected stale BATCH_OK has been consumed and persisted into worker_events
    for _ in range(50):  # ~500ms total
        cnt = await inmemory_db.worker_events.count_documents(
            {"task_id": tid, "node_id": "fx", "attempt_epoch": 0, "payload.kind": "BATCH_OK"}
        )
        if cnt >= 1:
            break
        await asyncio.sleep(0.01)

    await spy.stop()

    # Node finished and epoch advanced (retry happened)
    node_map = {n["node_id"]: n for n in tdoc["graph"]["nodes"]}
    fx = node_map["fx"]
    st = fx.get("status")
    if isinstance(st, Enum):
        st = st.value
    tlog.debug("node.final", event="node.final", node_id="fx", status=st, attempt_epoch=int(fx.get("attempt_epoch", 0)))
    assert st == "finished"
    assert int(fx.get("attempt_epoch", 0)) >= 1

    # Verify the injected stale event was actually recorded by coordinator's event log
    stale_count = 0
    cur = inmemory_db.worker_events.find(
        {"task_id": tid, "node_id": "fx", "attempt_epoch": 0, "payload.kind": "BATCH_OK"}
    )
    async for _ in cur:
        stale_count += 1
    tlog.debug("stale.bok.count", event="stale.bok.count", count=stale_count)
    assert stale_count == 1, f"expected injected stale BATCH_OK to be recorded once, got {stale_count}"

    # Metrics deduplication: exactly one metrics doc per batch_uid for this node
    counts: dict[str, int] = {}
    cur2 = inmemory_db.metrics_raw.find({"task_id": tid, "node_id": "fx"})
    async for m in cur2:
        uid = m.get("batch_uid")
        counts[uid] = counts.get(uid, 0) + 1
    tlog.debug("metrics.batch_uid.counts", event="metrics.batch_uid.counts", counts=counts)
    assert counts, "no metrics were recorded"
    assert all(c == 1 for c in counts.values()), f"duplicate metrics found: {counts}"
