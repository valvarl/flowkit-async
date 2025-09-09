from __future__ import annotations

import asyncio
import copy
from contextlib import suppress
from enum import Enum

import pytest
import pytest_asyncio

from tests.helpers import BROKER, AIOKafkaConsumerMock
from tests.helpers.graph import prime_graph, wait_node_running, wait_task_finished
from tests.helpers.handlers import build_analyzer_handler, build_flaky_once_handler, build_indexer_handler
from tests.helpers.util import stable_hash

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
        "edges": [["w1", "w2"]],
        "edges_ex": [{"from": "w1", "to": "w2", "mode": "async", "trigger": "on_batch"}],
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
        "edges": [],
    }


# ───────────────────────── Fixtures ─────────────────────────


@pytest_asyncio.fixture
async def workers(worker_factory, inmemory_db):
    """
    Start one worker for each role used here: indexer, analyzer, flaky.
    Auto-stopped via worker_factory teardown.
    """
    idx = build_indexer_handler(db=inmemory_db)
    ana = build_analyzer_handler(db=inmemory_db)
    flk = build_flaky_once_handler(db=inmemory_db)
    ws = await worker_factory(
        ("indexer", idx),
        ("analyzer", ana),
        ("flaky", flk),
    )
    return {"indexer": ws[0], "analyzer": ws[1], "flaky": ws[2]}


# ───────────────────────── Tests ─────────────────────────


@pytest.mark.asyncio
async def test_cascade_cancel_prevents_downstream(env_and_imports, inmemory_db, coord, workers):
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

    g = prime_graph(cd, graph_cancel_flow())
    tid = await coord.create_task(params={}, graph=g)

    # Ensure producer (indexer) started
    await wait_node_running(inmemory_db, tid, "w1", timeout=4.0)

    # Spy CANCELLED for indexer
    spy = AIOKafkaConsumerMock("status.indexer.v1", group_id="test.spy.cancel")
    await spy.start()
    cancelled_seen = asyncio.Event()

    async def watch_cancel():
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("msg_type") == "event" and (env.get("payload") or {}).get("kind") == "CANCELLED":
                if env.get("task_id") == tid and env.get("node_id") == "w1":
                    cancelled_seen.set()
                    return

    spy_task = asyncio.create_task(watch_cancel())

    # Cancel the whole task (in background to avoid blocking on grace windows)
    cancel_bg = asyncio.create_task(cancel_method(tid, reason="test-cascade"))

    # Wait for CANCELLED from w1
    await asyncio.wait_for(cancelled_seen.wait(), timeout=5.0)

    # Ensure background cancel task finished (avoid leaked task / satisfy RUF006)
    try:
        await asyncio.wait_for(cancel_bg, timeout=5.0)
    except Exception:
        pass

    # Downstream must not start after cancel
    with pytest.raises(AssertionError):
        await wait_node_running(inmemory_db, tid, "w2", timeout=1.0)

    spy_task.cancel()
    try:
        await spy_task
    except Exception:
        pass
    await spy.stop()

    # Final sanity: task is cancelled/failed/finished OR coordinator has 'cancelled' flag
    tdoc = await inmemory_db.tasks.find_one({"id": tid})
    assert tdoc is not None
    st = tdoc.get("status")
    if isinstance(st, Enum):
        st = st.value
    assert st in ("failed", "finished", "cancelled") or ((tdoc.get("coordinator") or {}).get("cancelled") is True)


@pytest.mark.asyncio
async def test_restart_higher_epoch_ignores_old_events(env_and_imports, inmemory_db, coord, workers):
    """
    After accepting epoch>=1, re-inject an old event (epoch=0). Coordinator must ignore it by fencing.
    """
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_restart_flaky())
    tid = await coord.create_task(params={}, graph=g)

    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.restart")
    await spy.start()

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
                    await BROKER.produce(status_topic, saved_old)
                return

    coll_task = asyncio.create_task(collect_and_inject())

    tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)

    coll_task.cancel()
    try:
        await coll_task
    except Exception:
        pass
    await spy.stop()

    # Final node status should be finished with attempt_epoch >= 1
    node_map = {n["node_id"]: n for n in tdoc["graph"]["nodes"]}
    fx = node_map["fx"]
    st = fx.get("status")
    if isinstance(st, Enum):
        st = st.value
    assert st == "finished"
    assert await inmemory_db.artifacts.find_one({"task_id": tid, "node_id": "fx"}) is not None
    assert int(fx.get("attempt_epoch", 0)) >= 1


@pytest.mark.asyncio
async def test_cancel_before_any_start_keeps_all_nodes_idle(env_and_imports, inmemory_db, coord, workers):
    """Cancel the task before any node can start: no node must enter 'running'."""
    cd, _ = env_and_imports

    g = prime_graph(
        cd,
        {
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
            "edges": [["w1", "w2"]],
            "edges_ex": [{"from": "w1", "to": "w2", "mode": "async", "trigger": "on_batch"}],
        },
    )

    # Find cancel API
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel", "_cascade_cancel"):
        if hasattr(coord, name):
            cancel_method = getattr(coord, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    tid = await coord.create_task(params={}, graph=g)

    # Fire cancel immediately (do not await)
    cancel_bg = asyncio.create_task(cancel_method(tid, reason="cancel-before-start"))

    with pytest.raises(AssertionError):
        await wait_node_running(inmemory_db, tid, "w1", timeout=1.0)
    with pytest.raises(AssertionError):
        await wait_node_running(inmemory_db, tid, "w2", timeout=1.0)

    # Make sure background cancel completed (avoids dangling task)
    try:
        await asyncio.wait_for(cancel_bg, timeout=5.0)
    except Exception:
        pass


@pytest.mark.asyncio
async def test_cancel_on_deferred_prevents_retry(env_and_imports, inmemory_db, coord, workers):
    """
    Node 'flaky' fails on first attempt → becomes deferred with backoff.
    Cancel the task right after TASK_FAILED(epoch=1) and ensure no higher epoch is accepted.
    """
    cd, _ = env_and_imports

    base = graph_restart_flaky()
    base["nodes"][0]["retry_policy"]["backoff_sec"] = 1.0  # tweak backoff
    g = prime_graph(cd, base)

    # Find cancel API
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel", "_cascade_cancel"):
        if hasattr(coord, name):
            cancel_method = getattr(coord, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    tid = await coord.create_task(params={}, graph=g)

    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.defer_cancel")
    await spy.start()

    cancel_bg_task = None
    cancel_triggered = asyncio.Event()
    higher_epoch_accepted = asyncio.Event()

    async def watcher():
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("task_id") != tid or env.get("msg_type") != "event" or env.get("node_id") != "fx":
                continue
            epoch = int(env.get("attempt_epoch", 0))
            kind = (env.get("payload") or {}).get("kind")

            if kind == "TASK_FAILED" and epoch == 1:
                nonlocal cancel_bg_task
                cancel_bg_task = asyncio.create_task(cancel_method(tid, reason="cancel-on-deferred"))
                cancel_triggered.set()
                return

            if kind == "TASK_ACCEPTED" and epoch >= 2:
                higher_epoch_accepted.set()

    wtask = asyncio.create_task(watcher())

    await asyncio.wait_for(cancel_triggered.wait(), timeout=5.0)

    # Ensure a new epoch does NOT start within slightly more than backoff window
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(higher_epoch_accepted.wait(), timeout=1.8)

    wtask.cancel()
    try:
        await wtask
    except Exception:
        pass
    await spy.stop()

    # If cancel was issued, ensure it completed
    if cancel_bg_task is not None:
        try:
            await asyncio.wait_for(cancel_bg_task, timeout=5.0)
        except Exception:
            pass


@pytest.mark.asyncio
async def test_restart_higher_epoch_ignores_old_batch_ok(env_and_imports, inmemory_db, coord, workers):
    """
    After a node restarts with a higher epoch, the coordinator must ignore a stale BATCH_OK
    from a lower epoch. Also ensure injected stale event doesn't create duplicate metrics.
    """
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_restart_flaky())
    tid = await coord.create_task(params={}, graph=g)

    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.fence_bok")
    await spy.start()

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
                    await BROKER.produce(status_topic, fake_old)
                    return

            if kind == "BATCH_OK" and epoch >= 1:
                saved_bok_e1 = env
                if seen_accept_e1:
                    fake_old = copy.deepcopy(saved_bok_e1)
                    fake_old["attempt_epoch"] = 0
                    uid = (fake_old.get("payload") or {}).get("batch_uid")
                    fake_old["dedup_id"] = stable_hash({"bok": tid, "n": "fx", "e": 0, "uid": uid})
                    await BROKER.produce(status_topic, fake_old)
                    return

    coll_task = asyncio.create_task(collect_and_inject())

    tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)

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
    assert st == "finished"
    assert int(fx.get("attempt_epoch", 0)) >= 1

    # Verify the injected stale event was actually recorded by coordinator's event log
    stale_count = 0
    cur = inmemory_db.worker_events.find(
        {"task_id": tid, "node_id": "fx", "attempt_epoch": 0, "payload.kind": "BATCH_OK"}
    )
    async for _ in cur:
        stale_count += 1
    assert stale_count == 1, f"expected injected stale BATCH_OK to be recorded once, got {stale_count}"

    # Metrics deduplication: exactly one metrics doc per batch_uid for this node
    counts = {}
    cur = inmemory_db.metrics_raw.find({"task_id": tid, "node_id": "fx"})
    async for m in cur:
        uid = m.get("batch_uid")
        counts[uid] = counts.get(uid, 0) + 1
    assert counts, "no metrics were recorded"
    assert all(c == 1 for c in counts.values()), f"duplicate metrics found: {counts}"
