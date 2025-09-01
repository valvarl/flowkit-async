import asyncio

import pytest
import pytest_asyncio

from flowkit.protocol.messages import Envelope, EventKind, MsgType, RoleKind
from tests.helpers import (BROKER, dbg, prime_graph, status_topic,
                           wait_task_finished)
from tests.helpers.handlers import (build_flaky_once_handler,
                                    build_noop_query_only_role,
                                    build_sleepy_handler)

pytestmark = pytest.mark.worker_types("sleepy,noop,flaky")

# ───────────────────────── Fixtures ─────────────────────────

@pytest_asyncio.fixture
async def worker_soft(env_and_imports, set_constants):
    cd, wu = set_constants(coord={"HEARTBEAT_SOFT_SEC": 0.4, "HEARTBEAT_HARD_SEC": 5.0},
                           worker={"HEARTBEAT_INTERVAL_SEC": 1.0})
    w = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=1.6)})
    dbg("WORKER.STARTING", role="sleepy-soft")
    await w.start()
    dbg("WORKER.STARTED", role="sleepy-soft")
    try:
        yield w
    finally:
        dbg("WORKER.STOPPING", role="sleepy-soft")
        await w.stop()
        dbg("WORKER.STOPPED", role="sleepy-soft")

@pytest.mark.asyncio
async def test_heartbeat_soft_deferred_then_recovers(env_and_imports, inmemory_db, worker_soft):
    cd, _ = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        async def saw_deferred(timeout=3.0):
            from time import time
            t0 = time()
            while time() - t0 < timeout:
                t = await inmemory_db.tasks.find_one({"id": task_id})
                if t and str(t.get("status")) == str(cd.RunState.deferred):
                    return True
                await asyncio.sleep(0.03)
            return False

        assert await saw_deferred(), "expected task to become deferred on SOFT"
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=8.0)
        assert str(tdoc.get("status")) == str(cd.RunState.finished)
        node = [n for n in tdoc["graph"]["nodes"] if n["node_id"] == "s"][0]
        assert str(node.get("status")) == str(cd.RunState.finished)
    finally:
        await coord.stop()

@pytest_asyncio.fixture
async def worker_hard(env_and_imports, set_constants):
    cd, wu = set_constants(coord={"HEARTBEAT_SOFT_SEC": 0.2, "HEARTBEAT_HARD_SEC": 0.5},
                           worker={"HEARTBEAT_INTERVAL_SEC": 10.0})
    w = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=1.2)})
    dbg("WORKER.STARTING", role="sleepy-hard")
    await w.start()
    dbg("WORKER.STARTED", role="sleepy-hard")
    try:
        yield w
    finally:
        dbg("WORKER.STOPPING", role="sleepy-hard")
        await w.stop()
        dbg("WORKER.STOPPED", role="sleepy-hard")

@pytest.mark.asyncio
async def test_heartbeat_hard_marks_task_failed(env_and_imports, inmemory_db, worker_hard):
    cd, _ = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        from time import time
        t0 = time()
        while time() - t0 < 4.0:
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t and str(t.get("status")) == str(cd.RunState.failed):
                break
            await asyncio.sleep(0.03)
        else:
            raise AssertionError("expected HARD to mark task failed")

        t = await inmemory_db.tasks.find_one({"id": task_id})
        node = [n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0]
        assert str(node.get("status")) != str(cd.RunState.finished)
    finally:
        await coord.stop()

@pytest.mark.asyncio
async def test_resume_inflight_worker_restarts_with_local_state(env_and_imports, inmemory_db, set_constants, monkeypatch, tmp_path):
    cd, wu = env_and_imports
    set_constants(coord={"HEARTBEAT_SOFT_SEC": 30, "HEARTBEAT_HARD_SEC": 60},
                  worker={"HEARTBEAT_INTERVAL_SEC": 100})
    worker_id = "w-resume"
    monkeypatch.setenv("WORKER_ID", worker_id)
    monkeypatch.setenv("WORKER_STATE_DIR", str(tmp_path))

    w1 = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=2.0)})
    await w1.start()

    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        async def wait_running():
            from time import time
            t0 = time()
            while time() - t0 < 2.5:
                t = await inmemory_db.tasks.find_one({"id": task_id})
                if t:
                    n = [n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0]
                    if str(n.get("status")) == str(cd.RunState.running):
                        return int(n.get("attempt_epoch", 0))
                await asyncio.sleep(0.03)
            raise AssertionError("node did not reach running in time")
        epoch_before = await wait_running()

        await w1.stop()

        w2 = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=0.1)})
        await w2.start()

        await asyncio.sleep(0.5)
        t = await inmemory_db.tasks.find_one({"id": task_id})
        n = [n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0]
        epoch_after = int(n.get("attempt_epoch", 0))
        assert epoch_after == epoch_before, "coordinator should not start a new attempt during adoption"
        await w2.stop()
    finally:
        await coord.stop()

@pytest_asyncio.fixture
async def worker_noop_query(env_and_imports):
    cd, wu = env_and_imports
    w = wu.Worker(roles=["noop"], handlers={"noop": build_noop_query_only_role(wu, "noop")})
    await w.start()
    try:
        yield w
    finally:
        await w.stop()

@pytest.mark.asyncio
async def test_task_discover_complete_artifacts_skips_node_start(env_and_imports, inmemory_db, worker_noop_query):
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "x", "type": "noop", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        await inmemory_db.artifacts.update_one(
            {"task_id": task_id, "node_id": "x"},
            {"$set": {"status": "complete"}}, upsert=True
        )

        await asyncio.sleep(0.3)

        t = await inmemory_db.tasks.find_one({"id": task_id})
        node = [n for n in t["graph"]["nodes"] if n["node_id"] == "x"][0]
        assert str(node.get("status")) == str(cd.RunState.finished), "node should finish without start"
    finally:
        await coord.stop()

@pytest.mark.asyncio
async def test_grace_gate_blocks_then_allows_after_window(env_and_imports, inmemory_db, set_constants):
    cd, wu = set_constants(coord={"DISCOVERY_WINDOW_SEC": 1.0, "SCHEDULER_TICK_SEC": 0.05,
                                  "HEARTBEAT_SOFT_SEC": 30, "HEARTBEAT_HARD_SEC": 60})

    w = wu.Worker(roles=["noop"], handlers={"noop": build_noop_query_only_role(wu, "noop")})
    await w.start()
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "x", "type": "noop", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        await asyncio.sleep(0.2)
        t = await inmemory_db.tasks.find_one({"id": task_id})
        node = [n for n in t["graph"]["nodes"] if n["node_id"] == "x"][0]
        assert str(node.get("status")) != str(cd.RunState.running), "expected grace-gate to delay start"

        from time import time
        t0 = time()
        started = False
        while time() - t0 < 2.0:
            t = await inmemory_db.tasks.find_one({"id": task_id})
            node = [n for n in t["graph"]["nodes"] if n["node_id"] == "x"][0]
            if str(node.get("status")) in (str(cd.RunState.running), str(cd.RunState.finished)):
                started = True
                break
            await asyncio.sleep(0.05)
        assert started, "expected coordinator to start after window"
    finally:
        await w.stop()
        await coord.stop()

@pytest.mark.asyncio
async def test_deferred_retry_ignores_grace_gate(env_and_imports, inmemory_db, set_constants):
    cd, wu = set_constants(coord={"DISCOVERY_WINDOW_SEC": 5.0, "SCHEDULER_TICK_SEC": 0.05,
                                  "FINALIZER_TICK_SEC": 0.05, "HEARTBEAT_SOFT_SEC": 30, "HEARTBEAT_HARD_SEC": 60})
    w = wu.Worker(roles=["flaky"], handlers={"flaky": build_flaky_once_handler(wu, "flaky")})
    await w.start()
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{
                "node_id": "f", "type": "flaky", "depends_on": [], "fan_in": "all",
                "retry_policy": {"max": 2, "backoff_sec": 0, "permanent_on": []},
                "io": {"input_inline": {}}
            }],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=6.0)
        assert str(tdoc.get("status")) == str(cd.RunState.finished)
        node = [n for n in tdoc["graph"]["nodes"] if n["node_id"] == "f"][0]
        assert str(node.get("status")) == str(cd.RunState.finished)
    finally:
        await w.stop()
        await coord.stop()

@pytest.mark.asyncio
async def test_no_task_resumed_on_worker_restart(env_and_imports, inmemory_db, set_constants, monkeypatch, tmp_path):
    cd, wu = set_constants(coord={"HEARTBEAT_SOFT_SEC": 30, "HEARTBEAT_HARD_SEC": 60})
    worker_id = "w-nores"
    monkeypatch.setenv("WORKER_ID", worker_id)
    monkeypatch.setenv("WORKER_STATE_DIR", str(tmp_path))

    w1 = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=1.0)})
    await w1.start()
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        from time import time
        t0 = time()
        while time() - t0 < 2.5:
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t:
                n = [n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0]
                if str(n.get("status")) == str(cd.RunState.running):
                    break
            await asyncio.sleep(0.03)
        await w1.stop()

        w2 = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=0.2)})
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
    finally:
        await coord.stop()

@pytest.mark.asyncio
async def test_heartbeat_updates_lease_deadline_simple(env_and_imports, inmemory_db, set_constants):
    cd, wu = set_constants(worker={"HEARTBEAT_INTERVAL_SEC": 0.05},
                           coord={"HEARTBEAT_SOFT_SEC": 5, "HEARTBEAT_HARD_SEC": 60})
    w = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=0.8)})
    await w.start()
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        first = None
        for _ in range(200):
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t:
                lease = ([n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0].get("lease") or {})
                if lease.get("deadline_ts"):
                    first = int(lease["deadline_ts"]); break
            await asyncio.sleep(0.02)
        assert first is not None, "expected first lease.deadline_ts"

        await asyncio.sleep(1.1)
        t = await inmemory_db.tasks.find_one({"id": task_id})
        lease2 = ([n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0].get("lease") or {})
        assert int(lease2.get("deadline_ts", 0)) > int(first), "heartbeat must move lease forward"
    finally:
        await w.stop()
        await coord.stop()
