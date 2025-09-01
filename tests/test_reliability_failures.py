import asyncio

import pytest
import pytest_asyncio

from flowkit.protocol.messages import Envelope, EventKind, MsgType, Role
from tests.helpers import (BROKER, AIOKafkaProducerMock, dbg, prime_graph,
                           status_topic, wait_task_finished)
from tests.helpers.handlers import (build_cancelable_source_handler,
                                    build_counting_source_handler,
                                    build_flaky_once_handler,
                                    build_noop_handler,
                                    build_permanent_fail_handler,
                                    build_slow_source_handler)

pytestmark = pytest.mark.worker_types("source,flaky,a,b,c")

# ───────────────────────── Fixtures ─────────────────────────

@pytest_asyncio.fixture
async def workers_source(env_and_imports):
    cd, wu = env_and_imports
    src = wu.Worker(roles=["source"], handlers={"source": build_counting_source_handler(wu, total=9, batch=3)})
    dbg("WORKER.STARTING", role="source")
    await src.start()
    dbg("WORKER.STARTED", role="source")
    try:
        yield src
    finally:
        dbg("WORKER.STOPPING", role="source")
        await src.stop()
        dbg("WORKER.STOPPED", role="source")

# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_idempotent_metrics_on_duplicate_events(env_and_imports, inmemory_db, workers_source, monkeypatch):
    cd, wu = env_and_imports

    orig_send = AIOKafkaProducerMock.send_and_wait
    async def dup_status(self, topic, value, key=None):
        await orig_send(self, topic, value, key)
        if topic.startswith("status.") and (value or {}).get("msg_type") == "event":
            kind = ((value.get("payload") or {}).get("kind") or "")
            if kind in ("BATCH_OK", "TASK_DONE"):
                await BROKER.produce(topic, value)
    monkeypatch.setattr("tests.helpers.kafka.AIOKafkaProducerMock.send_and_wait", dup_status, raising=True)

    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {
            "schema_version": "1.0",
            "nodes": [
                {"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all",
                 "io": {"input_inline": {}}},
                {"node_id": "agg", "type": "coordinator_fn", "depends_on": ["s"], "fan_in": "all",
                 "io": {"fn": "metrics.aggregate", "fn_args": {"node_id": "s", "mode": "sum"}}},
            ],
            "edges": [["s", "agg"]],
        }
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=10.0)
        def node_by_id(doc, node_id):
            for n in (doc.get("graph", {}).get("nodes") or []):
                if n.get("node_id") == node_id:
                    return n
            return {}
        s = node_by_id(tdoc, "s")
        got = int((s.get("stats") or {}).get("count") or 0)
        dbg("IDEMPOTENT.FINAL", count=got)
        assert got == 9
        assert str(node_by_id(tdoc, "agg").get("status")).endswith("finished")
    finally:
        await coord.stop()

@pytest_asyncio.fixture
async def workers_flaky(env_and_imports):
    cd, wu = env_and_imports
    f = wu.Worker(roles=["flaky"], handlers={"flaky": build_flaky_once_handler(wu)})
    dbg("WORKER.STARTING", role="flaky")
    await f.start()
    dbg("WORKER.STARTED", role="flaky")
    try:
        yield f
    finally:
        dbg("WORKER.STOPPING", role="flaky")
        await f.stop()
        dbg("WORKER.STOPPED", role="flaky")

@pytest.mark.asyncio
async def test_transient_failure_deferred_then_retry(env_and_imports, inmemory_db, workers_flaky):
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {
            "schema_version": "1.0",
            "nodes": [{
                "node_id": "f", "type": "flaky", "depends_on": [], "fan_in": "all",
                "retry_policy": {"max": 2, "backoff_sec": 1, "permanent_on": []},
                "io": {"input_inline": {}}
            }],
            "edges": []
        }
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        def node_by_id(doc, node_id):
            for n in (doc.get("graph", {}).get("nodes") or []):
                if n.get("node_id") == node_id:
                    return n
            return {}
        f = node_by_id(tdoc, "f")
        assert str(f.get("status")).endswith("finished")
        assert int(f.get("attempt_epoch", 0)) >= 2
    finally:
        await coord.stop()

@pytest_asyncio.fixture
async def workers_cascade(env_and_imports):
    cd, wu = env_and_imports
    wa = wu.Worker(roles=["a"], handlers={"a": build_permanent_fail_handler(wu)})
    wb = wu.Worker(roles=["b"], handlers={"b": build_noop_handler(wu, "b")})
    wc = wu.Worker(roles=["c"], handlers={"c": build_noop_handler(wu, "c")})
    for name, w in (("a", wa), ("b", wb), ("c", wc)):
        dbg("WORKER.STARTING", role=name)
        await w.start()
        dbg("WORKER.STARTED", role=name)
    try:
        yield (wa, wb, wc)
    finally:
        for name, w in (("a", wa), ("b", wb), ("c", wc)):
            dbg("WORKER.STOPPING", role=name)
            await w.stop()
            dbg("WORKER.STOPPED", role=name)

@pytest.mark.asyncio
async def test_permanent_fail_cascades_cancel_and_task_failed(env_and_imports, inmemory_db, workers_cascade):
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    cd.HB_MONITOR_TICK_SEC = 0.1  # harmless direct attr (apply_overrides also exists if needed)
    await coord.start()
    try:
        graph = {
            "schema_version": "1.0",
            "nodes": [
                {"node_id": "a", "type": "a", "depends_on": [], "fan_in": "all",
                 "io": {"input_inline": {}}},
                {"node_id": "b", "type": "b", "depends_on": ["a"], "fan_in": "all",
                 "io": {"input_inline": {}}},
                {"node_id": "c", "type": "c", "depends_on": ["a"], "fan_in": "all",
                 "io": {"input_inline": {}}},
            ],
            "edges": [["a","b"], ["a","c"]]
        }
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        async def wait_task_status(db, task_id, want, timeout=6.0):
            from time import time
            t0 = time()
            while time() - t0 < timeout:
                t = await db.tasks.find_one({"id": task_id})
                if t and str(t.get("status")) == want:
                    return t
                await asyncio.sleep(0.03)
            raise AssertionError(f"task not reached status={want} in time")

        tdoc = await wait_task_status(inmemory_db, task_id, want=str(cd.RunState.failed), timeout=8.0)

        def node_by_id(doc, node_id):
            for n in (doc.get("graph", {}).get("nodes") or []):
                if n.get("node_id") == node_id:
                    return n
            return {}

        b = node_by_id(tdoc, "b")
        c = node_by_id(tdoc, "c")
        bs = str(b.get("status"))
        cs = str(c.get("status"))
        dbg("CASCADE.FINAL", task=str(tdoc.get("status")), b=bs, c=cs)

        assert str(tdoc.get("status")) == str(cd.RunState.failed)
        assert bs in (str(cd.RunState.cancelling), str(cd.RunState.deferred), str(cd.RunState.queued))
        assert cs in (str(cd.RunState.cancelling), str(cd.RunState.deferred), str(cd.RunState.queued))
    finally:
        await coord.stop()

@pytest.mark.asyncio
async def test_status_fencing_ignores_stale_epoch(env_and_imports, inmemory_db, workers_source):
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        def node_by_id(doc, node_id):
            for n in (doc.get("graph", {}).get("nodes") or []):
                if n.get("node_id") == node_id:
                    return n
            return {}

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=10.0)
        base = int((node_by_id(tdoc, "s").get("stats") or {}).get("count") or 0)

        env = Envelope(
            msg_type=MsgType.event, role=Role.worker,
            dedup_id="stale1", task_id=task_id, node_id="s", step_type="source",
            attempt_epoch=0,
            payload={"kind": EventKind.BATCH_OK, "worker_id": "WZ", "metrics": {"count": 999},
                     "artifacts_ref": {"batch_uid": "zzz"}}
        )
        await BROKER.produce(status_topic("source"), env.model_dump(mode="json"))
        await asyncio.sleep(0.2)

        t2 = await inmemory_db.tasks.find_one({"id": task_id})
        got = int((node_by_id(t2, "s").get("stats") or {}).get("count") or 0)
        assert got == base, "stale event must be ignored by fencing"
    finally:
        await coord.stop()

@pytest.mark.asyncio
async def test_coordinator_restart_adopts_inflight_without_new_epoch(env_and_imports, inmemory_db, monkeypatch):
    cd, wu = env_and_imports
    try:
        setattr(wu, "HEARTBEAT_INTERVAL_SEC", 0.05)
    except Exception:
        pass

    w = wu.Worker(roles=["source"], handlers={"source": build_slow_source_handler(wu, total=60, batch=5, delay=0.08)})
    await w.start()
    coord1 = cd.Coordinator()
    await coord1.start()
    task_id = None
    coord2 = None
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord1.create_task(params={}, graph=graph)

        for _ in range(60):
            t = await inmemory_db.tasks.find_one({"id": task_id})
            def node_by_id(doc, node_id):
                for n in (doc.get("graph", {}).get("nodes") or []):
                    if n.get("node_id") == node_id:
                        return n
                return {}
            if t and str(node_by_id(t, "s").get("status")).endswith("running"):
                break
            await asyncio.sleep(0.05)

        await coord1.stop()
        coord2 = cd.Coordinator()
        await coord2.start()

        def node_by_id(doc, node_id):
            for n in (doc.get("graph", {}).get("nodes") or []):
                if n.get("node_id") == node_id:
                    return n
            return {}

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        s = node_by_id(tdoc, "s")
        assert int(s.get("attempt_epoch", 0)) == 1, "new coordinator must adopt inflight instead of restarting"
    finally:
        if coord2: await coord2.stop()
        await w.stop()

@pytest.mark.asyncio
async def test_explicit_cascade_cancel_moves_node_to_deferred(env_and_imports, inmemory_db, monkeypatch):
    cd, wu = env_and_imports
    try:
        cd.CANCEL_GRACE_SEC = 0.05
    except Exception:
        pass

    w = wu.Worker(roles=["source"], handlers={"source": build_cancelable_source_handler(wu, total=100, batch=10, delay=0.3)})
    await w.start()
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        def node_by_id(doc, node_id):
            for n in (doc.get("graph", {}).get("nodes") or []):
                if n.get("node_id") == node_id:
                    return n
            return {}

        for _ in range(120):
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t and str(node_by_id(t, "s").get("status")).endswith("running"):
                break
            await asyncio.sleep(0.03)

        await coord._cascade_cancel(task_id, reason="test_cancel")

        target = {str(cd.RunState.cancelling), str(cd.RunState.deferred), str(cd.RunState.queued)}
        status = None
        deadline = asyncio.get_running_loop().time() + getattr(cd, "CANCEL_GRACE_SEC", 0.05) + 1.0
        while asyncio.get_running_loop().time() < deadline:
            t2 = await inmemory_db.tasks.find_one({"id": task_id})
            status = str(node_by_id(t2, "s").get("status"))
            if status in target:
                break
            await asyncio.sleep(0.05)

        assert status in target, f"expected node status in {target}, got {status}"
    finally:
        await coord.stop()
        await w.stop()

@pytest.mark.asyncio
async def test_heartbeat_updates_lease_deadline(env_and_imports, inmemory_db, monkeypatch):
    cd, wu = env_and_imports
    try:
        setattr(wu, "HEARTBEAT_INTERVAL_SEC", 0.05)
    except Exception:
        pass

    w = wu.Worker(roles=["source"], handlers={"source": build_slow_source_handler(wu, total=40, batch=4, delay=0.12)})
    await w.start()
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        def node_by_id(doc, node_id):
            for n in (doc.get("graph", {}).get("nodes") or []):
                if n.get("node_id") == node_id:
                    return n
            return {}

        first = None
        for _ in range(120):
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t:
                lease = (node_by_id(t, "s").get("lease") or {})
                if lease.get("deadline_ts"):
                    first = int(lease["deadline_ts"])
                    break
            await asyncio.sleep(0.03)
        assert first is not None

        import time as _time
        _time.sleep(1.1)

        second = first
        for _ in range(40):
            t2 = await inmemory_db.tasks.find_one({"id": task_id})
            second = int((node_by_id(t2, "s").get("lease") or {}).get("deadline_ts") or 0)
            if second > first:
                break
            await asyncio.sleep(0.05)

        assert second > first, f"heartbeat should extend lease (first={first}, second={second})"
        await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    finally:
        await coord.stop()
        await w.stop()
