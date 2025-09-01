import asyncio

import pytest
import pytest_asyncio

from tests.helpers import (BROKER, AIOKafkaProducerMock, dbg, prime_graph,
                           wait_task_finished)
from tests.helpers.handlers import make_test_handlers

pytestmark = pytest.mark.worker_types("indexer,enricher,ocr,analyzer")

# ───────────────────────── Fixtures ─────────────────────────

@pytest_asyncio.fixture
async def coord(env_and_imports, inmemory_db):
    cd, _ = env_and_imports
    cfg = cd.CoordinatorConfig.load(overrides={
        "scheduler_tick_sec": 0.05,
        "discovery_window_sec": 0.05,
        "finalizer_tick_sec": 0.05,
        "hb_monitor_tick_sec": 0.2,
        "cancel_grace_sec": 0.1,
        "outbox_dispatch_tick_sec": 0.05,
    })
    c = cd.Coordinator(db=inmemory_db, cfg=cfg)
    dbg("COORD.STARTING")
    await c.start()
    dbg("COORD.STARTED")
    try:
        yield c
    finally:
        dbg("COORD.STOPPING")
        await c.stop()
        dbg("COORD.STOPPED")

@pytest_asyncio.fixture
async def workers_indexer_analyzer(env_and_imports, handlers, inmemory_db):
    _, wu = env_and_imports
    h = handlers  # from conftest; includes common handlers by default
    wcfg = wu.WorkerConfig.load(overrides={
        "hb_interval_sec": 0.2,
        "lease_ttl_sec": 2,
        "db_cancel_poll_ms": 50,
        "pull_poll_ms_default": 50,
        "pull_empty_backoff_ms_max": 300,
    })
    w_idx = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["indexer"],  handlers={"indexer":  h["indexer"]})
    w_ana = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["analyzer"], handlers={"analyzer": h["analyzer"]})
    for name, w in (("indexer", w_idx), ("analyzer", w_ana)):
        dbg("WORKER.STARTING", role=name)
        await w.start()
        dbg("WORKER.STARTED", role=name)
    try:
        yield (w_idx, w_ana)
    finally:
        for name, w in (("indexer", w_idx), ("analyzer", w_ana)):
            dbg("WORKER.STOPPING", role=name)
            await w.stop()
            dbg("WORKER.STOPPED", role=name)

@pytest_asyncio.fixture
async def workers_3indexers_analyzer(env_and_imports, handlers, inmemory_db):
    _, wu = env_and_imports
    h = handlers
    wcfg = wu.WorkerConfig.load(overrides={
        "hb_interval_sec": 0.2,
        "lease_ttl_sec": 2,
        "db_cancel_poll_ms": 50,
        "pull_poll_ms_default": 50,
        "pull_empty_backoff_ms_max": 300,
    })
    w_idx1 = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["indexer"],  handlers={"indexer":  h["indexer"]})
    w_idx2 = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["indexer"],  handlers={"indexer":  h["indexer"]})
    w_idx3 = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["indexer"],  handlers={"indexer":  h["indexer"]})
    w_ana  = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["analyzer"], handlers={"analyzer": h["analyzer"]})
    workers = [("indexer#1", w_idx1), ("indexer#2", w_idx2), ("indexer#3", w_idx3), ("analyzer", w_ana)]
    for name, w in workers:
        dbg("WORKER.STARTING", role=name)
        await w.start()
        dbg("WORKER.STARTED", role=name)
    try:
        yield (w_idx1, w_idx2, w_idx3, w_ana)
    finally:
        for name, w in workers:
            dbg("WORKER.STOPPING", role=name)
            await w.stop()
            dbg("WORKER.STOPPED", role=name)

# ───────────────────────── Small helpers ─────────────────────────

async def _get_task(db, task_id):
    return await db.tasks.find_one({"id": task_id})

def _node_status(doc, node_id):
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n.get("status")
    return None

async def wait_node_running(db, task_id, node_id, timeout=5.0):
    from time import time
    t0 = time()
    while time() - t0 < timeout:
        doc = await _get_task(db, task_id)
        if doc:
            st = _node_status(doc, node_id)
            if str(st).endswith("running"):
                return doc
        await asyncio.sleep(0.02)
    raise AssertionError(f"node {node_id} not running in time")

async def wait_node_not_running_for(db, task_id, node_id, hold=0.6):
    from time import time
    t0 = time()
    seen_running = False
    while time() - t0 < hold:
        doc = await _get_task(db, task_id)
        st = _node_status(doc or {}, node_id)
        if str(st).endswith("running"):
            seen_running = True
            break
        await asyncio.sleep(0.03)
    assert not seen_running, f"{node_id} unexpectedly started during hold window"

def _make_indexer(node_id, total, batch):
    return {
        "node_id": node_id, "type": "indexer",
        "depends_on": [], "fan_in": "all",
        "io": {"input_inline": {"batch_size": batch, "total_skus": total}},
        "status": None, "attempt_epoch": 0
    }

def add_metrics_agg(graph: dict, *, target: str = "d", name: str | None = None) -> dict:
    name = name or f"agg_{target}"
    graph.setdefault("nodes", [])
    graph.setdefault("edges", [])
    graph["nodes"].append({
        "node_id": name,
        "type": "coordinator_fn",
        "depends_on": [target],
        "fan_in": "all",
        "io": {"fn": "metrics.aggregate", "fn_args": {"node_id": target, "mode": "sum"}},
        "status": None, "attempt_epoch": 0,
    })
    graph["edges"].append([target, name])
    return graph

# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_start_when_first_batch_starts_early(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    cd, _ = env_and_imports
    u = _make_indexer("u", total=12, batch=4)
    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u"], "fan_in": "all",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u"], "poll_ms": 30}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.FIRSTBATCH.CREATED", task_id=task_id)

    doc_when_d_runs = await wait_node_running(inmemory_db, task_id, "d", timeout=6.0)
    st_u = _node_status(doc_when_d_runs, "u")
    dbg("FIRSTBATCH.START_OBSERVED", u=st_u, d="running")

    assert not str(st_u).endswith("finished"), "Upstream already finished, early start not verified"

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("FIRSTBATCH.FINAL", statuses=final)
    assert final["u"] == cd.RunState.finished
    assert final["d"] == cd.RunState.finished

@pytest.mark.asyncio
async def test_after_upstream_complete_delays_start(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    cd, _ = env_and_imports

    u = _make_indexer("u", total=10, batch=5)
    d = {
        "node_id":"d", "type":"analyzer",
        "depends_on":["u"], "fan_in":"all",
        "io": {"input_inline": {"input_adapter":"pull.from_artifacts",
                                "input_args":{"from_nodes":["u"], "poll_ms": 30}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version":"1.0", "nodes":[u,d], "edges":[["u","d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.AFTERCOMP.CREATED", task_id=task_id)

    await wait_node_not_running_for(inmemory_db, task_id, "d", hold=0.8)

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("AFTERCOMP.FINAL", statuses=final)
    assert final["u"] == cd.RunState.finished
    assert final["d"] == cd.RunState.finished

@pytest.mark.asyncio
async def test_multistream_fanin_stream_to_one_downstream(env_and_imports, inmemory_db, coord, workers_3indexers_analyzer):
    cd, _ = env_and_imports
    u1 = _make_indexer("u1", total=9,  batch=3)
    u2 = _make_indexer("u2", total=8,  batch=4)
    u3 = _make_indexer("u3", total=12, batch=3)

    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u1","u2","u3"], "fan_in": "any",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u1","u2","u3"], "poll_ms": 25}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version": "1.0",
             "nodes": [u1,u2,u3,d],
             "edges": [["u1","d"],["u2","d"],["u3","d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.MULTISTREAM.CREATED", task_id=task_id)

    doc_when_d_runs = await wait_node_running(inmemory_db, task_id, "d", timeout=8.0)
    st_u = {nid: _node_status(doc_when_d_runs, nid) for nid in ("u1","u2","u3")}
    dbg("MULTISTREAM.START_OBSERVED", u1=st_u["u1"], u2=st_u["u2"], u3=st_u["u3"], d="running")
    assert sum(1 for s in st_u.values() if str(s).endswith("finished")) < 3

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("MULTISTREAM.FINAL", statuses=final)
    assert final["d"] == cd.RunState.finished
    assert final["u1"] == cd.RunState.finished
    assert final["u2"] == cd.RunState.finished
    assert final["u3"] == cd.RunState.finished

    d_node = next(n for n in tdoc["graph"]["nodes"] if n["node_id"] == "d")
    got = int(((d_node.get("stats") or {}).get("count") or 0))
    dbg("MULTISTREAM.D.COUNT", count=got)
    assert got >= (9 + 8 + 12)

def _node_by_id(doc, node_id):
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n
    return {}

def _get_count(doc, node_id):
    node = _node_by_id(doc or {}, node_id)
    return int(((node.get("stats") or {}).get("count") or 0))

@pytest.mark.asyncio
async def test_metrics_single_stream_exact_count(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    cd, _ = env_and_imports
    total, batch = 13, 5
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u"], "fan_in": "all",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u"], "poll_ms": 25}}},
        "status": None, "attempt_epoch": 0
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.SINGLE", expect=total, got=got)
    assert got == total

@pytest.mark.asyncio
async def test_metrics_multistream_exact_sum(env_and_imports, inmemory_db, coord, workers_3indexers_analyzer):
    cd, _ = env_and_imports
    totals = {"u1": 6, "u2": 7, "u3": 9}
    u1 = _make_indexer("u1", total=totals["u1"], batch=3)
    u2 = _make_indexer("u2", total=totals["u2"], batch=4)
    u3 = _make_indexer("u3", total=totals["u3"], batch=4)

    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u1","u2","u3"], "fan_in": "any",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u1","u2","u3"], "poll_ms": 20}}},
        "status": None, "attempt_epoch": 0
    }
    graph = {"schema_version": "1.0",
             "nodes": [u1, u2, u3, d],
             "edges": [["u1","d"], ["u2","d"], ["u3","d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)

    expect = sum(totals.values())
    got = _get_count(tdoc, "d")
    dbg("METRICS.MULTI", expect=expect, got=got)
    assert got == expect

@pytest.mark.asyncio
async def test_metrics_partial_batches_exact_count(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    cd, _ = env_and_imports
    total, batch = 10, 3
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u"], "fan_in": "all",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u"], "poll_ms": 25}}},
        "status": None, "attempt_epoch": 0
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.PARTIAL", expect=total, got=got)
    assert got == total

@pytest.mark.asyncio
async def test_metrics_isolation_between_tasks(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    cd, _ = env_and_imports

    async def run_once(total, batch):
        u = _make_indexer("u", total=total, batch=batch)
        d = {
            "node_id": "d", "type": "analyzer",
            "depends_on": ["u"], "fan_in": "all",
            "io": {"start_when": "first_batch",
                   "input_inline": {"input_adapter": "pull.from_artifacts",
                                    "input_args": {"from_nodes": ["u"], "poll_ms": 25}}},
            "status": None, "attempt_epoch": 0
        }
        g = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
        g = add_metrics_agg(g, target="d")
        g = prime_graph(cd, g)
        tid = await coord.create_task(params={}, graph=g)
        tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)
        return tid, _get_count(tdoc, "d")

    tid1, c1 = await run_once(7, 3)
    tid2, c2 = await run_once(11, 4)

    dbg("METRICS.ISOLATION", task1=tid1, count1=c1, task2=tid2, count2=c2)
    assert c1 == 7
    assert c2 == 11

from tests.helpers import BROKER, AIOKafkaProducerMock  # type: ignore


@pytest.mark.asyncio
async def test_metrics_idempotent_on_duplicate_status_events(env_and_imports, inmemory_db, coord, workers_indexer_analyzer, monkeypatch):
    cd, _ = env_and_imports

    orig_send = AIOKafkaProducerMock.send_and_wait

    async def dup_status(self, topic, value, key=None):
        await orig_send(self, topic, value, key)
        if topic.startswith("status.") and (value or {}).get("msg_type") == "event":
            kind = ((value.get("payload") or {}).get("kind") or "")
            if kind in ("BATCH_OK", "TASK_DONE"):
                await BROKER.produce(topic, value)

    monkeypatch.setattr(
        "tests.helpers.kafka.AIOKafkaProducerMock.send_and_wait",
        dup_status,
        raising=True
    )

    total, batch = 18, 6
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u"], "fan_in": "all",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u"], "poll_ms": 25}}},
        "status": None, "attempt_epoch": 0
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.DEDUP", expect=total, got=got)
    assert got == total
