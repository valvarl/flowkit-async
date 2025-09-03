"""
Tests for streaming/async fan-in behavior and metric accounting.

This suite validates:
  1) Early start of downstream when `start_when=first_batch`.
  2) Delayed start of downstream when no `start_when` hint is provided.
  3) Multi-stream fan-in to a single downstream consumer.
  4) Exactness and isolation of metric aggregation.
  5) Idempotency of metrics on duplicated STATUS events (BATCH_OK / TASK_DONE).

All tests rely on in-memory Kafka and DB, configured via fixtures from `conftest.py`.
"""

import asyncio
from typing import Any, Dict, Optional

import pytest
import pytest_asyncio

from tests.helpers import BROKER, AIOKafkaProducerMock, dbg, prime_graph, wait_task_finished

# Limit worker types for this module (see conftest._worker_types_from_marker).
pytestmark = pytest.mark.worker_types("indexer,enricher,ocr,analyzer")


# ───────────────────────── Fixtures ─────────────────────────


@pytest_asyncio.fixture
async def coord(env_and_imports, inmemory_db, coord_cfg):
    """
    Start a Coordinator with a fast-ticking test config and in-memory DB.
    """
    cd, _ = env_and_imports
    c = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
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
async def workers_indexer_analyzer(env_and_imports, handlers, inmemory_db, worker_cfg):
    """
    Start a minimal worker set: one 'indexer' upstream and one 'analyzer' downstream.
    Handlers come from `conftest.handlers`.
    """
    _, wu = env_and_imports
    w_idx = wu.Worker(db=inmemory_db, cfg=worker_cfg, roles=["indexer"], handlers={"indexer": handlers["indexer"]})
    w_ana = wu.Worker(db=inmemory_db, cfg=worker_cfg, roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})

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
async def workers_3indexers_analyzer(env_and_imports, handlers, inmemory_db, worker_cfg):
    """
    Start three independent 'indexer' workers (same role, shared consumer group in in-mem Kafka)
    and a single 'analyzer' downstream. This creates real competition in the group.
    """
    _, wu = env_and_imports
    w_idx1 = wu.Worker(db=inmemory_db, cfg=worker_cfg, roles=["indexer"], handlers={"indexer": handlers["indexer"]})
    w_idx2 = wu.Worker(db=inmemory_db, cfg=worker_cfg, roles=["indexer"], handlers={"indexer": handlers["indexer"]})
    w_idx3 = wu.Worker(db=inmemory_db, cfg=worker_cfg, roles=["indexer"], handlers={"indexer": handlers["indexer"]})
    w_ana = wu.Worker(db=inmemory_db, cfg=worker_cfg, roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})

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


async def _get_task(db, task_id: str) -> Optional[Dict[str, Any]]:
    """Fetch task document by id from the in-memory DB."""
    return await db.tasks.find_one({"id": task_id})


def _node_status(doc: Dict[str, Any], node_id: str) -> Any:
    """Get node status by node_id from a task doc."""
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n.get("status")
    return None


async def wait_node_running(db, task_id: str, node_id: str, timeout: float = 5.0) -> Dict[str, Any]:
    """
    Wait until a specific node transitions to a 'running' status.
    Raises AssertionError on timeout.
    """
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


async def wait_node_not_running_for(db, task_id: str, node_id: str, hold: float = 0.6) -> None:
    """
    Ensure that a node does NOT enter 'running' within a small hold window.
    Useful for negative checks when downstream must wait for upstream completion.
    """
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


def _make_indexer(node_id: str, total: int, batch: int) -> Dict[str, Any]:
    """
    Build a simple indexer node:
      - emits `total` items in batches of `batch`.
      - uses inline input to avoid external dependencies.
    """
    return {
        "node_id": node_id,
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": batch, "total_skus": total}},
        "status": None,
        "attempt_epoch": 0,
    }


def add_metrics_agg(graph: Dict[str, Any], *, target: str = "d", name: Optional[str] = None) -> Dict[str, Any]:
    """
    Add a coordinator_fn node that aggregates metrics of `target` node into its graph node.stats.
    The function used is `metrics.aggregate` with mode='sum'.
    """
    name = name or f"agg_{target}"
    graph.setdefault("nodes", [])
    graph.setdefault("edges", [])
    graph["nodes"].append(
        {
            "node_id": name,
            "type": "coordinator_fn",
            "depends_on": [target],
            "fan_in": "all",
            "io": {"fn": "metrics.aggregate", "fn_args": {"node_id": target, "mode": "sum"}},
            "status": None,
            "attempt_epoch": 0,
        }
    )
    graph["edges"].append([target, name])
    return graph


def _node_by_id(doc: Dict[str, Any], node_id: str) -> Dict[str, Any]:
    """Return the node dict by id from a task document."""
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n
    return {}


def _get_count(doc: Dict[str, Any], node_id: str) -> int:
    """Return the aggregated 'count' metric from a node's stats (0 if absent)."""
    node = _node_by_id(doc or {}, node_id)
    return int(((node.get("stats") or {}).get("count") or 0))


# ───────────────────────── Tests ─────────────────────────


@pytest.mark.asyncio
async def test_start_when_first_batch_starts_early(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Downstream should start while upstream is still running when `start_when=first_batch` is set.
    """
    cd, _ = env_and_imports

    u = _make_indexer("u", total=12, batch=4)  # 3 upstream batches
    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {"input_adapter": "pull.from_artifacts", "input_args": {"from_nodes": ["u"], "poll_ms": 30}},
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u", "d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.FIRSTBATCH.CREATED", task_id=task_id)

    doc_when_d_runs = await wait_node_running(inmemory_db, task_id, "d", timeout=6.0)
    st_u = _node_status(doc_when_d_runs, "u")
    dbg("FIRSTBATCH.START_OBSERVED", u=st_u, d="running")

    # Upstream must not yet be finished; otherwise early start is not verified.
    assert not str(st_u).endswith("finished"), "Upstream already finished, early start not verified"

    # Eventually everything must finish.
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("FIRSTBATCH.FINAL", statuses=final)
    assert final["u"] == cd.RunState.finished
    assert final["d"] == cd.RunState.finished


@pytest.mark.asyncio
async def test_after_upstream_complete_delays_start(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Without `start_when`, downstream must not start until upstream completes.
    """
    cd, _ = env_and_imports

    u = _make_indexer("u", total=10, batch=5)  # noticeable running window
    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {"input_inline": {"input_adapter": "pull.from_artifacts", "input_args": {"from_nodes": ["u"], "poll_ms": 30}}},
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u", "d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.AFTERCOMP.CREATED", task_id=task_id)

    # Ensure the downstream does NOT start during the hold window.
    await wait_node_not_running_for(inmemory_db, task_id, "d", hold=0.8)

    # Then everything should finish.
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("AFTERCOMP.FINAL", statuses=final)
    assert final["u"] == cd.RunState.finished
    assert final["d"] == cd.RunState.finished


@pytest.mark.asyncio
async def test_multistream_fanin_stream_to_one_downstream(env_and_imports, inmemory_db, coord, workers_3indexers_analyzer):
    """
    Multi-stream fan-in: three upstream indexers stream into one analyzer.
    Analyzer should start early on first batch and eventually see the full flow.
    """
    cd, _ = env_and_imports

    # Three sources with several batches each.
    u1 = _make_indexer("u1", total=9, batch=3)   # 3 batches
    u2 = _make_indexer("u2", total=8, batch=4)   # 2 batches
    u3 = _make_indexer("u3", total=12, batch=3)  # 4 batches

    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u1", "u2", "u3"],
        "fan_in": "any",
        "io": {
            "start_when": "first_batch",
            "input_inline": {"input_adapter": "pull.from_artifacts", "input_args": {"from_nodes": ["u1", "u2", "u3"], "poll_ms": 25}},
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {"schema_version": "1.0", "nodes": [u1, u2, u3, d], "edges": [["u1", "d"], ["u2", "d"], ["u3", "d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.MULTISTREAM.CREATED", task_id=task_id)

    doc_when_d_runs = await wait_node_running(inmemory_db, task_id, "d", timeout=8.0)
    st_u = {nid: _node_status(doc_when_d_runs, nid) for nid in ("u1", "u2", "u3")}
    dbg("MULTISTREAM.START_OBSERVED", u1=st_u["u1"], u2=st_u["u2"], u3=st_u["u3"], d="running")

    # Not all upstreams should be finished at the analyzer start moment.
    assert sum(1 for s in st_u.values() if str(s).endswith("finished")) < 3

    # Final state must be finished for all nodes.
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("MULTISTREAM.FINAL", statuses=final)
    assert final["d"] == cd.RunState.finished
    assert final["u1"] == cd.RunState.finished
    assert final["u2"] == cd.RunState.finished
    assert final["u3"] == cd.RunState.finished

    # Analyzer aggregated count should be at least the total items across sources.
    d_node = next(n for n in tdoc["graph"]["nodes"] if n["node_id"] == "d")
    got = int(((d_node.get("stats") or {}).get("count") or 0))
    dbg("MULTISTREAM.D.COUNT", count=got)
    assert got >= (9 + 8 + 12)


@pytest.mark.asyncio
async def test_metrics_single_stream_exact_count(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Single upstream -> single downstream: analyzer's aggregated 'count' must equal the total items.
    """
    cd, _ = env_and_imports

    total, batch = 13, 5  # 5 + 5 + 3
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {"input_adapter": "pull.from_artifacts", "input_args": {"from_nodes": ["u"], "poll_ms": 25}},
        },
        "status": None,
        "attempt_epoch": 0,
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u", "d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.SINGLE", expect=total, got=got)
    assert got == total


@pytest.mark.asyncio
async def test_metrics_multistream_exact_sum(env_and_imports, inmemory_db, coord, workers_3indexers_analyzer):
    """
    Three upstreams -> one downstream: analyzer's aggregated 'count' must equal the sum of all totals.
    """
    cd, _ = env_and_imports

    totals = {"u1": 6, "u2": 7, "u3": 9}  # sum = 22
    u1 = _make_indexer("u1", total=totals["u1"], batch=3)
    u2 = _make_indexer("u2", total=totals["u2"], batch=4)
    u3 = _make_indexer("u3", total=totals["u3"], batch=4)

    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u1", "u2", "u3"],
        "fan_in": "any",
        "io": {
            "start_when": "first_batch",
            "input_inline": {"input_adapter": "pull.from_artifacts", "input_args": {"from_nodes": ["u1", "u2", "u3"], "poll_ms": 20}},
        },
        "status": None,
        "attempt_epoch": 0,
    }
    graph = {"schema_version": "1.0", "nodes": [u1, u2, u3, d], "edges": [["u1", "d"], ["u2", "d"], ["u3", "d"]]}
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
    """
    With a remainder in the last upstream batch, analyzer's 'count' must still exactly equal total.
    """
    cd, _ = env_and_imports

    total, batch = 10, 3  # 3 + 3 + 3 + 1
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {"input_adapter": "pull.from_artifacts", "input_args": {"from_nodes": ["u"], "poll_ms": 25}},
        },
        "status": None,
        "attempt_epoch": 0,
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u", "d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.PARTIAL", expect=total, got=got)
    assert got == total


@pytest.mark.asyncio
async def test_metrics_isolation_between_tasks(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Two back-to-back tasks must keep metric aggregation isolated per task document.
    """
    cd, _ = env_and_imports

    async def run_once(total: int, batch: int):
        u = _make_indexer("u", total=total, batch=batch)
        d = {
            "node_id": "d",
            "type": "analyzer",
            "depends_on": ["u"],
            "fan_in": "all",
            "io": {
                "start_when": "first_batch",
                "input_inline": {"input_adapter": "pull.from_artifacts", "input_args": {"from_nodes": ["u"], "poll_ms": 25}},
            },
            "status": None,
            "attempt_epoch": 0,
        }
        g = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u", "d"]]}
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


@pytest.mark.asyncio
async def test_metrics_idempotent_on_duplicate_status_events(env_and_imports, inmemory_db, coord, workers_indexer_analyzer, monkeypatch):
    """
    Duplicate STATUS events (BATCH_OK / TASK_DONE) must not double-increment aggregated metrics.

    We monkeypatch AIOKafkaProducerMock.send_and_wait to produce the same STATUS event twice
    for status topics. The Coordinator should deduplicate by envelope key and keep metrics stable.
    """
    cd, _ = env_and_imports

    # Save the original method to call once, then produce duplicate via broker.
    orig_send = AIOKafkaProducerMock.send_and_wait

    async def dup_status(self, topic, value, key=None):
        # First, the normal send.
        await orig_send(self, topic, value, key)
        # Then, if it's a STATUS event, produce a duplicate of the same envelope.
        if topic.startswith("status.") and (value or {}).get("msg_type") == "event":
            kind = ((value.get("payload") or {}).get("kind") or "")
            if kind in ("BATCH_OK", "TASK_DONE"):
                await BROKER.produce(topic, value)

    # Patch the mock implementation used by both worker and coordinator sides.
    monkeypatch.setattr("tests.helpers.kafka.AIOKafkaProducerMock.send_and_wait", dup_status, raising=True)

    total, batch = 18, 6  # 3 upstream batches
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {"input_adapter": "pull.from_artifacts", "input_args": {"from_nodes": ["u"], "poll_ms": 25}},
        },
        "status": None,
        "attempt_epoch": 0,
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u", "d"]]}
    graph = add_metrics_agg(graph, target="d")
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.DEDUP", expect=total, got=got)
    assert got == total
