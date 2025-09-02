import asyncio
from typing import Any, Dict

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
async def worker(env_and_imports, handlers, inmemory_db):
    _, wu = env_and_imports
    wcfg = wu.WorkerConfig.load(overrides={
        "hb_interval_sec": 0.2,
        "lease_ttl_sec": 2,
        "db_cancel_poll_ms": 50,
        "pull_poll_ms_default": 50,
        "pull_empty_backoff_ms_max": 300,
    })
    
    w_indexer  = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_enricher = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["enricher"], handlers={"enricher": handlers["enricher"]})
    w_ocr      = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["ocr"],      handlers={"ocr":      handlers["ocr"]})
    w_analyzer = wu.Worker(db=inmemory_db, cfg=wcfg, roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})

    for name, w in (("indexer", w_indexer), ("enricher", w_enricher), ("ocr", w_ocr), ("analyzer", w_analyzer)):
        dbg("WORKER.STARTING", role=name)
        await w.start()
        dbg("WORKER.STARTED", role=name)

    try:
        yield (w_indexer, w_enricher, w_ocr, w_analyzer)
    finally:
        for name, w in (("indexer", w_indexer), ("enricher", w_enricher), ("ocr", w_ocr), ("analyzer", w_analyzer)):
            dbg("WORKER.STOPPING", role=name)
            await w.stop()
            dbg("WORKER.STOPPED", role=name)


# ───────────────────────── Small helpers ─────────────────────────

def build_graph(total_skus=12, batch_size=5, mini_batch=2) -> Dict[str, Any]:
    return {
      "schema_version": "1.0",
      "nodes": [
        {"node_id": "w1", "type": "indexer", "depends_on": [], "fan_in": "all",
         "io": {"input_inline": {"batch_size": batch_size, "total_skus": total_skus}}},
        {"node_id": "w2", "type": "enricher", "depends_on": ["w1"], "fan_in": "any",
         "io": {"start_when": "first_batch",
                "input_inline": {
                    "input_adapter": "pull.from_artifacts.rechunk:size",
                    "input_args": {"from_nodes": ["w1"], "size": mini_batch, "poll_ms": 50, "meta_list_key": "skus"}
                }}},
        {"node_id": "w3", "type": "coordinator_fn", "depends_on": ["w1","w2"], "fan_in": "all",
         "io": {"fn": "merge.generic", "fn_args": {"from_nodes": ["w1","w2"], "target": {"key": "w3-merged"}}}},
        {"node_id": "w4", "type": "analyzer", "depends_on": ["w3","w5"], "fan_in": "any",
         "io": {"start_when": "first_batch",
                "input_inline": {
                    "input_adapter": "pull.from_artifacts",
                    "input_args": {"from_nodes": ["w5","w3"], "poll_ms": 40}
                }}},
        {"node_id": "w5", "type": "ocr", "depends_on": ["w2"], "fan_in": "any",
         "io": {"start_when": "first_batch",
                "input_inline": {
                    "input_adapter": "pull.from_artifacts.rechunk:size",
                    "input_args": {"from_nodes": ["w2"], "size": 1, "poll_ms": 40, "meta_list_key": "enriched"}
                }}},
        ],
        "edges": [
            ["w1","w2"], ["w2","w3"], ["w1","w3"], ["w3","w4"], ["w2","w5"], ["w5","w4"]
        ],
        "edges_ex": [
            {"from":"w1","to":"w2","mode":"async","trigger":"on_batch"},
            {"from":"w2","to":"w5","mode":"async","trigger":"on_batch"},
            {"from":"w5","to":"w4","mode":"async","trigger":"on_batch"},
            {"from":"w3","to":"w4","mode":"async","trigger":"on_batch"}
        ]
    }

# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_e2e_streaming_with_kafka_sim(env_and_imports, inmemory_db, coord, worker):
    cd, _ = env_and_imports

    graph = prime_graph(cd, build_graph(total_skus=12, batch_size=5, mini_batch=3))
    task_id = await coord.create_task(params={}, graph=graph)
    dbg("TEST.TASK_CREATED", task_id=task_id)

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("TEST.FINAL.STATUSES", statuses=st)
    assert st["w1"] == cd.RunState.finished
    assert st["w2"] == cd.RunState.finished
    assert st["w3"] == cd.RunState.finished
    assert st["w5"] == cd.RunState.finished
    assert st["w4"] == cd.RunState.finished

    # есть partial/complete артефакты у w1,w2,w5 (batch_uid-based)
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w1"}) > 0
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w2"}) > 0
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w5"}) > 0
