"""
End-to-end streaming smoke test.

Pipeline:
  w1(indexer) -> w2(enricher) -> w5(ocr) -> w4(analyzer)
                 \___________/           /
                      \_____ w3(merge) _/

Checks:
  - all nodes finish successfully
  - artifacts are produced for indexer/enricher/ocr
"""

from __future__ import annotations

from typing import Any, Dict

import pytest
import pytest_asyncio

from tests.helpers import dbg
from tests.helpers.graph import prime_graph, wait_task_finished
from tests.helpers.handlers import (build_analyzer_handler,
                                    build_enricher_handler,
                                    build_indexer_handler, build_ocr_handler)

# Limit available roles for the module
pytestmark = pytest.mark.worker_types("indexer,enricher,ocr,analyzer")


# ───────────────────────── Fixtures ─────────────────────────

@pytest_asyncio.fixture
async def coord(env_and_imports, inmemory_db, coord_cfg):
    """Coordinator bound to in-memory DB with fast defaults (see conftest.py)."""
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
async def workers_pipeline(env_and_imports, inmemory_db, worker_factory):
    """
    Start four workers for roles: indexer, enricher, ocr, analyzer.
    Handlers come from tests.helpers.handlers and already receive `db`.
    """
    _, wu = env_and_imports
    await worker_factory(
        ("indexer",  build_indexer_handler(db=inmemory_db)),
        ("enricher", build_enricher_handler(db=inmemory_db)),
        ("ocr",      build_ocr_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )
    # Nothing to yield; worker_factory auto-stops on teardown
    yield


# ───────────────────────── Helpers ─────────────────────────

def build_graph(*, total_skus=12, batch_size=5, mini_batch=2) -> Dict[str, Any]:
    # Use input adapters as in real flow; analyzer starts on first batch fan-in.
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
            {"node_id": "w3", "type": "coordinator_fn", "depends_on": ["w1", "w2"], "fan_in": "all",
             "io": {"fn": "merge.generic", "fn_args": {"from_nodes": ["w1", "w2"], "target": {"key": "w3-merged"}}}},
            {"node_id": "w4", "type": "analyzer", "depends_on": ["w3", "w5"], "fan_in": "any",
             "io": {"start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["w5", "w3"], "poll_ms": 40}
                    }}},
            {"node_id": "w5", "type": "ocr", "depends_on": ["w2"], "fan_in": "any",
             "io": {"start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts.rechunk:size",
                        "input_args": {"from_nodes": ["w2"], "size": 1, "poll_ms": 40, "meta_list_key": "enriched"}
                    }}},
        ],
        "edges": [
            ["w1", "w2"], ["w2", "w3"], ["w1", "w3"], ["w3", "w4"], ["w2", "w5"], ["w5", "w4"]
        ],
        # Optional extended-edge hints for async triggers (kept for realism)
        "edges_ex": [
            {"from": "w1", "to": "w2", "mode": "async", "trigger": "on_batch"},
            {"from": "w2", "to": "w5", "mode": "async", "trigger": "on_batch"},
            {"from": "w5", "to": "w4", "mode": "async", "trigger": "on_batch"},
            {"from": "w3", "to": "w4", "mode": "async", "trigger": "on_batch"},
        ],
    }


# ───────────────────────── Test ─────────────────────────

@pytest.mark.asyncio
async def test_e2e_streaming_with_kafka_sim(env_and_imports, inmemory_db, coord, workers_pipeline):
    """
    Full pipeline should complete and produce artifacts at indexer/enricher/ocr stages.
    """
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

    # Partial/complete artifacts must exist for w1, w2, w5 (batch_uid-based).
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w1"}) > 0
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w2"}) > 0
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w5"}) > 0
