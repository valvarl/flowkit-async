r"""
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

from typing import Any

import pytest
import pytest_asyncio

from flowkit.core.log import log_context
from tests.helpers.graph import wait_task_finished
from tests.helpers.handlers import (
    build_analyzer_handler,
    build_enricher_handler,
    build_indexer_handler,
    build_ocr_handler,
)

# Limit available roles for the module
pytestmark = pytest.mark.worker_types("indexer,enricher,ocr,analyzer")


# ───────────────────────── Fixtures ─────────────────────────


@pytest_asyncio.fixture
async def workers_pipeline(env_and_imports, inmemory_db, worker_factory, tlog):
    """
    Start four workers for roles: indexer, enricher, ocr, analyzer.
    Handlers come from tests.helpers.handlers and already receive `db`.
    """
    tlog.debug("test.workers.spawn", event="test.workers.spawn", roles=["indexer", "enricher", "ocr", "analyzer"])
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("enricher", build_enricher_handler(db=inmemory_db)),
        ("ocr", build_ocr_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )
    yield  # worker_factory auto-stops on teardown


# ───────────────────────── Helpers ─────────────────────────


def build_graph(*, total_skus=12, batch_size=5, mini_batch=2) -> dict[str, Any]:
    # Use input adapters as in real flow; analyzer starts on first batch fan-in.
    return {
        "schema_version": "1.0",
        "nodes": [
            {
                "node_id": "w1",
                "type": "indexer",
                "depends_on": [],
                "fan_in": "all",
                "io": {"input_inline": {"batch_size": batch_size, "total_skus": total_skus}},
            },
            {
                "node_id": "w2",
                "type": "enricher",
                "depends_on": ["w1"],
                "fan_in": "any",
                "io": {
                    "start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts.rechunk:size",
                        "input_args": {
                            "from_nodes": ["w1"],
                            "size": mini_batch,
                            "poll_ms": 50,
                            "meta_list_key": "skus",
                        },
                    },
                },
            },
            {
                "node_id": "w3",
                "type": "coordinator_fn",
                "depends_on": ["w1", "w2"],
                "fan_in": "all",
                "io": {"fn": "merge.generic", "fn_args": {"from_nodes": ["w1", "w2"], "target": {"key": "w3-merged"}}},
            },
            {
                "node_id": "w4",
                "type": "analyzer",
                "depends_on": ["w3", "w5"],
                "fan_in": "any",
                "io": {
                    "start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["w5", "w3"], "poll_ms": 40},
                    },
                },
            },
            {
                "node_id": "w5",
                "type": "ocr",
                "depends_on": ["w2"],
                "fan_in": "any",
                "io": {
                    "start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts.rechunk:size",
                        "input_args": {"from_nodes": ["w2"], "size": 1, "poll_ms": 40, "meta_list_key": "enriched"},
                    },
                },
            },
        ],
    }


# ───────────────────────── Test ─────────────────────────


@pytest.mark.asyncio
async def test_e2e_streaming_with_kafka_sim(env_and_imports, inmemory_db, coord, workers_pipeline, tlog):
    """
    Full pipeline should complete and produce artifacts at indexer/enricher/ocr stages.
    """
    cd, _ = env_and_imports

    graph = build_graph(total_skus=12, batch_size=5, mini_batch=3)
    tlog.debug("test.graph.built", event="test.graph.built", nodes=len(graph["nodes"]))

    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("test.task.created", event="test.task.created", task_id=task_id)

    # добавим task_id в контекст, чтобы все последующие логи теста были «связаны»
    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
        tlog.debug("test.final.statuses", event="test.final.statuses", statuses=st)

        assert st["w1"] == cd.RunState.finished
        assert st["w2"] == cd.RunState.finished
        assert st["w3"] == cd.RunState.finished
        assert st["w5"] == cd.RunState.finished
        assert st["w4"] == cd.RunState.finished

        # Partial/complete artifacts must exist for w1, w2, w5 (batch_uid-based).
        a_w1 = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w1"})
        a_w2 = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w2"})
        a_w5 = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w5"})
        tlog.debug("test.artifacts.counts", event="test.artifacts.counts", w1=a_w1, w2=a_w2, w5=a_w5)

        assert a_w1 > 0
        assert a_w2 > 0
        assert a_w5 > 0
