"""Isolation of aggregation and cross-talk guard between tasks."""

from __future__ import annotations

import pytest
from tests.helpers.graph import wait_task_finished
from tests.helpers.handlers import build_analyzer_handler, build_indexer_handler

from flowkit.core.log import log_context

from ._helpers import _get_count, _make_indexer

pytestmark = [pytest.mark.streaming, pytest.mark.worker_types("indexer,enricher,ocr,analyzer")]


@pytest.mark.asyncio
async def test_metrics_isolation_between_tasks(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Two back-to-back tasks must keep metric aggregation isolated per task document.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="metrics_isolation_between_tasks")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    async def run_once(total: int, batch: int):
        u = _make_indexer("u", total=total, batch=batch)
        d = {
            "node_id": "d",
            "type": "analyzer",
            "depends_on": ["u"],
            "fan_in": "all",
            "io": {
                "start_when": "first_batch",
                "input_inline": {
                    "input_adapter": "pull.from_artifacts",
                    "input_args": {"from_nodes": ["u"], "poll_ms": 25},
                },
            },
        }
        g = {"schema_version": "1.0", "nodes": [u, d]}
        tid = await coord.create_task(params={}, graph=g)
        with log_context(task_id=tid):
            tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)
            return tid, _get_count(tdoc, "d")

    tid1, c1 = await run_once(7, 3)
    tid2, c2 = await run_once(11, 4)

    tlog.debug("metrics.isolation", event="metrics.isolation", task1=tid1, count1=c1, task2=tid2, count2=c2)
    assert c1 == 7
    assert c2 == 11


@pytest.mark.asyncio
async def test_metrics_cross_talk_guard(env_and_imports, inmemory_db, coord, worker_factory, monkeypatch, tlog):
    """
    Two concurrent tasks (same node_ids across different task_ids) do not interfere:
    final 'count' values are isolated per task.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="metrics_cross_talk_guard")

    # Slow down slightly to force overlap in execution.
    monkeypatch.setenv("TEST_IDX_PROCESS_SLEEP_SEC", "0.1")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    def build_graph(total: int, batch: int):
        u = _make_indexer("u", total=total, batch=batch)
        d = {
            "node_id": "d",
            "type": "analyzer",
            "depends_on": ["u"],
            "fan_in": "all",
            "io": {
                "start_when": "first_batch",
                "input_inline": {
                    "input_adapter": "pull.from_artifacts",
                    "input_args": {"from_nodes": ["u"], "poll_ms": 20},
                },
            },
        }
        return {"schema_version": "1.0", "nodes": [u, d]}

    g1 = build_graph(15, 5)
    g2 = build_graph(21, 4)

    # Start both tasks back-to-back to ensure real overlap on topics.
    t1 = await coord.create_task(params={}, graph=g1)
    t2 = await coord.create_task(params={}, graph=g2)

    with log_context(task_id=t1):
        doc1 = await wait_task_finished(inmemory_db, t1, timeout=14.0)
        c1 = _get_count(doc1, "d")
    with log_context(task_id=t2):
        doc2 = await wait_task_finished(inmemory_db, t2, timeout=14.0)
        c2 = _get_count(doc2, "d")

    tlog.debug("metrics.crosstalk", event="metrics.crosstalk", t1=t1, c1=c1, t2=t2, c2=c2)
    assert c1 == 15
    assert c2 == 21
