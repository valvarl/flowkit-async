"""
Tests for streaming/async fan-in behavior: early vs delayed downstream start.
"""

from __future__ import annotations

import pytest
from tests.helpers.graph import (
    wait_node_finished,
    wait_node_not_running_for,
    wait_node_running,
    wait_task_finished,
)
from tests.helpers.handlers import build_analyzer_handler, build_indexer_handler

from flowkit.core.log import log_context

from ._helpers import _make_indexer, _node_status

# Limit worker types for this module (see conftest._worker_types_from_marker).
pytestmark = [pytest.mark.streaming, pytest.mark.worker_types("indexer,enricher,ocr,analyzer")]


@pytest.mark.asyncio
async def test_start_when_first_batch_starts_early(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Downstream should start while upstream is still running when `start_when=first_batch` is set.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="start_when_first_batch_starts_early")

    # Start workers via factory (separate handler instances, shared role topics).
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    u = _make_indexer("u", total=12, batch=4)  # 3 upstream batches
    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u"], "poll_ms": 30},
            },
        },
    }

    graph = {"schema_version": "1.0", "nodes": [u, d]}

    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("task.created", event="task.created", test_name="start_when_first_batch_starts_early", task_id=task_id)

    with log_context(task_id=task_id):
        doc_when_d_runs = await wait_node_running(inmemory_db, task_id, "d", timeout=6.0)
        st_u = _node_status(doc_when_d_runs, "u")
        tlog.debug(
            "firstbatch.start_observed",
            event="firstbatch.start_observed",
            upstream=str(st_u),
            downstream="running",
        )

        # Upstream must not yet be finished; otherwise early start is not verified.
        assert not str(st_u).endswith("finished"), "Upstream already finished, early start not verified"

        # Eventually everything must finish.
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        final = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
        tlog.debug("firstbatch.final", event="firstbatch.final", statuses=final)
        assert any(n["node_id"] == "u" and n["status"] == cd.RunState.finished for n in tdoc["graph"]["nodes"])
        assert any(n["node_id"] == "d" and n["status"] == cd.RunState.finished for n in tdoc["graph"]["nodes"])


@pytest.mark.asyncio
async def test_after_upstream_complete_delays_start(
    env_and_imports, inmemory_db, coord, worker_factory, monkeypatch, tlog
):
    """
    Without start_when, the downstream must not start until the upstream is fully finished.
    We first assert a negative hold window while the upstream is running, then assert the
    downstream starts and finishes after the upstream completes.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="after_upstream_complete_delays_start")

    # Slow down indexer batch processing: 2 batches x ~0.45s ~= 0.9s total
    monkeypatch.setenv("TEST_IDX_PROCESS_SLEEP_SEC", "0.45")

    # Start workers
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    # u: emits 10 SKUs in 2 batches of 5; d depends on u; no start_when
    u = _make_indexer("u", total=10, batch=5)
    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",  # wait for u to fully complete
        "io": {
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u"], "poll_ms": 30},
            }
        },
    }

    graph = {"schema_version": "1.0", "nodes": [u, d]}
    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("task.created", event="task.created", test_name="after_upstream_complete_delays_start", task_id=task_id)

    with log_context(task_id=task_id):
        # 1) While u is still running (~0.9s), d must NOT enter running (hold < u duration).
        await wait_node_not_running_for(inmemory_db, task_id, "d", hold=0.5)

        # 2) Wait until u finished.
        await wait_node_finished(inmemory_db, task_id, "u", timeout=3.0)

        # 3) Now d should start and quickly finish.
        await wait_node_finished(inmemory_db, task_id, "d", timeout=2.0)
        tlog.debug("delayed.start.ok", event="delayed.start.ok")
