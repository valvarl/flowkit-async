# tests/test_chaos_models.py
r"""
End-to-end streaming smoke tests under chaos.

Pipelines covered:
  - w1(indexer) -> w2(enricher) -> w5(ocr) -> w4(analyzer)
                    \___________/           /
                         \_____ w3(merge) _/

Checks:
  - all nodes finish successfully (including the merge node in the extended graph)
  - artifacts are produced for indexer/enricher/ocr (streaming stages)
  - resiliency under worker and coordinator restarts
"""

from __future__ import annotations

import asyncio
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

# Limit roles and enable Kafka chaos for this module
pytestmark = [
    pytest.mark.worker_types("indexer,enricher,ocr,analyzer"),
    pytest.mark.chaos,
]

# ───────────────────────── Fixtures ─────────────────────────


@pytest_asyncio.fixture
async def coord(env_and_imports, inmemory_db, coord_cfg, tlog):
    """
    Bring up a Coordinator using the shared in-memory DB and fast test config.
    """
    cd, _ = env_and_imports
    c = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
    await c.start()
    tlog.debug("coord.started", event="coord.started")
    try:
        yield c
    finally:
        await c.stop()
        tlog.debug("coord.stopped", event="coord.stopped")


@pytest_asyncio.fixture
async def workers_pipeline(env_and_imports, inmemory_db, worker_factory, tlog):
    """
    Start four workers for roles: indexer, enricher, ocr, analyzer.
    Handlers come from tests.helpers.handlers and already receive `db`.
    """
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("enricher", build_enricher_handler(db=inmemory_db)),
        ("ocr", build_ocr_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )
    tlog.debug("workers.started", event="workers.started", roles=["indexer", "enricher", "ocr", "analyzer"])
    # Nothing to yield; worker_factory auto-stops on teardown
    yield


@pytest_asyncio.fixture
async def workers(env_and_imports, inmemory_db, worker_factory, tlog):
    """
    Start one worker per role and return a mapping by node name.
    We keep handler refs to support mid-test restarts.
    """
    handlers = {
        "indexer": build_indexer_handler(db=inmemory_db),
        "enricher": build_enricher_handler(db=inmemory_db),
        "ocr": build_ocr_handler(db=inmemory_db),
        "analyzer": build_analyzer_handler(db=inmemory_db),
    }
    w1, w2, w3, w4 = await worker_factory(
        ("indexer", handlers["indexer"]),
        ("enricher", handlers["enricher"]),
        ("ocr", handlers["ocr"]),
        ("analyzer", handlers["analyzer"]),
    )
    tlog.debug("workers.started", event="workers.started", roles=["indexer", "enricher", "ocr", "analyzer"])
    yield {"w1": w1, "w2": w2, "w3": w3, "w4": w4, "handlers": handlers}


# ───────────────────────── Graph helpers ─────────────────────────


def build_graph(*, total_skus=18, batch_size=5, mini_batch=3) -> dict[str, Any]:
    """Graph matching the real flow, with a coordinator_fn merge in the middle."""
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
                            "poll_ms": 30,
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
                "node_id": "w5",
                "type": "ocr",
                "depends_on": ["w2"],
                "fan_in": "any",
                "io": {
                    "start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts.rechunk:size",
                        "input_args": {"from_nodes": ["w2"], "size": 2, "poll_ms": 25, "meta_list_key": "enriched"},
                    },
                },
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
                        "input_args": {"from_nodes": ["w5", "w3"], "poll_ms": 25},
                    },
                },
            },
        ],
    }


def graph_stream() -> dict[str, Any]:
    """Streaming pipeline: indexer → enricher → ocr → analyzer."""
    return {
        "schema_version": "1.0",
        "nodes": [
            {
                "node_id": "w1",
                "type": "indexer",
                "depends_on": [],
                "fan_in": "all",
                "io": {"input_inline": {"batch_size": 5, "total_skus": 18}},
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
                        "input_args": {"from_nodes": ["w1"], "size": 3, "poll_ms": 30, "meta_list_key": "skus"},
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
                        "input_args": {"from_nodes": ["w2"], "size": 2, "poll_ms": 25, "meta_list_key": "enriched"},
                    },
                },
            },
            {
                "node_id": "w4",
                "type": "analyzer",
                "depends_on": ["w5"],
                "fan_in": "any",
                "io": {
                    "start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["w5"], "poll_ms": 25},
                    },
                },
            },
        ],
    }


# ───────────────────────── Tests: extended graph ─────────────────────────


@pytest.mark.asyncio
async def test_e2e_streaming_with_kafka_chaos(env_and_imports, inmemory_db, coord, workers_pipeline, tlog):
    """
    With chaos enabled (broker/consumer jitter and message duplications, no drops),
    the full pipeline should still complete and produce artifacts at indexer/enricher/ocr stages.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="e2e_streaming_with_kafka_chaos")

    graph = build_graph(total_skus=18, batch_size=5, mini_batch=3)
    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("task.created", event="task.created", task_id=task_id)

    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=20.0)

    st = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
    tlog.debug("pipeline.final_status", event="pipeline.final_status", statuses=st)

    # All nodes (including merge) should finish.
    assert tdoc and all(st[nid].endswith("finished") for nid in ("w1", "w2", "w3", "w5", "w4"))

    # Streaming stages must leave artifacts behind.
    for nid in ("w1", "w2", "w5"):
        cnt = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": nid})
        tlog.debug("artifacts.count", event="artifacts.count", node_id=nid, count=int(cnt))
        assert cnt > 0


# ───────────────────────── Tests: exact snippets you asked to include ─────────────────────────


@pytest.mark.asyncio
async def test_chaos_delays_and_duplications(env_and_imports, inmemory_db, coord, workers, tlog):
    """
    Chaos mode: small broker/consumer jitter + message duplications (no drops).
    Expect the pipeline to finish and produce artifacts for w1/w2/w5.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="chaos_delays_and_duplications")

    g = graph_stream()
    task_id = await coord.create_task(params={}, graph=g)
    tlog.debug("task.created", event="task.created", task_id=task_id)

    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=20.0)

    st = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
    tlog.debug("pipeline.final_status", event="pipeline.final_status", statuses=st)
    assert all(st[n].endswith("finished") for n in ("w1", "w2", "w5", "w4"))

    # sanity: upstream/streaming nodes must leave artifacts behind
    for nid in ("w1", "w2", "w5"):
        cnt = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": nid})
        tlog.debug("artifacts.count", event="artifacts.count", node_id=nid, count=int(cnt))
        assert cnt > 0, f"no artifacts for {nid}"


@pytest.mark.asyncio
async def test_chaos_worker_restart_mid_stream(env_and_imports, inmemory_db, coord, workers, tlog):
    """
    Restart the 'enricher' worker in the middle of the stream.
    Expect the coordinator to fence and the pipeline to still finish.
    """
    cd, wu = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="chaos_worker_restart_mid_stream")

    g = graph_stream()
    task_id = await coord.create_task(params={}, graph=g)
    tlog.debug("task.created", event="task.created", task_id=task_id)

    async def restart_enricher():
        await asyncio.sleep(0.35)  # let it start
        tlog.debug("worker.restart.begin", event="worker.restart.begin", role="enricher")
        # stop
        await workers["w2"].stop()
        # start a new instance of same role/handlers
        new_w2 = wu.Worker(db=inmemory_db, roles=["enricher"], handlers={"enricher": workers["handlers"]["enricher"]})
        await new_w2.start()
        workers["w2"] = new_w2  # ensure teardown stops the new one
        tlog.debug("worker.restart.end", event="worker.restart.end", role="enricher")

    restart_enricher_task = asyncio.create_task(restart_enricher())

    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=25.0)

    # Ensure the background restart finished
    try:
        await asyncio.wait_for(restart_enricher_task, timeout=5.0)
    except Exception as e:
        tlog.warning("worker.restart.bg_timeout", event="worker.restart.bg_timeout", err=str(e))

    st = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
    tlog.debug("pipeline.final_status", event="pipeline.final_status", statuses=st)
    assert all(st[n].endswith("finished") for n in ("w1", "w2", "w5", "w4"))


@pytest.mark.asyncio
async def test_chaos_coordinator_restart(env_and_imports, inmemory_db, coord, workers, tlog):
    """
    Restart the Coordinator while the task is running.
    Expect the pipeline to recover and finish.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="chaos_coordinator_restart")

    g = graph_stream()
    task_id = await coord.create_task(params={}, graph=g)
    tlog.debug("task.created", event="task.created", task_id=task_id)

    async def restart_coord():
        await asyncio.sleep(0.3)
        tlog.debug("coord.restart.begin", event="coord.restart.begin")
        await coord.stop()
        await coord.start()
        tlog.debug("coord.restart.end", event="coord.restart.end")

    restart_coord_task = asyncio.create_task(restart_coord())

    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=25.0)

    # Ensure the background restart finished
    try:
        await asyncio.wait_for(restart_coord_task, timeout=5.0)
    except Exception as e:
        tlog.warning("coord.restart.bg_timeout", event="coord.restart.bg_timeout", err=str(e))

    st = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
    tlog.debug("pipeline.final_status", event="pipeline.final_status", statuses=st)
    assert all(st[n].endswith("finished") for n in ("w1", "w2", "w5", "w4"))
