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

from tests.helpers import dbg
from tests.helpers.graph import prime_graph, wait_task_finished
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
async def coord(env_and_imports, inmemory_db, coord_cfg):
    """
    Bring up a Coordinator using the shared in-memory DB and fast test config.
    """
    cd, _ = env_and_imports
    c = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
    await c.start()
    try:
        yield c
    finally:
        await c.stop()


@pytest_asyncio.fixture
async def workers_pipeline(env_and_imports, inmemory_db, worker_factory):
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
    # Nothing to yield; worker_factory auto-stops on teardown
    yield


@pytest_asyncio.fixture
async def workers(env_and_imports, inmemory_db, worker_factory):
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
        "edges": [
            ["w1", "w2"],
            ["w2", "w3"],
            ["w1", "w3"],
            ["w2", "w5"],
            ["w5", "w4"],
            ["w3", "w4"],
        ],
        "edges_ex": [
            {"from": "w1", "to": "w2", "mode": "async", "trigger": "on_batch"},
            {"from": "w2", "to": "w5", "mode": "async", "trigger": "on_batch"},
            {"from": "w5", "to": "w4", "mode": "async", "trigger": "on_batch"},
            {"from": "w3", "to": "w4", "mode": "async", "trigger": "on_batch"},
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
        "edges": [["w1", "w2"], ["w2", "w5"], ["w5", "w4"]],
        "edges_ex": [
            {"from": "w1", "to": "w2", "mode": "async", "trigger": "on_batch"},
            {"from": "w2", "to": "w5", "mode": "async", "trigger": "on_batch"},
            {"from": "w5", "to": "w4", "mode": "async", "trigger": "on_batch"},
        ],
    }


# ───────────────────────── Tests: extended graph ─────────────────────────


@pytest.mark.asyncio
async def test_e2e_streaming_with_kafka_chaos(env_and_imports, inmemory_db, coord, workers_pipeline):
    """
    With chaos enabled (broker/consumer jitter and message duplications, no drops),
    the full pipeline should still complete and produce artifacts at indexer/enricher/ocr stages.
    """
    cd, _ = env_and_imports

    graph = prime_graph(cd, build_graph(total_skus=18, batch_size=5, mini_batch=3))
    task_id = await coord.create_task(params={}, graph=graph)
    dbg("TEST.TASK_CREATED", task_id=task_id)

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=20.0)

    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("TEST.FINAL.STATUSES", statuses=st)

    # All nodes (including merge) should finish.
    assert st["w1"] == cd.RunState.finished
    assert st["w2"] == cd.RunState.finished
    assert st["w3"] == cd.RunState.finished
    assert st["w5"] == cd.RunState.finished
    assert st["w4"] == cd.RunState.finished

    # Streaming stages must leave artifacts behind.
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w1"}) > 0
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w2"}) > 0
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w5"}) > 0


# ───────────────────────── Tests: exact snippets you asked to include ─────────────────────────


@pytest.mark.asyncio
async def test_chaos_delays_and_duplications(env_and_imports, inmemory_db, coord, workers):
    """
    Chaos mode: small broker/consumer jitter + message duplications (no drops).
    Expect the pipeline to finish and produce artifacts for w1/w2/w5.
    """
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_stream())
    task_id = await coord.create_task(params={}, graph=g)

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=20.0)
    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    assert all(st[n] == cd.RunState.finished for n in ("w1", "w2", "w5", "w4"))

    # sanity: upstream/streaming nodes must leave artifacts behind
    for nid in ("w1", "w2", "w5"):
        cnt = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": nid})
        assert cnt > 0, f"no artifacts for {nid}"


@pytest.mark.asyncio
async def test_chaos_worker_restart_mid_stream(env_and_imports, inmemory_db, coord, workers):
    """
    Restart the 'enricher' worker in the middle of the stream.
    Expect the coordinator to fence and the pipeline to still finish.
    """
    cd, wu = env_and_imports
    g = prime_graph(cd, graph_stream())
    task_id = await coord.create_task(params={}, graph=g)

    async def restart_enricher():
        await asyncio.sleep(0.35)  # let it start
        # stop
        await workers["w2"].stop()
        # start a new instance of same role/handlers
        new_w2 = wu.Worker(db=inmemory_db, roles=["enricher"], handlers={"enricher": workers["handlers"]["enricher"]})
        await new_w2.start()
        workers["w2"] = new_w2  # ensure teardown stops the new one

    restart_enricher_task = asyncio.create_task(restart_enricher())
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=25.0)

    # Ensure the background restart finished
    try:
        await asyncio.wait_for(restart_enricher_task, timeout=5.0)
    except Exception:
        # best-effort only; test already validated completion
        pass

    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    assert all(st[n] == cd.RunState.finished for n in ("w1", "w2", "w5", "w4"))


@pytest.mark.asyncio
async def test_chaos_coordinator_restart(env_and_imports, inmemory_db, coord, workers):
    """
    Restart the Coordinator while the task is running.
    Expect the pipeline to recover and finish.
    """
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_stream())
    task_id = await coord.create_task(params={}, graph=g)

    async def restart_coord():
        await asyncio.sleep(0.3)
        await coord.stop()
        await coord.start()

    restart_coord_task = asyncio.create_task(restart_coord())
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=25.0)

    # Ensure the background restart finished
    try:
        await asyncio.wait_for(restart_coord_task, timeout=5.0)
    except Exception:
        # best-effort only; test already validated completion
        pass

    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    assert all(st[n] == cd.RunState.finished for n in ("w1", "w2", "w5", "w4"))
