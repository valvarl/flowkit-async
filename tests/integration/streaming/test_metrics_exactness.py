"""Exactness and accounting of aggregated metrics."""

from __future__ import annotations

import pytest
from tests.helpers.graph import wait_task_finished
from tests.helpers.handlers import build_analyzer_handler, build_indexer_handler

from ._helpers import _get_count, _make_indexer

pytestmark = [pytest.mark.streaming, pytest.mark.worker_types("indexer,enricher,ocr,analyzer")]


@pytest.mark.asyncio
async def test_metrics_single_stream_exact_count(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Single upstream -> single downstream: analyzer's aggregated 'count' must equal the total items.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="metrics_single_stream_exact_count")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    total, batch = 13, 5  # 5 + 5 + 3
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
    graph = {"schema_version": "1.0", "nodes": [u, d]}

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    tlog.debug("metrics.single", event="metrics.single", expect=total, got=got)
    assert got == total


@pytest.mark.asyncio
async def test_metrics_multistream_exact_sum(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Three upstreams -> one downstream: analyzer's aggregated 'count' must equal the sum of all totals.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="metrics_multistream_exact_sum")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

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
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u1", "u2", "u3"], "poll_ms": 20},
            },
        },
    }
    graph = {"schema_version": "1.0", "nodes": [u1, u2, u3, d]}

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)

    expect = sum(totals.values())
    got = _get_count(tdoc, "d")
    tlog.debug("metrics.multi", event="metrics.multi", expect=expect, got=got)
    assert got == expect


@pytest.mark.asyncio
async def test_metrics_partial_batches_exact_count(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    With a remainder in the last upstream batch, analyzer's 'count' must still exactly equal total.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="metrics_partial_batches_exact_count")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    total, batch = 10, 3  # 3 + 3 + 3 + 1
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
    graph = {"schema_version": "1.0", "nodes": [u, d]}

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    tlog.debug("metrics.partial", event="metrics.partial", expect=total, got=got)
    assert got == total
