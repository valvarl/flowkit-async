"""Idempotency & event ordering for aggregated metrics."""

from __future__ import annotations

import asyncio
import inspect

import pytest
from tests.helpers import BROKER, AIOKafkaProducerMock
from tests.helpers.graph import wait_task_finished
from tests.helpers.handlers import build_analyzer_handler, build_indexer_handler

from flowkit.core.log import log_context

from ._helpers import _get_count, _make_indexer

pytestmark = [pytest.mark.streaming, pytest.mark.worker_types("indexer,enricher,ocr,analyzer")]


@pytest.mark.asyncio
async def test_metrics_idempotent_on_duplicate_status_events(
    env_and_imports, inmemory_db, coord, worker_factory, monkeypatch, tlog
):
    """
    Duplicate STATUS events (BATCH_OK / TASK_DONE) must not double-increment aggregated metrics.

    We monkeypatch AIOKafkaProducerMock.send_and_wait to produce the same STATUS event twice
    for status topics. The Coordinator should deduplicate by envelope key and keep metrics stable.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="metrics_idempotent_on_duplicate_status_events")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    # Save the original method to call once, then produce duplicate via broker.
    orig_send = AIOKafkaProducerMock.send_and_wait

    async def dup_status(self, topic, value, key=None):
        # First, the normal send.
        await orig_send(self, topic, value, key)
        # Then, if it's a STATUS event, produce a duplicate of the same envelope.
        if topic.startswith("status.") and (value or {}).get("msg_type") == "event":
            kind = (value.get("payload") or {}).get("kind") or ""
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
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u"], "poll_ms": 25},
            },
        },
    }
    graph = {"schema_version": "1.0", "nodes": [u, d]}

    task_id = await coord.create_task(params={}, graph=graph)
    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        got = _get_count(tdoc, "d")
        tlog.debug("metrics.dedup", event="metrics.dedup", expect=total, got=got)
        assert got == total


@pytest.mark.asyncio
async def test_status_out_of_order_does_not_break_aggregation(
    env_and_imports, inmemory_db, coord, worker_factory, monkeypatch, tlog
):
    """
    BATCH_OK STATUS events arrive out of order → the aggregated metric remains correct.
    We hold the first BATCH_OK so later ones arrive first.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="status_out_of_order_does_not_break_aggregation")

    # Slightly slow batches so TASK_DONE arrives after the delayed BATCH_OK.
    monkeypatch.setenv("TEST_IDX_PROCESS_SLEEP_SEC", "0.2")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    # Intercept producer: hold the first BATCH_OK and re-inject it via the broker later.
    orig_send = AIOKafkaProducerMock.send_and_wait
    state = {"held_once": False}

    async def out_of_order(self, topic, value, key=None):
        payload = (value or {}).get("payload") or {}
        if (
            topic.startswith("status.")
            and (value or {}).get("msg_type") == "event"
            and payload.get("kind") == "BATCH_OK"
            and not state["held_once"]
        ):
            state["held_once"] = True

            async def later():
                await asyncio.sleep(0.15)
                await BROKER.produce(topic, value)

            state["delayed_task"] = asyncio.create_task(later())
            state["delayed_task"].add_done_callback(lambda _t: state.pop("delayed_task", None))
            return  # suppress original send; delayed one will be re-injected via BROKER

        return await orig_send(self, topic, value, key)

    monkeypatch.setattr("tests.helpers.kafka.AIOKafkaProducerMock.send_and_wait", out_of_order, raising=True)

    total, batch = 12, 4  # 3 batches
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
    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        got = _get_count(tdoc, "d")
        tlog.debug("metrics.ooo", event="metrics.ooo", expect=total, got=got)
        assert got == total


@pytest.mark.asyncio
async def test_metrics_dedup_persists_across_coord_restart(
    env_and_imports, inmemory_db, coord, worker_factory, monkeypatch, tlog
):
    """
    Metric deduplication survives a coordinator restart: duplicates of STATUS
    (BATCH_OK/TASK_DONE) sent during the restart must not double-count.
    Skips if the fixture `coord` has no `restart` method`.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="metrics_dedup_persists_across_coord_restart")

    restart_fn = getattr(coord, "restart", None)
    if restart_fn is None:
        pytest.skip("Coordinator restart is not supported by this fixture")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    orig_send = AIOKafkaProducerMock.send_and_wait
    fired = {"restarted": False}

    async def dup_and_restart(self, topic, value, key=None):
        payload = (value or {}).get("payload") or {}
        # (1) On the first BATCH_OK, restart the coordinator.
        # (2) Always send the original.
        # (3) Inject a duplicate STATUS to verify dedup after restart.
        if (
            not fired["restarted"]
            and topic.startswith("status.")
            and (value or {}).get("msg_type") == "event"
            and payload.get("kind") == "BATCH_OK"
        ):
            fired["restarted"] = True
            if inspect.iscoroutinefunction(restart_fn):
                await restart_fn()  # soft restart
            else:
                restart_fn()

        await orig_send(self, topic, value, key)
        if topic.startswith("status.") and (value or {}).get("msg_type") == "event":
            kind = (payload or {}).get("kind") or ""
            if kind in ("BATCH_OK", "TASK_DONE"):
                await BROKER.produce(topic, value)

    monkeypatch.setattr("tests.helpers.kafka.AIOKafkaProducerMock.send_and_wait", dup_and_restart, raising=True)

    total, batch = 18, 6  # 3 batches
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
    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)
        got = _get_count(tdoc, "d")
        tlog.debug("metrics.dedup.restart", event="metrics.dedup.restart", expect=total, got=got)
        assert got == total
