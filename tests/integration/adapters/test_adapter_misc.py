from __future__ import annotations

import pytest
from tests.helpers.graph import (
    make_graph,
    node_by_id,
    prime_graph,
    wait_task_finished,
    wait_task_status,
)
from tests.helpers.handlers import build_indexer_handler

from flowkit.core.log import log_context
from flowkit.protocol.messages import RunState
from flowkit.worker.handlers.base import Batch, BatchResult, RoleHandler  # type: ignore

pytestmark = [pytest.mark.integration, pytest.mark.adapters, pytest.mark.worker_types("indexer,probe")]


# ───────────────────────── Handlers for tests ─────────────────────────


class ProbeHandlerCounts(RoleHandler):
    """
    Counts items in each batch. Used when the worker streams via an input adapter.
    """

    role = "probe"

    async def process_batch(self, batch: Batch, ctx):
        items = (batch.payload or {}).get("items") or (batch.payload or {}).get("skus") or []
        return BatchResult(success=True, metrics={"count": len(items)})


class RoleHandlerCounts(RoleHandler):
    """
    Minimal test helper: increments 'count' by number of items in each batch.
    Not used in production, only in tests.
    """

    role = "probe"

    async def process_batch(self, batch: Batch, ctx):
        items = (batch.payload or {}).get("items") or (batch.payload or {}).get("skus") or []
        return BatchResult(success=True, metrics={"count": len(items)})


class ProbeHandlerAdapterFromHandler(RoleHandlerCounts):
    """
    load_input proposes a valid adapter (used only when cmd doesn't specify one).
    """

    def __init__(self, *, from_nodes):
        self._from_nodes = list(from_nodes)

    async def load_input(self, ref, inline):
        return {
            "input_inline": {
                "input_adapter": "pull.from_artifacts.rechunk:size",
                "input_args": {"from_nodes": self._from_nodes, "poll_ms": 20, "size": 2, "meta_list_key": "skus"},
            }
        }


class ProbeHandlerIterOnly(RoleHandler):
    """
    No adapter suggested. Fallback to iter_batches should be used.
    Produces two synthetic batches to aggregate.
    """

    role = "probe"

    async def iter_batches(self, loaded):
        # Two batches: 3 + 2 items => expect count == 5
        yield Batch(batch_uid="b1", payload={"items": [1, 2, 3]})
        yield Batch(batch_uid="b2", payload={"items": [10, 20]})

    async def process_batch(self, batch: Batch, ctx):
        items = (batch.payload or {}).get("items") or []
        return BatchResult(success=True, metrics={"count": len(items)})


class ProbeHandlerGuardIterCalled(RoleHandler):
    """
    Used to assert iter_batches is NOT called when cmd adapter is unknown (task should fail early).
    """

    role = "probe"

    def __init__(self):
        self.iter_called = False

    async def iter_batches(self, loaded):
        self.iter_called = True
        yield Batch(batch_uid="should-not-happen", payload={"items": [1]})


# ───────────────────────── Tests ─────────────────────────


@pytest.mark.asyncio
async def test_handler_adapter_used_when_cmd_absent(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    When cmd does not specify an adapter, the worker may use the handler's adapter if it is known.
    """
    cd, _ = env_and_imports

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("probe", ProbeHandlerAdapterFromHandler(from_nodes=["u"])),
    )

    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 4, "total_skus": 8}},  # 4 + 4
        "status": None,
        "attempt_epoch": 0,
    }
    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            # No cmd.input_inline here → handler-provided adapter should be used
            "start_when": "first_batch"
        },
        "status": None,
        "attempt_epoch": 0,
    }

    g = prime_graph(cd, make_graph(nodes=[u, probe], edges=[("u", "probe")], agg={"after": "probe"}))
    tid = await coord.create_task(params={}, graph=g)

    with log_context(task_id=tid):
        tdoc = await wait_task_finished(inmemory_db, tid, timeout=5.0)
        assert node_by_id(tdoc, "u")["status"] == cd.RunState.finished
        assert node_by_id(tdoc, "probe")["status"] == cd.RunState.finished
        count = int((node_by_id(tdoc, "probe").get("stats") or {}).get("count") or 0)
        assert count == 8


@pytest.mark.asyncio
async def test_fallback_to_iter_batches_when_no_adapter(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    If neither cmd nor handler specifies an adapter, worker must fallback to handler.iter_batches.
    """
    cd, _ = env_and_imports

    await worker_factory(("probe", ProbeHandlerIterOnly()))

    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": [],
        "fan_in": "all",
        "io": {
            # No input_inline at all
        },
        "status": None,
        "attempt_epoch": 0,
    }

    g = prime_graph(cd, make_graph(nodes=[probe], edges=[], agg={"after": "probe"}))
    tid = await coord.create_task(params={}, graph=g)

    with log_context(task_id=tid):
        tdoc = await wait_task_finished(inmemory_db, tid, timeout=8.0)
        assert node_by_id(tdoc, "probe")["status"] == cd.RunState.finished
        # 3 + 2 = 5
        count = int((node_by_id(tdoc, "probe").get("stats") or {}).get("count") or 0)
        assert count == 5


@pytest.mark.asyncio
async def test_cmd_unknown_adapter_causes_permanent_fail(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    If cmd specifies an unknown adapter, the worker must fail the task permanently
    and must NOT call handler.iter_batches.
    """
    cd, _ = env_and_imports

    handler = ProbeHandlerGuardIterCalled()
    await worker_factory(("probe", handler))

    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": [],
        "fan_in": "all",
        "io": {
            "input_inline": {
                "input_adapter": "this.adapter.does.not.exist",
                "input_args": {"poll_ms": 10},
            }
        },
        "status": None,
        "attempt_epoch": 0,
    }

    g = prime_graph(cd, make_graph(nodes=[probe], edges=[]))
    tid = await coord.create_task(params={}, graph=g)

    # Task should become failed (permanent) quickly.
    await wait_task_status(inmemory_db, tid, RunState.failed.value, timeout=8.0)
    assert handler.iter_called is False


@pytest.mark.asyncio
async def test_cmd_from_node_alias_supported(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    cmd.input_inline may provide 'from_node' (single) instead of 'from_nodes' (list).
    """
    cd, _ = env_and_imports

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("probe", ProbeHandlerCounts()),
    )

    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 3, "total_skus": 9}},  # 3 + 3 + 3
        "status": None,
        "attempt_epoch": 0,
    }
    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts.rechunk:size",
                # Use 'from_node' instead of 'from_nodes'
                "input_args": {"from_node": "u", "poll_ms": 20, "size": 3, "meta_list_key": "skus"},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }

    g = prime_graph(cd, make_graph(nodes=[u, probe], edges=[("u", "probe")], agg={"after": "probe"}))
    tid = await coord.create_task(params={}, graph=g)

    with log_context(task_id=tid):
        tdoc = await wait_task_finished(inmemory_db, tid, timeout=5.0)
        assert node_by_id(tdoc, "u")["status"] == cd.RunState.finished
        assert node_by_id(tdoc, "probe")["status"] == cd.RunState.finished
        count = int((node_by_id(tdoc, "probe").get("stats") or {}).get("count") or 0)
        assert count == 9


@pytest.mark.asyncio
async def test_handler_unknown_adapter_ignored_when_cmd_valid(
    env_and_imports, inmemory_db, coord, worker_factory, tlog
):
    """
    If the handler proposes an unknown adapter but cmd provides a valid one,
    the worker must follow cmd and succeed.
    """
    cd, _ = env_and_imports

    class ProbeHandlerUnknownButCounts(ProbeHandlerCounts):
        async def load_input(self, ref, inline):
            # Propose an adapter name the worker doesn't know.
            return {
                "input_inline": {
                    "input_adapter": "some.unknown.adapter",
                    "input_args": {"from_nodes": ["u2"], "poll_ms": 10},
                }
            }

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("probe", ProbeHandlerUnknownButCounts()),
    )

    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 2, "total_skus": 6}},  # 2 + 2 + 2
        "status": None,
        "attempt_epoch": 0,
    }
    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts.rechunk:size",
                "input_args": {"from_nodes": ["u"], "poll_ms": 20, "size": 2, "meta_list_key": "skus"},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }

    g = prime_graph(cd, make_graph(nodes=[u, probe], edges=[("u", "probe")], agg={"after": "probe"}))
    tid = await coord.create_task(params={}, graph=g)

    with log_context(task_id=tid):
        tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)
        assert node_by_id(tdoc, "u")["status"] == cd.RunState.finished
        assert node_by_id(tdoc, "probe")["status"] == cd.RunState.finished
        count = int((node_by_id(tdoc, "probe").get("stats") or {}).get("count") or 0)
        assert count == 6
