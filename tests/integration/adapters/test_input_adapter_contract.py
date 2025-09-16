"""
Integration tests for the orchestrator-first input adapter contract.

Covers:
  - Unknown adapter in cmd → permanent TASK_FAILED (bad_input_adapter).
  - Missing required adapter args in cmd → permanent TASK_FAILED (bad_input_args).
  - Empty upstream is not an error (finished with count=0).
  - Rechunk without meta_list_key treats each artifact as a single logical item (no heuristics).
"""

from __future__ import annotations

import pytest
from tests.helpers.graph import node_by_id, wait_task_finished, wait_task_status
from tests.helpers.handlers import build_indexer_handler

from flowkit.core.log import log_context
from flowkit.protocol.messages import RunState
from flowkit.worker.handlers.base import Batch, BatchResult, RoleHandler  # type: ignore

pytestmark = [pytest.mark.integration, pytest.mark.adapters, pytest.mark.worker_types("indexer,probe")]


class ProbeCounts(RoleHandler):
    """Counts len(items) or len(skus) in each batch."""

    role = "probe"

    async def process_batch(self, batch: Batch, ctx):
        p = batch.payload or {}
        items = p.get("items") or p.get("skus") or []
        return BatchResult(success=True, metrics={"count": len(items)})


class ProbeGuardNoIter(RoleHandler):
    """Asserts iter_batches must NOT be called under adapter streaming / early failures."""

    role = "probe"

    def __init__(self):
        self.iter_called = False

    async def iter_batches(self, loaded):
        self.iter_called = True
        yield Batch(batch_uid="must-not-be-called", payload={"items": [1]})


@pytest.mark.asyncio
async def test_missing_required_args_yields_bad_input_args(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    cmd.input_inline with 'pull.from_artifacts.rechunk:size' must include 'size'.
    Omission → early permanent failure (bad_input_args). Handler.iter_batches() must NOT be called.
    """
    cd, _ = env_and_imports
    guard = ProbeGuardNoIter()
    await worker_factory(("probe", guard))

    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": [],
        "fan_in": "all",
        "io": {
            "input_inline": {
                "input_adapter": "pull.from_artifacts.rechunk:size",
                # 'size' is intentionally missing here
                "input_args": {"from_nodes": ["u"], "poll_ms": 10},
            }
        },
    }
    g = {"schema_version": "1.0", "nodes": [probe]}
    tid = await coord.create_task(params={}, graph=g)
    await wait_task_status(inmemory_db, tid, RunState.failed.value, timeout=6.0)
    assert guard.iter_called is False


@pytest.mark.asyncio
async def test_empty_upstream_is_not_error(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Valid adapter + empty upstream → normal finish (count=0), not a failure.
    """
    cd, _ = env_and_imports
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("probe", ProbeCounts()),
    )

    # Upstream with zero items
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 5, "total_skus": 0}},
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
                "input_args": {"from_nodes": ["u"], "poll_ms": 20, "size": 3, "meta_list_key": "skus"},
            },
        },
    }
    g = {"schema_version": "1.0", "nodes": [u, probe]}
    tid = await coord.create_task(params={}, graph=g)

    with log_context(task_id=tid):
        tdoc = await wait_task_finished(inmemory_db, tid, timeout=6.0)
        assert node_by_id(tdoc, "u")["status"] == cd.RunState.finished
        assert node_by_id(tdoc, "probe")["status"] == cd.RunState.finished
        count = int((node_by_id(tdoc, "probe").get("stats") or {}).get("count") or 0)
        assert count == 0


@pytest.mark.asyncio
async def test_rechunk_without_meta_list_key_treats_each_artifact_as_single_item(
    env_and_imports, inmemory_db, coord, worker_factory, tlog
):
    """
    No meta_list_key → each upstream artifact's meta is a single logical item (no heuristics).
    If upstream emits 2 partial artifacts, probe should aggregate 'count' == 2 (not N of inner lists).
    """
    cd, _ = env_and_imports
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("probe", ProbeCounts()),
    )

    # Upstream emits 8 items in 2 partial artifacts: [4] + [4]
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 4, "total_skus": 8}},
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
                # NOTE: meta_list_key intentionally omitted to assert singleton-per-artifact behavior.
                "input_args": {"from_nodes": ["u"], "poll_ms": 20, "size": 10},
            },
        },
    }

    g = {"schema_version": "1.0", "nodes": [u, probe]}
    tid = await coord.create_task(params={}, graph=g)

    with log_context(task_id=tid):
        tdoc = await wait_task_finished(inmemory_db, tid, timeout=8.0)
        assert node_by_id(tdoc, "u")["status"] == cd.RunState.finished
        assert node_by_id(tdoc, "probe")["status"] == cd.RunState.finished
        # Expect 2 (two partial artifacts → two logical items), NOT 8.
        count = int((node_by_id(tdoc, "probe").get("stats") or {}).get("count") or 0)
        assert count == 2
