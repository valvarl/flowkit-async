from __future__ import annotations

import pytest
from tests.helpers.graph import wait_task_status
from tests.helpers.handlers import build_indexer_handler

from flowkit.protocol.messages import RunState
from flowkit.worker.handlers.base import Batch, BatchResult, RoleHandler  # type: ignore

pytestmark = [pytest.mark.integration, pytest.mark.adapters, pytest.mark.worker_types("indexer,probe")]


class ProbeCounts(RoleHandler):
    role = "probe"

    async def process_batch(self, batch: Batch, ctx):
        items = (batch.payload or {}).get("items") or []
        return BatchResult(success=True, metrics={"count": len(items)})


def _make_nodes_missing_size():
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 3, "total_skus": 6}},
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
                "input_args": {"from_nodes": ["u"], "poll_ms": 10},  # ← size missing
            },
        },
    }
    return u, probe


@pytest.mark.asyncio
async def test_rechunk_missing_size_fails_early(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    cd, _ = env_and_imports
    await worker_factory(("indexer", build_indexer_handler(db=inmemory_db)), ("probe", ProbeCounts()))
    u, probe = _make_nodes_missing_size()
    g = {"schema_version": "1.0", "nodes": [u, probe]}
    tid = await coord.create_task(params={}, graph=g)
    await wait_task_status(inmemory_db, tid, RunState.failed.value, timeout=6.0)


@pytest.mark.asyncio
async def test_rechunk_invalid_size_fails_early(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    cd, _ = env_and_imports
    await worker_factory(("indexer", build_indexer_handler(db=inmemory_db)), ("probe", ProbeCounts()))
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 3, "total_skus": 6}},
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
                "input_args": {"from_nodes": ["u"], "poll_ms": 10, "size": 0},  # ← invalid
            },
        },
    }
    g = {"schema_version": "1.0", "nodes": [u, probe]}
    tid = await coord.create_task(params={}, graph=g)
    await wait_task_status(inmemory_db, tid, RunState.failed.value, timeout=6.0)


@pytest.mark.asyncio
async def test_rechunk_invalid_meta_list_key_type_fails(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    cd, _ = env_and_imports
    await worker_factory(("indexer", build_indexer_handler(db=inmemory_db)), ("probe", ProbeCounts()))
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 2, "total_skus": 4}},
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
                "input_args": {"from_nodes": ["u"], "poll_ms": 10, "size": 2, "meta_list_key": 123},  # ← wrong type
            },
        },
    }
    g = {"schema_version": "1.0", "nodes": [u, probe]}
    tid = await coord.create_task(params={}, graph=g)
    await wait_task_status(inmemory_db, tid, RunState.failed.value, timeout=6.0)
