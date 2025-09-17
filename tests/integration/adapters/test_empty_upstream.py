from __future__ import annotations

import pytest
from tests.helpers.graph import node_by_id, wait_task_finished
from tests.helpers.handlers import build_indexer_handler

from flowkit.worker.handlers.base import Batch, BatchResult, RoleHandler  # type: ignore

pytestmark = [pytest.mark.integration, pytest.mark.adapters, pytest.mark.worker_types("indexer,probe")]


class ProbeCounts(RoleHandler):
    role = "probe"

    async def process_batch(self, batch: Batch, ctx):
        items = (batch.payload or {}).get("items") or []
        return BatchResult(success=True, metrics={"count": len(items)})


@pytest.mark.asyncio
async def test_empty_upstream_finishes_with_zero_count(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    cd, _ = env_and_imports
    await worker_factory(("indexer", build_indexer_handler(db=inmemory_db)), ("probe", ProbeCounts()))
    # Upstream produces zero data (total_skus=0)
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 3, "total_skus": 0}},
    }
    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u"], "poll_ms": 15},
            },
        },
    }
    agg = {
        "node_id": "agg_probe",
        "type": "coordinator_fn",
        "depends_on": ["probe"],
        "fan_in": "all",
        "io": {"fn": "metrics.aggregate", "fn_args": {"node_id": "probe", "mode": "sum"}},
    }
    g = {"schema_version": "1.0", "nodes": [u, probe, agg]}
    tid = await coord.create_task(params={}, graph=g)
    tdoc = await wait_task_finished(inmemory_db, tid, timeout=8.0)
    assert node_by_id(tdoc, "u")["status"] == cd.RunState.finished
    assert node_by_id(tdoc, "probe")["status"] == cd.RunState.finished
    count = int((node_by_id(tdoc, "probe").get("stats") or {}).get("count") or 0)
    assert count == 0
