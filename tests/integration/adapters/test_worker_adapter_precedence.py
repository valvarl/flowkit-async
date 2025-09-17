from __future__ import annotations

import pytest
from tests.helpers.graph import node_by_id, wait_task_finished
from tests.helpers.handlers import build_indexer_handler

from flowkit.core.log import log_context
from flowkit.protocol.messages import RunState
from flowkit.worker.handlers.base import Batch, BatchResult, RoleHandler  # type: ignore

pytestmark = [pytest.mark.integration, pytest.mark.adapters, pytest.mark.worker_types("indexer,probe")]


class ProbeHandlerConflicting(RoleHandler):
    """
    Deliberately returns an adapter pointing to 'u2' (which is empty) from load_input,
    while the graph (via cmd.input_inline) points to 'u' (which has data).
    Expectation: the worker must honor cmd.input_inline and ignore the handler's suggestion.
    """

    role = "probe"

    def __init__(self, *, db):
        self.db = db

    async def load_input(self, ref, inline):
        # Intentionally divert to the empty upstream 'u2'
        return {
            "input_inline": {
                "input_adapter": "pull.from_artifacts.rechunk:size",
                "input_args": {
                    "from_nodes": ["u2"],
                    "poll_ms": 15,
                    "size": 2,
                    "meta_list_key": "skus",
                },
            }
        }

    async def iter_batches(self, loaded):
        raise AssertionError("iter_batches must not be called when an input adapter is configured")

    async def process_batch(self, batch: Batch, ctx):
        items = (batch.payload or {}).get("items") or (batch.payload or {}).get("skus") or []
        return BatchResult(success=True, metrics={"count": len(items)})


@pytest.mark.asyncio
async def test_cmd_input_inline_overrides_handler_load_input(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    When configurations conflict, cmd.input_inline must take precedence over handler-provided load_input.
    """
    cd, _ = env_and_imports

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("probe", ProbeHandlerConflicting(db=inmemory_db)),
    )

    # u has data; u2 is empty
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 3, "total_skus": 6}},
    }
    u2 = {
        "node_id": "u2",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 1, "total_skus": 0}},
    }

    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": ["u", "u2"],
        "fan_in": "any",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts.rechunk:size",
                "input_args": {
                    "from_nodes": ["u"],  # ← correct source provided by the command
                    "poll_ms": 25,
                    "size": 2,
                    "meta_list_key": "skus",
                },
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

    graph = {"schema_version": "1.0", "nodes": [u, u2, probe, agg]}

    task_id = await coord.create_task(params={}, graph=graph)

    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

        st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
        assert st["u"] == RunState.finished
        assert st["u2"] == RunState.finished
        assert st["probe"] == RunState.finished

        # If cmd.input_inline (u) was honored, we must see items (count > 0).
        probe_node = node_by_id(tdoc, "probe")
        count = int((probe_node.get("stats") or {}).get("count") or 0)
        assert count > 0, "cmd.input_inline must override the handler-provided input adapter"
