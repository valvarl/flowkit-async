from __future__ import annotations

import pytest
from tests.helpers.graph import node_by_id, prime_graph, wait_task_finished
from tests.helpers.handlers import build_indexer_handler

from flowkit.core.log import log_context
from flowkit.protocol.messages import RunState
from flowkit.worker.handlers.base import Batch, BatchResult, RoleHandler  # type: ignore

# Limit worker types for this module (include custom 'probe' role).
pytestmark = [pytest.mark.integration, pytest.mark.vars, pytest.mark.worker_types("indexer,probe")]


class ProbeHandler(RoleHandler):
    """
    Custom role used to assert that cmd.input_inline takes precedence over
    handler.load_input when selecting worker input adapter.

    load_input intentionally returns an adapter pointing to an upstream with no items (u2),
    while the node's cmd.input_inline targets 'u' which has data. The test asserts that
    the worker obeys the command-specified adapter (consumes 'u' → count > 0).
    """

    role = "probe"

    def __init__(self, *, db):
        self.db = db

    async def load_input(self, ref, inline):
        # Intentional conflict: point to 'u2' (no items).
        return {
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u2"], "poll_ms": 15, "size": 2, "meta_list_key": "skus"},
            }
        }

    async def iter_batches(self, loaded):
        # Should not be used if an input adapter is configured (adapter path must win).
        raise AssertionError("iter_batches must not be called when adapter is specified")

    async def process_batch(self, batch: Batch, ctx):
        items = (batch.payload or {}).get("items") or (batch.payload or {}).get("skus") or []
        return BatchResult(success=True, metrics={"count": len(items)})


@pytest.mark.asyncio
async def test_cmd_input_inline_overrides_handler_load_input(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Given conflicting adapter configs (handler vs cmd.input_inline),
    the worker must honor the command's input_inline.
    """
    cd, _ = env_and_imports

    # Start workers: indexer + probe
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("probe", ProbeHandler(db=inmemory_db)),
    )

    # Upstreams: u (with items), u2 (no items)
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 3, "total_skus": 6}},  # 2 batches x 3
        "status": None,
        "attempt_epoch": 0,
    }
    u2 = {
        "node_id": "u2",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 1, "total_skus": 0}},  # produces no batches
        "status": None,
        "attempt_epoch": 0,
    }

    # Downstream probe: cmd.input_inline points to 'u' (with items).
    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": ["u", "u2"],
        "fan_in": "any",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u"], "poll_ms": 20, "size": 2, "meta_list_key": "skus"},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {"schema_version": "1.0", "nodes": [u, u2, probe], "edges": [["u", "probe"], ["u2", "probe"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)

    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        statuses = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
        assert statuses["u"] == RunState.finished
        assert statuses["u2"] == RunState.finished
        assert statuses["probe"] == RunState.finished

        # The probe must have consumed items from 'u' (count > 0).
        probe_node = node_by_id(tdoc, "probe")
        count = int((probe_node.get("stats") or {}).get("count") or 0)
        assert count > 0, "probe must use cmd.input_inline adapter (from 'u') and see items"
