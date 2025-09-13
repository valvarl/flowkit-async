from __future__ import annotations

import pytest
from tests.helpers.graph import make_graph, prime_graph, wait_task_finished, wait_task_status
from tests.helpers.handlers import build_indexer_handler

from flowkit.core.config import WorkerConfig
from flowkit.protocol.messages import RunState

pytestmark = [pytest.mark.integration, pytest.mark.adapters, pytest.mark.worker_types("indexer,probe")]


@pytest.mark.asyncio
async def test_rechunk_requires_meta_key_in_strict_mode(
    env_and_imports, inmemory_db, coord, worker_factory, monkeypatch, tlog
):
    """
    In strict mode, rechunk without meta_list_key must fail early.
    """
    cd, _ = env_and_imports

    # Patch WorkerConfig.load to enable strict mode for this test
    orig_load = WorkerConfig.load

    def strict_load(path: str = "configs/worker.default.json", overrides=None):
        cfg = orig_load(path, overrides=overrides)
        cfg.strict_input_adapters = True
        return cfg

    monkeypatch.setattr("flowkit.core.config.WorkerConfig.load", strict_load, raising=True)

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        (
            "probe",
            __import__("flowkit.worker.handlers.base", fromlist=["RoleHandler"]).worker.handlers.base.RoleHandler(),
        ),
    )
    # ↑ probe handler не важен, адаптер отвалится до стриминга; но worker_factory требует пару

    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 2, "total_skus": 4}},
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
                "input_args": {"from_nodes": ["u"], "poll_ms": 10, "size": 2},  # ← no meta_list_key
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }
    g = prime_graph(cd, make_graph(nodes=[u, probe], edges=[("u", "probe")]))
    tid = await coord.create_task(params={}, graph=g)
    await wait_task_status(inmemory_db, tid, RunState.failed.value, timeout=6.0)


@pytest.mark.asyncio
async def test_rechunk_with_meta_key_passes_in_strict_mode(
    env_and_imports, inmemory_db, coord, worker_factory, monkeypatch, tlog
):
    cd, _ = env_and_imports
    orig_load = WorkerConfig.load

    def strict_load(path: str = "configs/worker.default.json", overrides=None):
        cfg = orig_load(path, overrides=overrides)
        cfg.strict_input_adapters = True
        return cfg

    monkeypatch.setattr("flowkit.core.config.WorkerConfig.load", strict_load, raising=True)

    from flowkit.worker.handlers.base import Batch, BatchResult, RoleHandler

    class ProbeCounts(RoleHandler):
        role = "probe"

        async def process_batch(self, batch: Batch, ctx):
            items = (batch.payload or {}).get("items") or []
            return BatchResult(success=True, metrics={"count": len(items)})

    await worker_factory(("indexer", build_indexer_handler(db=inmemory_db)), ("probe", ProbeCounts()))
    u = {
        "node_id": "u",
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": 2, "total_skus": 6}},
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
                "input_args": {"from_nodes": ["u"], "poll_ms": 10, "size": 2, "meta_list_key": "skus"},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }
    g = prime_graph(cd, make_graph(nodes=[u, probe], edges=[("u", "probe")]))
    tid = await coord.create_task(params={}, graph=g)
    tdoc = await wait_task_finished(inmemory_db, tid, timeout=8.0)
    assert any(n["node_id"] == "probe" and str(n["status"]).endswith("finished") for n in tdoc["graph"]["nodes"])
