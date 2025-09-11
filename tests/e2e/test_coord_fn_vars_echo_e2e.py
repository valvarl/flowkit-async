"""
E2E (slow): scheduler-driven coordinator_fn → coordinator_fn chain.

Graph:
  n1: coordinator_fn (vars.set)     → routing.sla="gold", limits.max_batches=5
  n2: coordinator_fn (vars.echo)    → reads coordinator.vars and writes an artifact echo

Checks:
  - scheduler starts nodes automatically (no manual _run_coordinator_fn)
  - both nodes finish
  - echo artifact exists for n2 and contains an exact copy of coordinator.vars
  - updated_at present (and likely advanced)
"""

from __future__ import annotations

import pytest

from flowkit.coordinator.runner import Coordinator
from flowkit.core.time import ManualClock
from flowkit.protocol.messages import RunState


pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.vars]


# ───────────────────────── helpers ─────────────────────────

async def _wait_task_finished(db, task_id: str, *, timeout_s: float = 10.0):
    """Minimal waiter to reuse without pulling extra helpers."""
    import time
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        doc = await db.tasks.find_one({"id": task_id}, {"status": 1, "graph": 1, "coordinator": 1, "updated_at": 1})
        if doc and all(n.get("status") == RunState.finished for n in (doc.get("graph", {}).get("nodes") or [])):
            return doc
        await db.clock.sleep_ms(20) if hasattr(db, "clock") else None  # best-effort
    raise AssertionError("timeout waiting for task to finish")


def _build_graph() -> dict:
    return {
        "nodes": [
            {
                "node_id": "n1",
                "type": "coordinator_fn",
                "status": RunState.queued,
                "io": {"fn": "vars.set", "fn_args": {"kv": {"routing.sla": "gold", "limits.max_batches": 5}}},
            },
            {
                "node_id": "n2",
                "type": "coordinator_fn",
                "status": RunState.queued,
                # `target.node_id` будет подставлен координатором автоматически
                "io": {"fn": "vars.echo", "fn_args": {"target": {}}},
                "depends_on": ["n1"],
            },
        ],
        "edges": [("n1", "n2")],
        "edges_ex": [],
    }


# ───────────────────────── test ─────────────────────────

@pytest.mark.asyncio
async def test_scheduler_runs_coordfn_chain_and_child_sees_vars(env_and_imports, inmemory_db, tlog):
    """
    Full pipeline with scheduler; second coordinator_fn echoes coordinator.vars to artifacts.
    """
    # 1) создаём координатор со своим адаптером vars.echo
    clock = ManualClock(start_ms=0)
    coord = Coordinator(db=inmemory_db, clock=clock)

    async def vars_echo_adapter(task_id: str, target: dict | None = None, **_):
        """
        Записывает артефакт со снимком task.coordinator.vars в meta.
        """
        tdoc = await inmemory_db.tasks.find_one({"id": task_id}, {"coordinator": 1})
        vars_copy = ((tdoc or {}).get("coordinator") or {}).get("vars", {})
        node_id = (target or {}).get("node_id") or "coordinator"
        await inmemory_db.artifacts.update_one(
            {"task_id": task_id, "node_id": node_id},
            {
                "$set": {
                    "status": "complete",
                    "meta": {"vars": vars_copy},
                    "updated_at": coord.clock.now_dt(),
                },
                "$setOnInsert": {
                    "task_id": task_id,
                    "node_id": node_id,
                    "attempt_epoch": 0,
                    "created_at": coord.clock.now_dt(),
                },
            },
            upsert=True,
        )
        return {"ok": True, "node_id": node_id, "size": len(vars_copy)}

    # регистрируем тестовый адаптер (оставляя дефолтные)
    coord.adapters["vars.echo"] = vars_echo_adapter

    # 2) стартуем координатор (шедулер, лупы и т.д.)
    await coord.start()

    try:
        graph = _build_graph()
        task_id = await coord.create_task(params={}, graph=graph)

        # 3) ждём завершения таска (обе ноды должны финишировать самостоятельно)
        final_doc = await _wait_task_finished(inmemory_db, task_id, timeout_s=8.0)

        # 4) статусы нод
        statuses = {n["node_id"]: n["status"] for n in final_doc["graph"]["nodes"]}
        assert statuses["n1"] == RunState.finished
        assert statuses["n2"] == RunState.finished

        # 5) проверяем артефакт-эхо от n2
        art = await inmemory_db.artifacts.find_one({"task_id": task_id, "node_id": "n2"})
        assert art is not None, "echo artifact must exist for n2"
        meta_vars = (art.get("meta") or {}).get("vars") or {}

        # ожидаемые значения, установленные n1
        assert meta_vars.get("routing", {}).get("sla") == "gold"
        assert meta_vars.get("limits", {}).get("max_batches") == 5

        # 6) updated_at присутствует (и логично после стартового момента)
        assert final_doc.get("updated_at") is not None
    finally:
        await coord.stop()
