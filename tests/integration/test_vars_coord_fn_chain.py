import pytest

from flowkit.coordinator.runner import Coordinator
from flowkit.core.time import ManualClock
from flowkit.protocol.messages import RunState


pytestmark = [pytest.mark.integration, pytest.mark.vars]


async def _updated_at(db, tid: str):
    doc = await db.tasks.find_one({"id": tid}, {"updated_at": 1})
    return (doc or {}).get("updated_at")


@pytest.mark.asyncio
async def test_coord_fn_chain_set_merge_incr_unset(inmemory_db):
    """
    Интеграционный сценарий:
      n1: vars.set   → routing.sla="gold", limits.max_batches=5
      n2: vars.merge → flags={"ab": False, "beta": True}
      n3: vars.incr  → counters.pages += 3
      n4: vars.unset → limits.max_batches

    Проверяем:
      - каждая нода завершилась (status=finished)
      - итоговый coordinator.vars соответствует ожиданию
      - updated_at меняется между шагами (если поле трекается inmemory_db)
    """
    clock = ManualClock(start_ms=0)
    coord = Coordinator(db=inmemory_db, clock=clock)

    g = {
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
                "io": {"fn": "vars.merge", "fn_args": {"data": {"flags": {"ab": False, "beta": True}}}},
            },
            {
                "node_id": "n3",
                "type": "coordinator_fn",
                "status": RunState.queued,
                "io": {"fn": "vars.incr", "fn_args": {"key": "counters.pages", "by": 3}},
            },
            {
                "node_id": "n4",
                "type": "coordinator_fn",
                "status": RunState.queued,
                # NOTE: реализация адаптера vars.unset должна принимать keys=['...'] через kwargs;
                # если у тебя сигнатура *keys, добавь поддержку kwargs внутри адаптера.
                "io": {"fn": "vars.unset", "fn_args": {"keys": ["limits.max_batches"]}},
            },
        ],
        "edges": [("n1", "n2"), ("n2", "n3"), ("n3", "n4")],
        "edges_ex": [],
    }

    tid = await coord.create_task(params={}, graph=g)

    # helper: найти ноду по id из свежего документа
    async def _node(nid: str):
        doc = await inmemory_db.tasks.find_one({"id": tid}, {"graph": 1})
        return next(n for n in (doc["graph"]["nodes"] or []) if n["node_id"] == nid)

    updated = []

    # n1
    node = await _node("n1")
    await coord._run_coordinator_fn({"id": tid, "graph": g}, node)
    updated.append(await _updated_at(inmemory_db, tid))
    await clock.sleep_ms(1)

    # n2
    node = await _node("n2")
    await coord._run_coordinator_fn({"id": tid, "graph": g}, node)
    updated.append(await _updated_at(inmemory_db, tid))
    await clock.sleep_ms(1)

    # n3
    node = await _node("n3")
    await coord._run_coordinator_fn({"id": tid, "graph": g}, node)
    updated.append(await _updated_at(inmemory_db, tid))
    await clock.sleep_ms(1)

    # n4
    node = await _node("n4")
    await coord._run_coordinator_fn({"id": tid, "graph": g}, node)
    updated.append(await _updated_at(inmemory_db, tid))

    # Проверка статусов нод
    final_doc = await inmemory_db.tasks.find_one({"id": tid}, {"graph": 1, "coordinator": 1})
    assert all(n["status"] == RunState.finished for n in final_doc["graph"]["nodes"])

    # Итоговые значения coordinator.vars
    vars_ = (final_doc.get("coordinator") or {}).get("vars") or {}
    assert vars_.get("routing", {}).get("sla") == "gold"
    assert vars_.get("flags", {}).get("ab") is False
    assert vars_.get("flags", {}).get("beta") is True
    assert vars_.get("counters", {}).get("pages") == 3
    assert "max_batches" not in (vars_.get("limits") or {})

    # updated_at прогрессирует (если трекается inmemory_db)
    # Отфильтруем None и проверим монотонный рост.
    ups = [u for u in updated if u is not None]
    if len(ups) >= 2:
        for prev, cur in zip(ups, ups[1:]):
            assert cur > prev, f"updated_at должен расти: {prev!r} → {cur!r}"
