import pytest

from flowkit.coordinator.adapters import CoordinatorAdapters, AdapterError
from flowkit.core.time import ManualClock


pytestmark = [pytest.mark.unit, pytest.mark.vars]


async def _mk_task(db, tid="t_vars_unset"):
    await db.tasks.insert_one(
        {"id": tid, "pipeline_id": tid, "status": "queued", "graph": {"nodes": [], "edges": [], "edges_ex": []}}
    )
    return tid


@pytest.mark.asyncio
async def test_vars_unset_simple_nested_and_noop(inmemory_db):
    """
    Удаляем простой лист и вложенную ветку; отсутствующий ключ — no-op (без ошибок).
    """
    tid = await _mk_task(inmemory_db)
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    # Подготовим состояние.
    await impl.vars_set(
        tid,
        kv={
            "limits.max": 5,
            "flags.beta": True,
            "nested": {"lvl1": {"lvl2": 123}},
        },
    )

    # 1) простой лист
    res1 = await impl.vars_unset(tid, "limits.max")
    assert res1["ok"] is True and res1["touched"] == 1
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v = doc["coordinator"]["vars"]
    assert "max" not in (v.get("limits") or {}), "лист удалён"
    assert v["flags"]["beta"] is True, "соседние поля не тронули"

    # 2) вложенная ветка
    res2 = await impl.vars_unset(tid, "nested.lvl1")
    assert res2["ok"] is True and res2["touched"] == 1
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v = doc["coordinator"]["vars"]
    assert "lvl1" not in (v.get("nested") or {}), "ветка удалена целиком"

    # 3) отсутствующий ключ — no-op (док неизменён, но операция валидна)
    res3 = await impl.vars_unset(tid, "missing.key")
    assert res3["ok"] is True and res3["touched"] == 1  # по текущей реализации — сколько путей запросили
    doc2 = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc2 == doc, "состояние не изменилось"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_key",
    [
        "",             # пустой ключ
        "$bad",         # сегмент начинается с $
        "a..b",         # пустой сегмент
        "bad\0seg",     # NUL в сегменте
        "a." + ("s" * 129),  # длина сегмента > лимита
    ],
)
async def test_vars_unset_validates_paths(inmemory_db, bad_key):
    """
    Валидация путей в unset: $, '.', NUL, длина сегмента.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_unset_validate")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    with pytest.raises(AdapterError):
        await impl.vars_unset(tid, bad_key)


@pytest.mark.asyncio
async def test_vars_unset_path_length_limit(inmemory_db):
    """
    Валидация длины полного пути (с учётом префикса 'coordinator.vars.').
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_unset_longpath")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    path_limit = impl._limits["max_path_len"]
    prefix_len = len("coordinator.vars.")
    too_long_key = "k" * (path_limit - prefix_len + 1)
    with pytest.raises(AdapterError):
        await impl.vars_unset(tid, too_long_key)
