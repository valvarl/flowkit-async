import pytest

from flowkit.coordinator.adapters import CoordinatorAdapters, AdapterError
from flowkit.core.time import ManualClock
from tests.helpers.detectors import FlagLeakDetector


pytestmark = [pytest.mark.unit, pytest.mark.vars]


async def _mk_task(db, tid="t_vars_merge"):
    await db.tasks.insert_one(
        {"id": tid, "pipeline_id": tid, "status": "queued", "graph": {"nodes": [], "edges": [], "edges_ex": []}}
    )
    return tid


@pytest.mark.asyncio
async def test_vars_merge_deep_overwrite_mixed_types(inmemory_db, caplog):
    """
    deep-merge с перезаписью листьев; смешанные типы (dict→лист).
    """
    tid = await _mk_task(inmemory_db)
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    # стартовый глубокий dict + скалярный лист
    await impl.vars_merge(tid, data={"obj": {"a": 1, "b": {"c": 2}}, "leaf": 7})
    # deep overwrite: обновляем obj.b.c и добавляем obj.d, плюс переписываем leaf
    caplog.set_level("INFO")
    await impl.vars_merge(tid, data={"obj": {"b": {"c": 3}, "d": 9}, "leaf": 100})

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v = doc["coordinator"]["vars"]
    assert v["obj"]["a"] == 1
    assert v["obj"]["b"]["c"] == 3
    assert v["obj"]["d"] == 9
    assert v["leaf"] == 100

    # dict → leaf: переписываем целиком obj в скаляр
    await impl.vars_merge(tid, data={"obj": 42})
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["obj"] == 42  # целиком заменился

    # лог: ключи отсортированы и есть sensitive_hits
    rec = next(r for r in caplog.records if getattr(r, "event", "") == "coord.vars.merge")
    assert getattr(rec, "keys", []) == sorted(
        [
            "coordinator.vars.leaf",
            "coordinator.vars.obj.b.c",
            "coordinator.vars.obj.d",
        ]
    )
    assert getattr(rec, "n", None) == 3
    assert getattr(rec, "sensitive_hits", None) == 0


@pytest.mark.asyncio
async def test_vars_merge_noop_empty(inmemory_db):
    """
    пустой ввод → no-op (touched=0), поле vars не создаётся.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_merge_noop")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    res = await impl.vars_merge(tid, data={})
    assert res["ok"] is True and res["touched"] == 0
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    # coordinator/vars не появились
    assert (doc.get("coordinator") or {}).get("vars") is None


@pytest.mark.asyncio
async def test_vars_merge_limits_and_value_size(inmemory_db):
    """
    лимиты: количество путей, глубина, длина пути, размер значения.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_merge_limits")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    # кол-во путей
    limit = impl._limits["max_paths_per_op"]
    big = {f"k{i}": i for i in range(limit + 1)}  # на 1 больше
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"bulk": big})

    # глубина
    depth_limit = impl._limits["max_key_depth"]
    need_segments = (depth_limit - 2) + 1  # 2 — за префикс "coordinator.vars."
    deep_obj = cur = {}
    for _ in range(need_segments):
        nxt = {}
        cur["seg"] = nxt
        cur = nxt
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"root": deep_obj})

    # длина полного пути
    path_limit = impl._limits["max_path_len"]
    prefix_len = len("coordinator.vars.")
    too_long_key = "k" * (path_limit - prefix_len + 1)
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={too_long_key: 1})

    # размер значения
    vlim = impl._limits["max_value_bytes"]
    big_val = "a" * (vlim + 1)
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"payload": big_val})


@pytest.mark.asyncio
async def test_vars_merge_block_sensitive_with_external_detector(inmemory_db, caplog):
    """
    block_sensitive=True + внешний детектор → исключение;
    без block_sensitive → запись проходит, sensitive_hits > 0.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_merge_sensitive")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector=FlagLeakDetector())

    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"secrets": {"token": "LEAK"}}, block_sensitive=True)

    caplog.set_level("INFO")
    await impl.vars_merge(tid, data={"secrets": {"token": "LEAK"}})  # мягкий режим
    rec = next(r for r in caplog.records if getattr(r, "event", "") == "coord.vars.merge")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["secrets"]["token"] == "LEAK"
