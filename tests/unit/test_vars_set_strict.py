import asyncio
import pytest

from flowkit.coordinator.adapters import CoordinatorAdapters, AdapterError
from flowkit.core.time import ManualClock
from tests.helpers.detectors import FlagLeakDetector


pytestmark = [pytest.mark.unit, pytest.mark.vars]


async def _mk_task(db, tid="t_vars_set"):
    await db.tasks.insert_one(
        {"id": tid, "pipeline_id": tid, "status": "queued", "graph": {"nodes": [], "edges": [], "edges_ex": []}}
    )
    return tid


@pytest.mark.asyncio
async def test_vars_set_dotted_and_nested_combo_and_logging(inmemory_db, caplog):
    """
    dotted + nested формы в одном вызове; проверка значений и structured-логов.
    """
    tid = await _mk_task(inmemory_db)
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    caplog.set_level("INFO")
    await impl.vars_set(
        tid,
        kv={
            "plain": 7,
            "sla.max_delay": 500,
            "flags": {"ab": True, "beta": False},
            "nested": {"lvl1": {"lvl2": 123}},
        },
    )

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v = doc["coordinator"]["vars"]
    assert v["plain"] == 7
    assert v["sla"]["max_delay"] == 500
    assert v["flags"]["ab"] is True and v["flags"]["beta"] is False
    assert v["nested"]["lvl1"]["lvl2"] == 123

    rec = next(r for r in caplog.records if getattr(r, "event", "") == "coord.vars.set")
    # В логах должны быть отсортированные полные пути set_doc (с префиксом coordinator.vars.)
    expected = sorted(
        [
            "coordinator.vars.plain",
            "coordinator.vars.sla.max_delay",
            "coordinator.vars.flags.ab",
            "coordinator.vars.flags.beta",
            "coordinator.vars.nested.lvl1.lvl2",
        ]
    )
    assert getattr(rec, "keys", []) == expected
    assert getattr(rec, "n", None) == len(expected)
    # чувствительные значения не передавались
    assert getattr(rec, "sensitive_hits", None) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_kv",
    [
        {"$bad": 1},                      # сегмент начинается с $
        {"x": {"bad.seg": 1}},            # точка внутри имени сегмента (вложенный dict)
        {"bad\0seg.ok": 1},               # NUL в сегменте (в dotted-ключе)
        {"x": {"a" * 129: 1}},            # сегмент длинее лимита
    ],
)
async def test_vars_set_validates_key_segments(inmemory_db, bad_kv):
    """
    Валидация сегментов: $, '.', NUL, длина сегмента.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_set_validate")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv=bad_kv)


@pytest.mark.asyncio
async def test_vars_set_limits_paths_count(inmemory_db):
    """
    Лимит на количество путей в одном $set.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_set_paths")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    limit = impl._limits["max_paths_per_op"]  # приватное, но для теста допустимо
    bulk = {f"k{i}": i for i in range(limit + 1)}  # на 1 больше лимита
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"bulk": bulk})


@pytest.mark.asyncio
async def test_vars_set_limits_depth_and_path_len(inmemory_db):
    """
    Лимиты: глубина пути и длина полного пути.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_set_depth_path")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    # depth: учитываем, что префикс "coordinator.vars." уже дает 2 точки
    depth_limit = impl._limits["max_key_depth"]
    need_segments = (depth_limit - 2) + 1
    deep_key = ".".join(["seg"] * need_segments)
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={deep_key: 1})

    # path length: строим ключ чуть длиннее лимита (с учётом префикса)
    path_limit = impl._limits["max_path_len"]
    prefix_len = len("coordinator.vars.")
    too_long = "k" * (path_limit - prefix_len + 1)
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={too_long: 1})


@pytest.mark.asyncio
async def test_vars_set_limits_value_size(inmemory_db):
    """
    Лимит на размер значения (строка/байты).
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_set_valsize")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    vlim = impl._limits["max_value_bytes"]
    big = "a" * (vlim + 1)
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"payload": big})


@pytest.mark.asyncio
async def test_vars_set_deterministic_keys_order_and_sensitive_hits(inmemory_db, caplog):
    """
    Детерминированная сортировка путей в логах + sensitive_hits счётчик.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_set_order")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    caplog.set_level("INFO")
    # порядок ключей в вводе «перемешан»
    await impl.vars_set(
        tid,
        kv={
            "b.z": 1,
            "a.y": 2,
            "b": {"x": 3},
            # нет секретов → sensitive_hits должен быть 0
        },
    )
    rec = next(r for r in caplog.records if getattr(r, "event", "") == "coord.vars.set")
    assert getattr(rec, "keys", []) == ["coordinator.vars.a.y", "coordinator.vars.b.x", "coordinator.vars.b.z"]
    assert getattr(rec, "sensitive_hits", None) == 0


@pytest.mark.asyncio
async def test_vars_set_block_sensitive_with_external_detector(inmemory_db, caplog):
    """
    При block_sensitive=True внешний детектор блокирует запись.
    Также без block_sensitive операция проходит, а в логах видно sensitive_hits=1.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_set_sensitive")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector=FlagLeakDetector())

    # strict режим: блокируем
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"token": "LEAK", "block_sensitive": True})

    # мягкий режим: без block_sensitive — записываем, но в логах считаем hit
    caplog.set_level("INFO")
    await impl.vars_set(tid, kv={"token": "LEAK"})  # без block_sensitive
    rec = next(r for r in caplog.records if getattr(r, "event", "") == "coord.vars.set")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["token"] == "LEAK"
