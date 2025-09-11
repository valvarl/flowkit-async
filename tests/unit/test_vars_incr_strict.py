import asyncio
import math
import pytest

from flowkit.coordinator.adapters import CoordinatorAdapters, AdapterError
from flowkit.core.time import ManualClock


pytestmark = [pytest.mark.unit, pytest.mark.vars]


async def _mk_task(db, tid="t_vars_incr"):
    await db.tasks.insert_one(
        {"id": tid, "pipeline_id": tid, "status": "queued", "graph": {"nodes": [], "edges": [], "edges_ex": []}}
    )
    return tid


@pytest.mark.asyncio
async def test_vars_incr_creates_missing_leaf(inmemory_db):
    """
    создание отсутствующего ключа (должен появиться и инкрементироваться).
    """
    tid = await _mk_task(inmemory_db)
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    await impl.vars_incr(tid, key="counters.pages", by=2)
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["counters"]["pages"] == 2


@pytest.mark.asyncio
async def test_vars_incr_concurrency_sum(inmemory_db):
    """
    параллельность: 10×by=1 + 5×by=2 ⇒ точная сумма.
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_incr_conc")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    async def bump(n):
        await impl.vars_incr(tid, key="counters.pages", by=n)

    await asyncio.gather(*(bump(1) for _ in range(10)), *(bump(2) for _ in range(5)))
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["counters"]["pages"] == (10 * 1 + 5 * 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("val", [float("nan"), float("inf"), -float("inf")])
async def test_vars_incr_rejects_nan_inf(inmemory_db, val):
    """
    запрет NaN/Inf.
    """
    tid = await _mk_task(inmemory_db, tid=f"t_vars_incr_bad_{repr(val)}")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    with pytest.raises(AdapterError):
        await impl.vars_incr(tid, key="counters.pages", by=val)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_key",
    [
        "",             # пусто
        "$bad",         # сегмент начинается с $
        "a..b",         # пустой сегмент
        "bad\0x",       # NUL
        "a." + ("s" * 129),  # длина сегмента > лимита
    ],
)
async def test_vars_incr_validates_key(inmemory_db, bad_key):
    """
    валидация ключа (сегменты).
    """
    tid = await _mk_task(inmemory_db, tid="t_vars_incr_validate")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    with pytest.raises(AdapterError):
        await impl.vars_incr(tid, key=bad_key, by=1)
