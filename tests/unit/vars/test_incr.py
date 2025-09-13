from __future__ import annotations

import asyncio
import math

import pytest

from flowkit.coordinator.adapters import AdapterError, CoordinatorAdapters
from flowkit.core.time import ManualClock

from ._shared import BAD_KEYS, mk_task

pytestmark = [pytest.mark.unit, pytest.mark.vars]


@pytest.mark.asyncio
async def test_vars_incr_creates_missing_leaf(inmemory_db):
    """Creating a missing leaf should create it and increment."""
    tid = await mk_task(inmemory_db, "t_vars_incr_create")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    await impl.vars_incr(tid, key="counters.pages", by=2)
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["counters"]["pages"] == 2


@pytest.mark.asyncio
async def test_vars_incr_concurrency_sum(inmemory_db):
    """Concurrency: 10x1 + 5x2 → exact sum."""
    tid = await mk_task(inmemory_db, "t_vars_incr_conc")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    async def bump(n):
        await impl.vars_incr(tid, key="counters.pages", by=n)

    await asyncio.gather(*(bump(1) for _ in range(10)), *(bump(2) for _ in range(5)))
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["counters"]["pages"] == (10 * 1 + 5 * 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("val", [float("nan"), float("inf"), -float("inf")])
async def test_vars_incr_rejects_nan_inf(inmemory_db, val):
    """NaN / +/-Inf must be rejected."""
    tid = await mk_task(inmemory_db, f"t_vars_incr_bad_{val!r}")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    with pytest.raises(AdapterError):
        await impl.vars_incr(tid, key="counters.pages", by=val)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_key", BAD_KEYS)
async def test_vars_incr_validates_key(inmemory_db, bad_key):
    """Key validation (segments)."""
    tid = await mk_task(inmemory_db, "t_vars_incr_validate")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    with pytest.raises(AdapterError):
        await impl.vars_incr(tid, key=bad_key, by=1)


@pytest.mark.asyncio
async def test_vars_incr_float_and_negative_and_type_conflict(inmemory_db):
    """
    Mixed numeric types are allowed; negative increments also work.
    If a non-numeric value already exists at the leaf, the adapter should raise.
    """
    tid = await mk_task(inmemory_db, "t_vars_incr_mixed")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    # start with float
    await impl.vars_incr(tid, key="metrics.score", by=1.5)
    await impl.vars_incr(tid, key="metrics.score", by=-0.5)
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert math.isclose(doc["coordinator"]["vars"]["metrics"]["score"], 1.0, rel_tol=1e-9)

    # inject a conflicting non-numeric leaf and ensure incr fails
    await impl.vars_set(tid, kv={"metrics.label": "A"})
    with pytest.raises(AdapterError):
        await impl.vars_incr(tid, key="metrics.label", by=1)
