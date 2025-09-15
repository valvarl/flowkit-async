from __future__ import annotations

import pytest

from flowkit.coordinator.adapters import AdapterError, CoordinatorAdapters
from flowkit.core.time import ManualClock

from ._shared import BAD_KEYS, mk_task, too_long_key

pytestmark = [pytest.mark.unit, pytest.mark.vars]


@pytest.mark.asyncio
async def test_vars_unset_simple_nested_and_noop(inmemory_db):
    """
    Remove a simple leaf and a nested branch; missing key is a no-op
    (operation remains valid and accounted in 'touched').
    """
    tid = await mk_task(inmemory_db, "t_vars_unset")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    await impl.vars_set(
        tid,
        kv={
            "limits.max": 5,
            "flags.beta": True,
            "nested": {"lvl1": {"lvl2": 123}},
        },
    )

    # 1) simple leaf
    res1 = await impl.vars_unset(tid, "limits.max")
    assert res1["ok"] is True and res1["touched"] == 1
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v = doc["coordinator"]["vars"]
    assert "max" not in (v.get("limits") or {})
    assert v["flags"]["beta"] is True

    # 2) nested branch
    res2 = await impl.vars_unset(tid, "nested.lvl1")
    assert res2["ok"] is True and res2["touched"] == 1
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v = doc["coordinator"]["vars"]
    assert "lvl1" not in (v.get("nested") or {})

    # 3) missing key — no-op on state, still counted in touched per current API
    res3 = await impl.vars_unset(tid, "missing.key")
    assert res3["ok"] is True and res3["touched"] == 1
    doc2 = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc2 == doc


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_key", BAD_KEYS)
async def test_vars_unset_validates_paths(inmemory_db, bad_key):
    """Validate key/path formats: $, '.', NUL, segment length."""
    tid = await mk_task(inmemory_db, "t_vars_unset_validate")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    with pytest.raises(AdapterError):
        await impl.vars_unset(tid, bad_key)


@pytest.mark.asyncio
async def test_vars_unset_path_length_limit(inmemory_db):
    """Validate full path length limit (including prefix 'coordinator.vars.')."""
    tid = await mk_task(inmemory_db, "t_vars_unset_longpath")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    path_limit = impl._limits["max_path_len"]
    prefix_len = len("coordinator.vars.")
    with pytest.raises(AdapterError):
        await impl.vars_unset(tid, too_long_key(path_limit, prefix_len))


@pytest.mark.asyncio
async def test_vars_unset_accepts_args_and_kwargs(inmemory_db):
    """
    The adapter should accept both *args (varargs keys) and kwargs form 'keys=[...]'.
    """
    tid = await mk_task(inmemory_db, "t_vars_unset_forms")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    await impl.vars_set(tid, kv={"a.b": 1, "c.d": 2, "e.f": 3})

    # varargs
    res1 = await impl.vars_unset(tid, "a.b", "c.d")
    assert res1["ok"] is True and res1["touched"] == 2
    doc1 = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v1 = (doc1.get("coordinator") or {}).get("vars") or {}
    assert "b" not in (v1.get("a") or {})
    assert "d" not in (v1.get("c") or {})
    assert v1.get("e", {}).get("f") == 3

    # kwargs
    res2 = await impl.vars_unset(tid, keys=["e.f", "missing.zz"])
    assert res2["ok"] is True and res2["touched"] == 2
    doc2 = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v2 = (doc2.get("coordinator") or {}).get("vars") or {}
    assert "f" not in (v2.get("e") or {})
