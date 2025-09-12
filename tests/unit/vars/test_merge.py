from __future__ import annotations

import pytest
from tests.helpers.detectors import FlagLeakDetector

from flowkit.coordinator.adapters import AdapterError, CoordinatorAdapters
from flowkit.core.time import ManualClock

from ._shared import get_record_by_event, make_deep_obj, mk_task, too_long_key

pytestmark = [pytest.mark.unit, pytest.mark.vars]


@pytest.mark.asyncio
async def test_vars_merge_deep_overwrite_mixed_types(inmemory_db, caplog):
    """Deep-merge with leaf overwrite; mixed types (dict → leaf)."""
    tid = await mk_task(inmemory_db, "t_vars_merge")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    # seed: deep dict + scalar leaf
    await impl.vars_merge(tid, data={"obj": {"a": 1, "b": {"c": 2}}, "leaf": 7})
    # deep overwrite: update obj.b.c and add obj.d; overwrite leaf
    caplog.set_level("INFO")
    await impl.vars_merge(tid, data={"obj": {"b": {"c": 3}, "d": 9}, "leaf": 100})

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    v = doc["coordinator"]["vars"]
    assert v["obj"]["a"] == 1
    assert v["obj"]["b"]["c"] == 3
    assert v["obj"]["d"] == 9
    assert v["leaf"] == 100

    # dict → scalar: replace the whole obj
    await impl.vars_merge(tid, data={"obj": 42})
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["obj"] == 42

    # structured log: sorted keys and sensitive_hits
    rec = get_record_by_event(caplog, "coord.vars.merge")
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
    """Empty input → no-op (touched=0), 'vars' field is not created."""
    tid = await mk_task(inmemory_db, "t_vars_merge_noop")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    res = await impl.vars_merge(tid, data={})
    assert res["ok"] is True and res["touched"] == 0
    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert (doc.get("coordinator") or {}).get("vars") is None


@pytest.mark.asyncio
async def test_vars_merge_limits_and_value_size_str_and_bytes(inmemory_db):
    """Limits: number of paths, depth, full path length, and value size (str/bytes)."""
    tid = await mk_task(inmemory_db, "t_vars_merge_limits")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    # number of paths
    limit = impl._limits["max_paths_per_op"]  # allowed in tests
    big = {f"k{i}": i for i in range(limit + 1)}  # 1 over
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"bulk": big})

    # depth (account for 'coordinator.vars.' two dots)
    depth_limit = impl._limits["max_key_depth"]
    need_segments = (depth_limit - 2) + 1
    deep_obj = make_deep_obj(need_segments)
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"root": deep_obj})

    # full path length
    path_limit = impl._limits["max_path_len"]
    prefix_len = len("coordinator.vars.")
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={too_long_key(path_limit, prefix_len): 1})

    # value size (str)
    vlim = impl._limits["max_value_bytes"]
    big_str = "a" * (vlim + 1)
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"payload": big_str})

    # value size (bytes)
    big_bytes = b"a" * (vlim + 1)
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"blob": big_bytes})


@pytest.mark.asyncio
async def test_vars_merge_block_sensitive_with_external_detector(inmemory_db, caplog):
    """
    External detector: block when block_sensitive=True,
    otherwise write but count sensitive_hits in logs.
    """
    tid = await mk_task(inmemory_db, "t_vars_merge_sensitive")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector=FlagLeakDetector())

    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"secrets": {"token": "LEAK"}}, block_sensitive=True)

    caplog.set_level("INFO")
    await impl.vars_merge(tid, data={"secrets": {"token": "LEAK"}})  # soft mode
    rec = get_record_by_event(caplog, "coord.vars.merge")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["secrets"]["token"] == "LEAK"
