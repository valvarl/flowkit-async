from __future__ import annotations

import pytest
from tests.helpers.detectors import FlagLeakDetector

from flowkit.coordinator.adapters import AdapterError, CoordinatorAdapters
from flowkit.core.time import ManualClock

from ._shared import get_record_by_event, mk_task, too_long_key

pytestmark = [pytest.mark.unit, pytest.mark.vars]


@pytest.mark.asyncio
async def test_vars_set_dotted_and_nested_combo_and_logging(inmemory_db, caplog):
    """Dotted + nested forms in one call; verify values and structured logs."""
    tid = await mk_task(inmemory_db, "t_vars_set_mix")
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

    rec = get_record_by_event(caplog, "coord.vars.set")
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
    assert getattr(rec, "sensitive_hits", None) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_kv",
    [
        {"$bad": 1},
        {"x": {"bad.seg": 1}},
        {"bad\0seg.ok": 1},
        {"x": {"a" * 129: 1}},
    ],
)
async def test_vars_set_validates_key_segments(inmemory_db, bad_kv):
    """Validate segments: $, '.', NUL, segment length."""
    tid = await mk_task(inmemory_db, "t_vars_set_validate")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv=bad_kv)


@pytest.mark.asyncio
async def test_vars_set_limits_paths_count(inmemory_db):
    """Limit on number of paths in a single $set."""
    tid = await mk_task(inmemory_db, "t_vars_set_paths")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    limit = impl._limits["max_paths_per_op"]
    bulk = {f"k{i}": i for i in range(limit + 1)}  # 1 over
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"bulk": bulk})


@pytest.mark.asyncio
async def test_vars_set_limits_depth_and_path_len(inmemory_db):
    """Limits: key depth and full path length."""
    tid = await mk_task(inmemory_db, "t_vars_set_depth_path")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    depth_limit = impl._limits["max_key_depth"]
    need_segments = (depth_limit - 2) + 1  # 'coordinator.vars.' contributes two dots
    deep_key = ".".join(["seg"] * need_segments)
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={deep_key: 1})

    path_limit = impl._limits["max_path_len"]
    prefix_len = len("coordinator.vars.")
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={too_long_key(path_limit, prefix_len): 1})


@pytest.mark.asyncio
async def test_vars_set_limits_value_size_str_and_bytes(inmemory_db):
    """Limit on value size (string/bytes)."""
    tid = await mk_task(inmemory_db, "t_vars_set_valsize")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    vlim = impl._limits["max_value_bytes"]
    big_str = "a" * (vlim + 1)
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"payload": big_str})

    big_bytes = b"a" * (vlim + 1)
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"payload": big_bytes})


@pytest.mark.asyncio
async def test_vars_set_deterministic_keys_order_and_sensitive_hits(inmemory_db, caplog):
    """Deterministic sort order in logs + sensitive_hits counter."""
    tid = await mk_task(inmemory_db, "t_vars_set_order")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock())

    caplog.set_level("INFO")
    await impl.vars_set(
        tid,
        kv={
            "b.z": 1,
            "a.y": 2,
            "b": {"x": 3},
        },
    )
    rec = get_record_by_event(caplog, "coord.vars.set")
    assert getattr(rec, "keys", []) == ["coordinator.vars.a.y", "coordinator.vars.b.x", "coordinator.vars.b.z"]
    assert getattr(rec, "sensitive_hits", None) == 0


@pytest.mark.asyncio
async def test_vars_set_block_sensitive_with_external_detector(inmemory_db, caplog):
    """
    With block_sensitive=True the detector blocks.
    Without it, write succeeds and logs record sensitive_hits=1.
    """
    tid = await mk_task(inmemory_db, "t_vars_set_sensitive")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector=FlagLeakDetector())

    # strict mode: block
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"token": "LEAK"}, block_sensitive=True)

    # soft mode
    caplog.set_level("INFO")
    await impl.vars_set(tid, kv={"token": "LEAK"})
    rec = get_record_by_event(caplog, "coord.vars.set")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["token"] == "LEAK"
