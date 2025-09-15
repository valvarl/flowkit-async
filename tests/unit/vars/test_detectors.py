from __future__ import annotations

import sys
import types

import pytest
from tests.helpers.detectors import FlagLeakDetector

from flowkit.coordinator.adapters import AdapterError, CoordinatorAdapters
from flowkit.core.time import ManualClock

from ._shared import get_record_by_event, mk_task

pytestmark = [pytest.mark.unit, pytest.mark.vars]


@pytest.mark.asyncio
async def test_detector_object_block_and_soft(inmemory_db, caplog):
    """
    DI: object detector. With block_sensitive=True — raise; without — write and count hit.
    """
    tid = await mk_task(inmemory_db, "t_det_obj")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector=FlagLeakDetector())

    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"token": "LEAK"}, block_sensitive=True)

    caplog.set_level("INFO")
    await impl.vars_set(tid, kv={"token": "LEAK"})
    rec = get_record_by_event(caplog, "coord.vars.set")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["token"] == "LEAK"


@pytest.mark.asyncio
async def test_detector_function_with_merge(inmemory_db, caplog):
    """
    DI: callable detector. Block big binary payload when block_sensitive=True, otherwise log hit.
    """
    tid = await mk_task(inmemory_db, "t_det_func")

    def detect_big_bytes(path, value):
        return isinstance(value, bytes | bytearray) and len(value) > 1000

    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector=detect_big_bytes)

    payload = b"a" * 2000
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"blob": payload}, block_sensitive=True)

    caplog.set_level("INFO")
    await impl.vars_merge(tid, data={"blob": payload})
    rec = get_record_by_event(caplog, "coord.vars.merge")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert isinstance(doc["coordinator"]["vars"]["blob"], bytes | bytearray)


@pytest.mark.asyncio
async def test_detector_string_import(inmemory_db, caplog, monkeypatch):
    """
    DI: string import "module:factory". Factory returns an object with is_sensitive().
    """
    tid = await mk_task(inmemory_db, "t_det_import")

    # Dynamically create a module and register in sys.modules
    mod = types.ModuleType("ext_det_mod")

    class LocalDetector:
        def is_sensitive(self, key_path, value):
            return value == "XYZ"

    def make_detector():
        return LocalDetector()

    mod.make_detector = make_detector  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "ext_det_mod", mod)

    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector="ext_det_mod:make_detector")

    # strict mode → exception
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"x": "XYZ"}, block_sensitive=True)

    # soft mode → write + hit
    caplog.set_level("INFO")
    await impl.vars_set(tid, kv={"x": "XYZ"})
    rec = get_record_by_event(caplog, "coord.vars.set")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["x"] == "XYZ"
