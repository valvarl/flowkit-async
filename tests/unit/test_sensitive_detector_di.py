import types
import sys
import pytest

from flowkit.coordinator.adapters import CoordinatorAdapters, AdapterError
from flowkit.core.time import ManualClock
from tests.helpers.detectors import FlagLeakDetector


pytestmark = [pytest.mark.unit, pytest.mark.vars]


async def _mk_task(db, tid="t_detectors"):
    await db.tasks.insert_one(
        {"id": tid, "pipeline_id": tid, "status": "queued", "graph": {"nodes": [], "edges": [], "edges_ex": []}}
    )
    return tid


@pytest.mark.asyncio
async def test_detector_object_block_and_soft(inmemory_db, caplog):
    """
    DI: объект-детектор. При block_sensitive=True — исключение; без — пишем и считаем hit.
    """
    tid = await _mk_task(inmemory_db, tid="t_det_obj")
    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector=FlagLeakDetector())

    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"token": "LEAK", "block_sensitive": True})

    caplog.set_level("INFO")
    await impl.vars_set(tid, kv={"token": "LEAK"})
    rec = next(r for r in caplog.records if getattr(r, "event", "") == "coord.vars.set")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["token"] == "LEAK"


@pytest.mark.asyncio
async def test_detector_function_with_merge(inmemory_db, caplog):
    """
    DI: функция-детектор. Блокируем большие бинарные значения при block_sensitive=True.
    """
    tid = await _mk_task(inmemory_db, tid="t_det_func")

    def detect_big_bytes(path, value):
        return isinstance(value, (bytes, bytearray)) and len(value) > 1000

    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector=detect_big_bytes)

    payload = b"a" * 2000
    with pytest.raises(AdapterError):
        await impl.vars_merge(tid, data={"blob": payload}, block_sensitive=True)

    caplog.set_level("INFO")
    await impl.vars_merge(tid, data={"blob": payload})
    rec = next(r for r in caplog.records if getattr(r, "event", "") == "coord.vars.merge")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert isinstance(doc["coordinator"]["vars"]["blob"], (bytes, bytearray))


@pytest.mark.asyncio
async def test_detector_string_import(inmemory_db, caplog, monkeypatch):
    """
    DI: строковый импорт "module:factory". Фабрика возвращает объект с is_sensitive().
    """
    tid = await _mk_task(inmemory_db, tid="t_det_import")

    # динамически создаём модуль и регистрируем в sys.modules
    mod = types.ModuleType("ext_det_mod")

    class LocalDetector:
        def is_sensitive(self, key_path, value):
            return value == "XYZ"

    def make_detector():
        return LocalDetector()

    mod.make_detector = make_detector  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "ext_det_mod", mod)

    impl = CoordinatorAdapters(db=inmemory_db, clock=ManualClock(), detector="ext_det_mod:make_detector")

    # строгий режим → исключение
    with pytest.raises(AdapterError):
        await impl.vars_set(tid, kv={"x": "XYZ", "block_sensitive": True})

    # мягкий режим → write + hit
    caplog.set_level("INFO")
    await impl.vars_set(tid, kv={"x": "XYZ"})
    rec = next(r for r in caplog.records if getattr(r, "event", "") == "coord.vars.set")
    assert getattr(rec, "sensitive_hits", None) == 1

    doc = await inmemory_db.tasks.find_one({"id": tid}, {"coordinator": 1})
    assert doc["coordinator"]["vars"]["x"] == "XYZ"
