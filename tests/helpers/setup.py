import os
from typing import Any, Dict, Optional

from .inmemory_db import InMemDB
from .kafka import (BROKER, AIOKafkaConsumerMock, AIOKafkaProducerMock,
                    reset_broker)
from .util import dbg


def _best_effort_set_constants(monkeypatch, mod, updates: Dict[str, Any]):
    """
    Tries multiple paths so we don't fight with different config styles.
    1) env (FLOWKIT_* and plain names)
    2) direct module attributes
    3) nested config objects/dicts: cfg / CONFIG / settings
    """
    # 1) env
    for k, v in updates.items():
        monkeypatch.setenv(f"FLOWKIT_{k}", str(v))
        monkeypatch.setenv(k, str(v))  # fallback

    # 2) direct attrs
    for k, v in updates.items():
        try:
            monkeypatch.setattr(mod, k, v, raising=False)
        except Exception:
            pass

    # 3) config containers
    for container_name in ("cfg", "CONFIG", "settings"):
        if hasattr(mod, container_name):
            cont = getattr(mod, container_name)
            try:
                if isinstance(cont, dict):
                    cont.update(updates)
                else:
                    for k, v in updates.items():
                        setattr(cont, k, v)
            except Exception:
                pass

def setup_env_and_imports(monkeypatch, *, worker_types: str = "indexer,enricher,ocr,analyzer"):
    """
    Wires in-memory Kafka + installs outbox bypass (optional) and imports coordinator/worker fresh.
    """
    reset_broker()
    monkeypatch.setenv("WORKER_TYPES", worker_types)

    # ensure fresh import
    import sys as _sys
    for m in list(_sys.modules.keys()):
        if m.startswith("flowkit.") and (".runner" in m or ".dispatcher" in m or ".artifacts" in m):
            del _sys.modules[m]

    from flowkit.coordinator import runner as cd
    from flowkit.worker import runner as wu
    from flowkit.bus import kafka as bus

    # replace aiokafka
    monkeypatch.setattr(bus, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(bus, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)

    # speed up loops (safe if absent)
    _best_effort_set_constants(monkeypatch, cd, {
        "SCHEDULER_TICK_SEC": 0.05,
        "DISCOVERY_WINDOW_SEC": 0.05,
        "FINALIZER_TICK_SEC": 0.05,
        "HB_MONITOR_TICK_SEC": 0.2,
    })

    # outbox bypass (optional)
    if os.getenv("TEST_USE_OUTBOX", "0") != "1":
        try:
            import flowkit.outbox.dispatcher as disp
            async def _enqueue_direct(self, *, topic: str, key: bytes, env):
                dbg("OUTBOX.BYPASS", topic=topic)
                await BROKER.produce(topic, env.model_dump(mode="json"))
            monkeypatch.setattr(disp.OutboxDispatcher, "enqueue", _enqueue_direct, raising=False)
        except Exception:
            pass

    dbg("ENV.READY", worker_types=worker_types)
    return cd, wu

def install_inmemory_db() -> InMemDB:
    """
    Installs a shared in-memory DB into coordinator and worker modules (and worker.artifacts if present).
    """
    db = InMemDB()
    dbg("DB.INSTALLED", mode="injected_at_construction")
    return db

def apply_overrides(monkeypatch, *, cd=None, wu=None, coord_overrides: Optional[Dict[str, Any]] = None, worker_overrides: Optional[Dict[str, Any]] = None):
    """
    Single entry to adjust runtime constants safely.
    """
    if cd and coord_overrides:
        _best_effort_set_constants(monkeypatch, cd, coord_overrides)
    if wu and worker_overrides:
        _best_effort_set_constants(monkeypatch, wu, worker_overrides)
