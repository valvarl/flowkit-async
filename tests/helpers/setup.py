import os
from typing import Any, Dict, Optional

from .inmemory_db import InMemDB
from .kafka import (BROKER, AIOKafkaConsumerMock, AIOKafkaProducerMock,
                    reset_broker)
from .util import dbg


def setup_env_and_imports(monkeypatch, *, worker_types: str = "indexer,enricher,ocr,analyzer"):
    """
    Wires in-memory Kafka + installs outbox bypass (optional) and imports coordinator/worker fresh.
    """
    reset_broker()

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
