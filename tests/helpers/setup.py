from __future__ import annotations

import os
import sys

from flowkit.core.log import get_logger

from .inmemory_db import InMemDB
from .kafka import BROKER, AIOKafkaConsumerMock, AIOKafkaProducerMock, reset_broker

LOG = get_logger("tests.helpers.env")


def setup_env_and_imports(monkeypatch, *, worker_types: str = "indexer,enricher,ocr,analyzer"):
    """
    Wire in-memory Kafka, optionally bypass the outbox, and import coordinator/worker fresh.
    """
    # Reset broker
    reset_broker()
    LOG.debug("env.broker.reset", event="env.broker.reset")

    # Ensure fresh import: drop previously loaded runner/dispatcher/artifacts modules
    to_drop = [
        m
        for m in list(sys.modules.keys())
        if m.startswith("flowkit.") and (".runner" in m or ".dispatcher" in m or ".artifacts" in m)
    ]
    for m in to_drop:
        del sys.modules[m]
    LOG.debug(
        "env.modules.reloaded",
        event="env.modules.reloaded",
        dropped=len(to_drop),
        modules=to_drop,
    )

    # Import after purge
    from flowkit.bus import kafka as bus  # type: ignore
    from flowkit.coordinator import runner as cd  # type: ignore
    from flowkit.worker import runner as wu  # type: ignore

    # Replace aiokafka in both bus and worker with in-memory mocks
    monkeypatch.setattr(bus, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(bus, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)
    LOG.debug("env.kafka.mocked", event="env.kafka.mocked")

    # Optional outbox bypass
    if os.getenv("TEST_USE_OUTBOX", "0") != "1":
        try:
            import flowkit.outbox.dispatcher as disp  # type: ignore

            async def _enqueue_direct(self, *, topic: str, key: bytes, env):
                LOG.debug(
                    "outbox.bypass",
                    event="outbox.bypass",
                    topic=topic,
                    key=(key.decode(errors="ignore") if isinstance(key, bytes | bytearray) else str(key)),
                )
                await BROKER.produce(topic, env.model_dump(mode="json"))

            # Note: raising=False to tolerate attribute differences across versions
            monkeypatch.setattr(disp.OutboxDispatcher, "enqueue", _enqueue_direct, raising=False)
            LOG.debug("outbox.bypass.enabled", event="outbox.bypass.enabled")
        except Exception as e:
            LOG.debug("outbox.bypass.unavailable", event="outbox.bypass.unavailable", error=str(e))

    LOG.debug("env.ready", event="env.ready", worker_types=worker_types)
    return cd, wu


def install_inmemory_db() -> InMemDB:
    """
    Install a shared in-memory DB for coordinator/worker modules (and artifacts if present).
    """
    db = InMemDB()
    LOG.debug("db.installed", event="db.installed", mode="injected_at_construction")
    return db
