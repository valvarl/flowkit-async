from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ..core.log import get_logger, swallow
from ..core.utils import dumps, loads
from ..protocol.messages import Envelope


class KafkaBus:
    """
    Thin wrapper around AIOKafka with a minimal reply correlator (by corr_id).
    Config-agnostic: knows only 'bootstrap' and (de)serialization.
    """

    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self._producer: AIOKafkaProducer | None = None
        self._consumers: list[AIOKafkaConsumer] = []
        self._replies: dict[str, list[Envelope]] = {}
        self._reply_events: dict[str, asyncio.Event] = {}
        self.log = get_logger("bus.kafka")

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=dumps,
            enable_idempotence=True,
        )
        await self._producer.start()

    async def stop(self) -> None:
        for c in self._consumers:
            with swallow(
                logger=self.log,
                code="bus.kafka.consumer.stop",
                msg="consumer stop failed",
                level=logging.WARNING,
                expected=True,
            ):
                await c.stop()
        self._consumers.clear()
        if self._producer:
            with swallow(
                logger=self.log,
                code="bus.kafka.producer.stop",
                msg="producer stop failed",
                level=logging.WARNING,
                expected=True,
            ):
                await self._producer.stop()
        self._producer = None

    async def new_consumer(self, topics: list[str], group_id: str, *, manual_commit: bool = True) -> AIOKafkaConsumer:
        c = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap,
            group_id=group_id,
            value_deserializer=loads,
            enable_auto_commit=not manual_commit,
            auto_offset_reset="latest",
        )
        await c.start()
        self._consumers.append(c)
        return c

    # ---- send
    async def send(self, topic: str, key: bytes | None, env: Envelope) -> None:
        """
        Public send: accepts Envelope, applies serializer configured in producer.
        """
        if self._producer is None:
            raise RuntimeError("KafkaBus producer is not initialized")
        await self._producer.send_and_wait(topic, env.model_dump(mode="json"), key=key)

    # ---- replies
    def register_reply(self, corr_id: str) -> asyncio.Event:
        ev = asyncio.Event()
        self._replies[corr_id] = []
        self._reply_events[corr_id] = ev
        return ev

    def push_reply(self, corr_id: str, env: Envelope) -> None:
        bucket = self._replies.get(corr_id)
        if bucket is not None:
            bucket.append(env)
        ev = self._reply_events.get(corr_id)
        if ev:
            ev.set()

    def collect_replies(self, corr_id: str) -> list[Envelope]:
        envs = self._replies.pop(corr_id, [])
        self._reply_events.pop(corr_id, None)
        return envs
