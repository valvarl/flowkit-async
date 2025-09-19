# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Kafka-backed implementation of the transport bus using aiokafka.

Features:
- JSON (de)serialization via flowkit.core.utils.dumps/loads
- optional manual commit consumers (default), with `commit()` committing current positions
- lightweight Reply correlator by `Envelope.corr_id` (useful for request/reply flows)

This module purposefully stays config-agnostic: callers pass bootstrap and then
create topic/group-bound consumers as needed.
"""

import asyncio
import logging
from typing import AsyncIterator, Mapping, cast

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, OffsetAndMetadata

from ..core.logging import get_logger, swallow
from ..core.utils import dumps, loads
from ..protocol.messages import Envelope, MsgType
from .bus import Bus, Consumer, Received


class _KafkaConsumerWrapper(Consumer):
    """
    Thin adapter over AIOKafkaConsumer to yield `Received` and support manual commits.
    """

    def __init__(self, inner: AIOKafkaConsumer, *, log) -> None:
        self._c = inner
        self._log = log

    async def stop(self) -> None:
        with swallow(
            logger=self._log,
            code="bus.kafka.consumer.stop",
            msg="consumer stop failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self._c.stop()

    async def commit(self) -> None:
        # Commits the last consumed offsets for all assigned partitions.
        with swallow(
            logger=self._log,
            code="bus.kafka.consumer.commit",
            msg="commit failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self._c.commit()

    async def commit_message(self, msg: Received) -> None:  # not part of the public protocol, but handy
        """Commit up to and including `msg.offset` for its topic/partition."""
        if msg.partition is None or msg.offset is None:
            return
        tp = TopicPartition(msg.topic, msg.partition)
        md = OffsetAndMetadata(msg.offset + 1, "")
        with swallow(
            logger=self._log,
            code="bus.kafka.consumer.commit_msg",
            msg="commit(msg) failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self._c.commit({tp: md})

    def __aiter__(self) -> AsyncIterator[Received]:
        return self._iter_messages()

    async def _iter_messages(self) -> AsyncIterator[Received]:
        async for rec in self._c:
            try:
                payload = cast(Mapping[str, object], rec.value)
                env = Envelope.model_validate(payload)
            except Exception:  # noqa: BLE001
                # Do not raise on bad payloads — log and continue.
                self._log.warning("failed to decode envelope", exc_info=True, topic=rec.topic, partition=rec.partition)
                continue
            # Map headers to bytes dict (aiokafka exposes list[tuple[str, bytes]])
            headers: Mapping[str, bytes] | None = None
            if rec.headers:
                headers = {k: v for (k, v) in rec.headers if isinstance(k, str)}
            yield Received(
                topic=rec.topic,
                partition=rec.partition,
                offset=rec.offset,
                key=rec.key,
                headers=headers,
                envelope=env,
                raw=rec,
            )


class KafkaBus(Bus):
    """
    A minimal Kafka bus wrapper with:
      - producer using idempotence,
      - consumer factory (manual commit by default),
      - in-process reply correlation (by corr_id) for convenience.

    The correlator is opt-in: call `register_reply(corr_id)` before sending a
    request to obtain an event; call `wait_replies(corr_id, timeout_ms=...)` to
    collect replies that arrived meanwhile. Incoming replies are registered when
    a consumer created by this bus iterates messages (no background thread).
    """

    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self._producer: AIOKafkaProducer | None = None
        self._consumers: list[_KafkaConsumerWrapper] = []

        # correlation: corr_id -> list[Envelope] and waiters
        self._replies: dict[str, list[Envelope]] = {}
        self._reply_events: dict[str, asyncio.Event] = {}

        self.log = get_logger("transport.kafka")

    # ---- lifecycle

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=dumps,  # expects `env.model_dump(...)`
            enable_idempotence=True,
        )
        await self._producer.start()

    async def stop(self) -> None:
        # Stop consumers first
        for cw in list(self._consumers):
            await cw.stop()
        self._consumers.clear()
        # Then producer
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
        # Clear correlator state
        self._replies.clear()
        self._reply_events.clear()

    # ---- consumer factory

    async def new_consumer(self, topics: list[str], group_id: str, *, manual_commit: bool = True) -> Consumer:
        c = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap,
            group_id=group_id,
            value_deserializer=loads,  # -> dict for Envelope.model_validate(...)
            enable_auto_commit=not manual_commit,
            auto_offset_reset="latest",
        )
        await c.start()
        wrapper = _KafkaConsumerWrapper(c, log=self.log)

        # Interpose an iterator to auto-feed correlator for replies
        async def iter_and_correlate():
            async for msg in wrapper:
                try:
                    if msg.envelope.msg_type == MsgType.reply:
                        self.push_reply(msg.envelope.corr_id, msg.envelope)
                except Exception:
                    # Best-effort: correlator errors must not break consumption
                    self.log.debug("correlator push failed", exc_info=True)
                yield msg

        class _ProxyConsumer(Consumer):
            async def stop(self_nonlocal) -> None:
                await wrapper.stop()

            async def commit(self_nonlocal) -> None:
                await wrapper.commit()

            def __aiter__(self_nonlocal) -> AsyncIterator[Received]:
                return iter_and_correlate()

        proxy = _ProxyConsumer()
        self._consumers.append(wrapper)
        return proxy

    # ---- send

    async def send(self, topic: str, key: bytes | None, env: Envelope) -> None:
        """
        Serialize and send an Envelope to Kafka.

        The `value_serializer` configured on the producer converts `env.model_dump(...)`
        to bytes. `key` is expected to be raw bytes (partitioning key).
        """
        if self._producer is None:
            raise RuntimeError("KafkaBus producer is not initialized; call start() first")
        await self._producer.send_and_wait(topic, env.model_dump(mode="json"), key=key)

    # ---- reply correlation helpers

    def register_reply(self, corr_id: str) -> asyncio.Event:
        """
        Declare interest in replies for this correlation id.
        Returns an asyncio.Event that is set when at least one reply arrives.
        """
        ev = self._reply_events.get(corr_id)
        if ev is None:
            ev = asyncio.Event()
            self._reply_events[corr_id] = ev
        self._replies.setdefault(corr_id, [])
        return ev

    def push_reply(self, corr_id: str, env: Envelope) -> None:
        """Internal: record a reply and notify waiters."""
        bucket = self._replies.setdefault(corr_id, [])
        bucket.append(env)
        if ev := self._reply_events.get(corr_id):
            ev.set()

    def collect_replies(self, corr_id: str) -> list[Envelope]:
        """
        Collect and clear replies accumulated so far for `corr_id`.
        Returns an empty list if none.
        """
        envs = self._replies.pop(corr_id, [])
        self._reply_events.pop(corr_id, None)
        return envs

    async def wait_replies(self, corr_id: str, *, timeout_ms: int | None = None) -> list[Envelope]:
        """
        Wait until at least one reply arrives (or timeout), then collect and return all.
        If there were replies already, returns immediately.
        """
        if self._replies.get(corr_id):
            return self.collect_replies(corr_id)
        ev = self._reply_events.get(corr_id) or self.register_reply(corr_id)
        try:
            if timeout_ms is None:
                await ev.wait()
            else:
                await asyncio.wait_for(ev.wait(), timeout=timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            pass
        return self.collect_replies(corr_id)
