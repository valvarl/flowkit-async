from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any

from flowkit.core.log import get_logger, log_context

LOG = get_logger("tests.kafka")


# ───────────────────────── Chaos config ─────────────────────────


@dataclass
class ChaosConfig:
    """
    Lightweight chaos mode for the in-memory Kafka:
      - broker/consumer jitter (small sleeps)
      - message duplication / drop by topic prefix

    Keep probabilities small to avoid flaky tests.
    """

    seed: int = 12345
    broker_delay_range: tuple[float, float] = (0.0, 0.003)  # seconds
    consumer_poll_delay_range: tuple[float, float] = (0.0, 0.002)
    dup_prob_by_topic: dict[str, float] = field(default_factory=dict)  # e.g. {"status.": 0.06}
    drop_prob_by_topic: dict[str, float] = field(default_factory=dict)  # e.g. {"cmd.": 0.01}
    enable_broker_delays: bool = True
    enable_consumer_delays: bool = True
    enable_dup_drop: bool = True


class _Chaos:
    """Runtime helper bound to a broker instance."""

    def __init__(self, cfg: ChaosConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)

    def _topic_prob(self, topic: str, table: dict[str, float]) -> float:
        for pref, p in table.items():
            if topic.startswith(pref):
                return float(p)
        return 0.0

    async def broker_jitter(self) -> None:
        if not self.cfg.enable_broker_delays:
            return
        lo, hi = self.cfg.broker_delay_range
        if hi <= 0:
            return
        await asyncio.sleep(self.rng.uniform(lo, hi))

    async def consumer_jitter(self) -> None:
        if not self.cfg.enable_consumer_delays:
            return
        lo, hi = self.cfg.consumer_poll_delay_range
        if hi <= 0:
            return
        await asyncio.sleep(self.rng.uniform(lo, hi))

    def should_duplicate(self, topic: str) -> bool:
        if not self.cfg.enable_dup_drop:
            return False
        return self.rng.random() < self._topic_prob(topic, self.cfg.dup_prob_by_topic)

    def should_drop(self, topic: str) -> bool:
        if not self.cfg.enable_dup_drop:
            return False
        return self.rng.random() < self._topic_prob(topic, self.cfg.drop_prob_by_topic)


# ───────────────────────── In-memory Kafka ─────────────────────────


class _Rec:
    __slots__ = ("topic", "value")

    def __init__(self, value: Any, topic: str) -> None:
        self.value = value
        self.topic = topic


class InMemKafkaBroker:
    """
    Tiny in-memory pub/sub with per-topic consumer groups.
    Optional chaos mode can add jitter and dup/drop to mimic real networks.
    """

    def __init__(self) -> None:
        self.topics: dict[str, dict[str, asyncio.Queue]] = {}
        self.rev: dict[int, str] = {}
        self._chaos: _Chaos | None = None

    # ── chaos control ────────────────────────────────────────────
    def set_chaos(self, cfg: ChaosConfig | None) -> None:
        """Enable or disable chaos. Pass None to disable."""
        self._chaos = _Chaos(cfg) if cfg is not None else None
        LOG.debug("kafka.chaos", event="kafka.chaos", enabled=bool(self._chaos))

    @property
    def chaos(self) -> _Chaos | None:
        return self._chaos

    # ── lifecycle / plumbing ────────────────────────────────────
    def reset(self) -> None:
        """Clear all topic queues (chaos mode is preserved)."""
        self.topics.clear()
        self.rev.clear()
        LOG.debug("kafka.reset", event="kafka.reset")

    def ensure_queue(self, topic: str, group_id: str) -> asyncio.Queue:
        tg = self.topics.setdefault(topic, {})
        q = tg.get(group_id)
        if q is None:
            q = asyncio.Queue()
            tg[group_id] = q
            self.rev[id(q)] = topic
            LOG.debug(
                "kafka.group_bind",
                event="kafka.group_bind",
                topic=topic,
                group_id=group_id,
            )
        return q

    def topic_of(self, q: asyncio.Queue) -> str:
        return self.rev.get(id(q), "?")

    async def produce(self, topic: str, value: Any) -> None:
        payload = value or {}
        msg_type = payload.get("msg_type")
        pay = payload.get("payload") or {}
        kind = pay.get("kind") or pay.get("reply")

        # chaos: broker-side jitter / drop / duplication
        if self._chaos is not None:
            await self._chaos.broker_jitter()
            if self._chaos.should_drop(topic):
                LOG.debug("kafka.drop", event="kafka.drop", topic=topic, msg_type=msg_type, kind=kind)
                return
            duplicate = self._chaos.should_duplicate(topic)
        else:
            duplicate = False

        deliveries = 2 if duplicate else 1
        for i in range(deliveries):
            for q in self.topics.setdefault(topic, {}).values():
                await q.put(_Rec(value, topic))
            if duplicate and i == 0:
                LOG.debug("kafka.dup", event="kafka.dup", topic=topic)


# single broker instance
BROKER = InMemKafkaBroker()


def reset_broker() -> None:
    BROKER.reset()


def enable_chaos(cfg: ChaosConfig | None = None) -> None:
    """
    Turn chaos ON/OFF. Call with a ChaosConfig to enable, or with None to disable.
    """
    BROKER.set_chaos(cfg)


# ───────────────────────── aiokafka mocks ─────────────────────────


class AIOKafkaProducerMock:
    def __init__(self, *_, **__) -> None:
        pass

    async def start(self) -> None:
        LOG.debug("producer.start", event="producer.start")

    async def stop(self) -> None:
        LOG.debug("producer.stop", event="producer.stop")

    async def send_and_wait(self, topic: str, value: Any, key: bytes | None = None) -> None:
        await BROKER.produce(topic, value)


class AIOKafkaConsumerMock:
    def __init__(
        self,
        *topics: str,
        bootstrap_servers: str | None = None,
        group_id: str | None = None,
        value_deserializer=None,
        enable_auto_commit: bool = False,
        auto_offset_reset: str = "latest",
    ) -> None:
        self._topics = list(topics)
        self._group = group_id or "default"
        self._deser = value_deserializer
        self._queues: list[asyncio.Queue] = []
        self._paused = False

    async def start(self) -> None:
        self._queues = [BROKER.ensure_queue(t, self._group) for t in self._topics]
        LOG.debug("consumer.start", event="consumer.start", group_id=self._group, topics=self._topics)

    async def stop(self) -> None:
        LOG.debug("consumer.stop", event="consumer.stop", group_id=self._group)
        for t in self._topics:
            tg = BROKER.topics.get(t)
            if tg:
                tg.pop(self._group, None)

    async def getone(self):
        while True:
            if self._paused:
                await asyncio.sleep(0.01)
                continue

            for q in self._queues:
                try:
                    rec = q.get_nowait()
                    val = rec.value or {}
                    msg_type = val.get("msg_type")
                    pay = val.get("payload") or {}
                    kind = pay.get("kind") or pay.get("reply")

                    with log_context(group_id=self._group, topic=BROKER.topic_of(q)):
                        LOG.debug("consumer.get", event="consumer.get", msg_type=msg_type, kind=kind)

                    # chaos: consumer-side jitter
                    if BROKER.chaos is not None:
                        await BROKER.chaos.consumer_jitter()
                    return rec
                except asyncio.QueueEmpty:
                    continue

            await asyncio.sleep(0.003)

    async def commit(self) -> None:
        return

    def pause(self, *parts) -> None:
        self._paused = True
        LOG.debug("consumer.pause", event="consumer.pause", group_id=self._group)

    def resume(self, *parts) -> None:
        self._paused = False
        LOG.debug("consumer.resume", event="consumer.resume", group_id=self._group)

    def assignment(self):
        return {("t", 0)}
