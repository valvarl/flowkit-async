# tests/helpers/kafka.py
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .util import dbg


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
    broker_delay_range: Tuple[float, float] = (0.0, 0.003)      # seconds
    consumer_poll_delay_range: Tuple[float, float] = (0.0, 0.002)
    dup_prob_by_topic: Dict[str, float] = None                  # e.g. {"status.": 0.06}
    drop_prob_by_topic: Dict[str, float] = None                 # e.g. {"cmd.": 0.01}
    enable_broker_delays: bool = True
    enable_consumer_delays: bool = True
    enable_dup_drop: bool = True

    def __post_init__(self) -> None:
        if self.dup_prob_by_topic is None:
            self.dup_prob_by_topic = {}
        if self.drop_prob_by_topic is None:
            self.drop_prob_by_topic = {}


class _Chaos:
    """Runtime helper bound to a broker instance."""
    def __init__(self, cfg: ChaosConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)

    def _topic_prob(self, topic: str, table: Dict[str, float]) -> float:
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
    __slots__ = ("value", "topic")
    def __init__(self, value: Any, topic: str) -> None:
        self.value = value
        self.topic = topic


class InMemKafkaBroker:
    """
    Tiny in-memory pub/sub with per-topic consumer groups.
    Optional chaos mode can add jitter and dup/drop to mimic real networks.
    """
    def __init__(self) -> None:
        self.topics: Dict[str, Dict[str, asyncio.Queue]] = {}
        self.rev: Dict[int, str] = {}
        self._chaos: Optional[_Chaos] = None

    # ── chaos control ────────────────────────────────────────────
    def set_chaos(self, cfg: Optional[ChaosConfig]) -> None:
        """Enable or disable chaos. Pass None to disable."""
        self._chaos = _Chaos(cfg) if cfg is not None else None
        dbg("KAFKA.CHAOS", enabled=bool(self._chaos))

    @property
    def chaos(self) -> Optional[_Chaos]:
        return self._chaos

    # ── lifecycle / plumbing ────────────────────────────────────
    def reset(self) -> None:
        """Clear all topic queues (chaos mode is preserved)."""
        self.topics.clear()
        self.rev.clear()
        dbg("KAFKA.RESET")

    def ensure_queue(self, topic: str, group_id: str) -> asyncio.Queue:
        tg = self.topics.setdefault(topic, {})
        q = tg.get(group_id)
        if q is None:
            q = asyncio.Queue()
            tg[group_id] = q
            self.rev[id(q)] = topic
            dbg("KAFKA.GROUP_BIND", topic=topic, group_id=group_id)
        return q

    def topic_of(self, q: asyncio.Queue) -> str:
        return self.rev.get(id(q), "?")

    async def produce(self, topic: str, value: Any) -> None:
        payload = value or {}
        msg_type = payload.get("msg_type")
        kind = (payload.get("payload") or {}).get("kind") or (payload.get("payload") or {}).get("reply")

        # chaos: broker-side jitter / drop / duplication
        if self._chaos is not None:
            await self._chaos.broker_jitter()
            if self._chaos.should_drop(topic):
                dbg("KAFKA.DROP", topic=topic, msg_type=msg_type, kind=kind)
                return
            duplicate = self._chaos.should_duplicate(topic)
        else:
            duplicate = False

        dbg("KAFKA.PRODUCE", topic=topic, msg_type=msg_type, kind=kind)

        deliveries = 2 if duplicate else 1
        for i in range(deliveries):
            for q in self.topics.setdefault(topic, {}).values():
                await q.put(_Rec(value, topic))
            if duplicate and i == 0:
                dbg("KAFKA.DUP", topic=topic)


# single broker instance
BROKER = InMemKafkaBroker()


def reset_broker() -> None:
    BROKER.reset()


def enable_chaos(cfg: Optional[ChaosConfig] = None) -> None:
    """
    Turn chaos ON/OFF. Call with a ChaosConfig to enable, or with None to disable.
    """
    BROKER.set_chaos(cfg)


# ───────────────────────── aiokafka mocks ─────────────────────────

class AIOKafkaProducerMock:
    def __init__(self, *_, **__) -> None:
        pass

    async def start(self) -> None:
        dbg("PRODUCER.START")

    async def stop(self) -> None:
        dbg("PRODUCER.STOP")

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
        self._queues: List[asyncio.Queue] = []
        self._paused = False

    async def start(self) -> None:
        self._queues = [BROKER.ensure_queue(t, self._group) for t in self._topics]
        dbg("CONSUMER.START", group_id=self._group, topics=self._topics)

    async def stop(self) -> None:
        dbg("CONSUMER.STOP", group_id=self._group)
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
                    kind = (val.get("payload") or {}).get("kind") or (val.get("payload") or {}).get("reply")
                    dbg("CONSUMER.GET", group_id=self._group, topic=BROKER.topic_of(q), msg_type=msg_type, kind=kind)

                    # chaos: consumer-side jitter
                    if BROKER.chaos is not None:
                        await BROKER.chaos.consumer_jitter()
                    return rec
                except asyncio.QueueEmpty:
                    continue

            await asyncio.sleep(0.003)

    async def commit(self) -> None:
        pass

    def pause(self, *parts) -> None:
        self._paused = True
        dbg("CONSUMER.PAUSE", group_id=self._group)

    def resume(self, *parts) -> None:
        self._paused = False
        dbg("CONSUMER.RESUME", group_id=self._group)

    def assignment(self):
        return {("t", 0)}
