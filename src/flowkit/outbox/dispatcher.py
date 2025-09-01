from __future__ import annotations
import asyncio
from typing import Optional

from ..core.config import CoordinatorConfig
from ..core.time import now_dt, now_ms
from ..core.utils import jitter_ms, stable_hash
from ..protocol.messages import Envelope
from ..bus.kafka import KafkaBus


class OutboxDispatcher:
    """
    At-least-once outbox + idempotent producer. Uses app-provided `db`.
    """
    def __init__(self, *, db, bus: KafkaBus, cfg: CoordinatorConfig) -> None:
        self.db = db
        self.bus = bus
        self.cfg = cfg
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    async def _loop(self) -> None:
        try:
            while self._running:
                now_ms_ = now_ms()
                cur = self.db.outbox.find(
                    {"state": {"$in": ["pending", "retry"]}, "next_attempt_at_ms": {"$lte": now_ms_}},
                    {"_id": 1, "fp": 1, "topic": 1, "key": 1, "envelope": 1, "attempts": 1}
                ).sort([("next_attempt_at_ms", 1)]).limit(200)
                any_sent = False
                async for ob in cur:
                    try:
                        env = Envelope.model_validate(ob["envelope"])
                        await self.bus._raw_send(ob["topic"], ob["key"].encode("utf-8"), env)
                        _flt = {"_id": ob["_id"]} if ob.get("_id") is not None else {"fp": ob.get("fp")}
                        await self.db.outbox.update_one(_flt, {"$set": {"state": "sent", "sent_at": now_dt(), "updated_at": now_dt()}})
                        any_sent = True
                    except Exception as e:
                        attempts = int(ob.get("attempts", 0)) + 1
                        if attempts >= self.cfg.outbox_max_retry:
                            _flt = {"_id": ob.get("_id")} if ob.get("_id") is not None else {"fp": ob.get("fp")}
                            await self.db.outbox.update_one(_flt, {"$set": {"state": "failed", "last_error": str(e), "updated_at": now_dt()}})
                        else:
                            backoff_ms = min(self.cfg.outbox_backoff_max_ms,
                                             max(self.cfg.outbox_backoff_min_ms, (2 ** attempts) * 100))
                            backoff_ms = jitter_ms(backoff_ms)
                            _flt = {"_id": ob.get("_id")} if ob.get("_id") is not None else {"fp": ob.get("fp")}
                            await self.db.outbox.update_one(_flt, {"$set": {
                                "state": "retry",
                                "attempts": attempts,
                                "last_error": str(e),
                                "next_attempt_at_ms": now_ms_ + backoff_ms,
                                "updated_at": now_dt()
                            }})
                await asyncio.sleep(0 if any_sent else (self.cfg.outbox_dispatch_tick_ms / 1000.0))
        except asyncio.CancelledError:
            return

    async def enqueue(self, *, topic: str, key: str, env: Envelope) -> None:
        fp = stable_hash({"topic": topic, "key": key, "dedup_id": env.dedup_id})
        doc = {
            "fp": fp,
            "topic": topic,
            "key": key,
            "envelope": env.model_dump(mode="json"),
            "state": "pending",
            "attempts": 0,
            "next_attempt_at_ms": now_ms(),
            "created_at": now_dt(),
            "updated_at": now_dt(),
        }
        try:
            await self.db.outbox.insert_one(doc)
        except Exception:
            # duplicate fp is ok (idempotent)
            pass
