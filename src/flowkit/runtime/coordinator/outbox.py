# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
At-least-once outbox dispatcher — storage-agnostic.

This is the coordinator's durable egress: it reads pending/retry records from
an OutboxStore, sends them to the Bus, and performs state transitions
(sent/failed/retry) with exponential backoff and jitter.

See: flowkit/storage/outbox.py for the minimal OutboxStore protocol.
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Optional

from ...protocol.messages import Envelope
from ...transport.bus import Bus
from ...storage.outbox import OutboxStore, OutboxRecord


@dataclass
class OutboxConfig:
    dispatch_tick_ms: int = 250
    max_batch: int = 200
    max_retry: int = 12
    backoff_min_ms: int = 250
    backoff_max_ms: int = 60_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _jitter_ms(ms: int) -> int:
    if ms <= 0:
        return 0
    delta = int(ms * 0.2)  # ±20%
    return max(0, ms + random.randint(-delta, +delta))


class OutboxDispatcher:
    """Storage-agnostic dispatcher for the coordinator's message outbox."""

    def __init__(
        self, *, store: OutboxStore, bus: Bus, cfg: OutboxConfig | None = None, logger: Optional[logging.Logger] = None
    ) -> None:
        self.store = store
        self.bus = bus
        self.cfg = cfg or OutboxConfig()
        self.log = logger or logging.getLogger("flowkit.outbox")
        self._task: asyncio.Task | None = None
        self._running = False

    # ---- lifecycle

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="flowkit-outbox")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

    # ---- enqueue

    async def enqueue(self, *, topic: str, key: str | None, env: Envelope, fp: str | None = None) -> None:
        """Insert a new outbox record (idempotent if fingerprint is provided)."""
        await self.store.enqueue(topic=topic, key=key, envelope=env.model_dump(mode="json"), fp=fp)

    # ---- loop

    async def _loop(self) -> None:
        try:
            while self._running:
                any_sent = False
                async for ob in self.store.next_batch(now_ms=_now_ms(), limit=self.cfg.max_batch):
                    await self._process_one(ob)
                    any_sent = True
                await asyncio.sleep(0 if any_sent else self.cfg.dispatch_tick_ms / 1000.0)
        except asyncio.CancelledError:
            return

    async def _process_one(self, ob: OutboxRecord) -> None:
        try:
            env = Envelope.model_validate(ob.envelope)
            key_bytes = (ob.key or "").encode("utf-8") or None
            await self.bus.send(ob.topic, key_bytes, env)
            await self.store.mark_sent(ob.rec_id)
        except Exception as e:  # noqa: BLE001
            await self._on_send_fail(ob, e)

    async def _on_send_fail(self, ob: OutboxRecord, err: Exception) -> None:
        attempts = ob.attempts + 1
        if attempts >= self.cfg.max_retry:
            await self.store.mark_failed(ob.rec_id, attempts=attempts, error=str(err))
            return
        base = min(self.cfg.backoff_max_ms, max(self.cfg.backoff_min_ms, (2**attempts) * 100))
        await self.store.schedule_retry(
            ob.rec_id,
            attempts=attempts,
            next_attempt_at_ms=_now_ms() + _jitter_ms(base),
            error=str(err),
        )
