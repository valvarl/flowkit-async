# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
At-least-once outbox dispatcher (storage-agnostic).

This module provides a durable egress component:
  - pulls ready records from `OutboxStore`,
  - sends envelopes to a `Bus`,
  - marks sent or schedules retry with exponential backoff + jitter.

It is intentionally independent from Coordinator — other components can reuse it.
"""

import asyncio
from dataclasses import dataclass
from typing import Protocol

from ...core.utils import jitter_ms
from ...protocol.messages import Envelope
from ...transport.bus import Bus

# ---- store protocol & record -------------------------------------------------


@dataclass(frozen=True)
class OutboxRecord:
    rec_id: str
    topic: str
    key: bytes | None
    envelope: dict  # JSON-serializable dict form of Envelope
    attempts: int
    next_attempt_at_ms: int


class OutboxStore(Protocol):
    """
    Minimal persistence protocol for the outbox.

    Implementations MUST be idempotent and safe under concurrent dispatchers.
    """

    async def enqueue(self, *, topic: str, key: bytes | None, envelope: dict, fp: str | None = None) -> None: ...
    async def next_batch(self, *, now_ms: int, limit: int) -> list[OutboxRecord]: ...
    async def mark_sent(self, rec_id: str) -> None: ...
    async def schedule_retry(
        self, rec_id: str, *, attempts: int, next_attempt_at_ms: int, error: str | None
    ) -> None: ...
    async def mark_failed(self, rec_id: str, *, attempts: int, error: str | None) -> None: ...


# ---- dispatcher --------------------------------------------------------------


@dataclass
class OutboxConfig:
    dispatch_tick_ms: int = 250
    max_batch: int = 200
    max_retry: int = 12
    backoff_min_ms: int = 250
    backoff_max_ms: int = 60_000
    jitter_pct: float = 0.20


class OutboxDispatcher:
    """
    Storage-agnostic outbox dispatcher.

    Call `start()` to begin periodic dispatch; `stop()` cancels the loop.
    You may also call `dispatch_once()` in tests to process a single batch.
    """

    def __init__(self, *, store: OutboxStore, bus: Bus, cfg: OutboxConfig | None = None) -> None:
        self.store = store
        self.bus = bus
        self.cfg = cfg or OutboxConfig()
        self._task: asyncio.Task | None = None
        self._running = False

    # lifecycle ----------------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
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

    # API ----------------------------------------------------------------------

    async def enqueue(self, *, topic: str, key: str | bytes | None, env: Envelope, fp: str | None = None) -> None:
        """Insert a record into the durable outbox (idempotent if `fp` provided)."""
        key_bytes = key if isinstance(key, (bytes, bytearray)) else (key.encode("utf-8") if key else None)
        await self.store.enqueue(topic=topic, key=key_bytes, envelope=env.model_dump(mode="json"), fp=fp)

    async def dispatch_once(self, *, now_ms: int) -> int:
        """Process up to `cfg.max_batch` records. Returns the number of processed items."""
        processed = 0
        for ob in await self.store.next_batch(now_ms=now_ms, limit=self.cfg.max_batch):
            await self._process_one(ob)
            processed += 1
        return processed

    # internals ----------------------------------------------------------------

    async def _loop(self) -> None:
        try:
            while self._running:
                n = await self.dispatch_once(now_ms=_now_ms())
                if n == 0:
                    await asyncio.sleep(self.cfg.dispatch_tick_ms / 1000.0)
        except asyncio.CancelledError:
            return

    async def _process_one(self, ob: OutboxRecord) -> None:
        try:
            env = Envelope.model_validate(ob.envelope)
            await self.bus.send(ob.topic, ob.key, env)
            await self.store.mark_sent(ob.rec_id)
        except Exception as e:
            await self._on_send_fail(ob, str(e))

    async def _on_send_fail(self, ob: OutboxRecord, err: str | None) -> None:
        attempts = ob.attempts + 1
        if attempts >= self.cfg.max_retry:
            await self.store.mark_failed(ob.rec_id, attempts=attempts, error=err)
            return
        # exponential backoff with jitter; base grows up to backoff_max_ms
        base = min(self.cfg.backoff_max_ms, max(self.cfg.backoff_min_ms, (2**attempts) * 100))
        next_at = _now_ms() + jitter_ms(base, pct=self.cfg.jitter_pct, floor_ms=self.cfg.backoff_min_ms)
        await self.store.schedule_retry(ob.rec_id, attempts=attempts, next_attempt_at_ms=next_at, error=err)


# ---- helpers -----------------------------------------------------------------


def _now_ms() -> int:
    # Simple helper avoiding extra imports — SystemClock is overkill here.
    import time

    return time.time_ns() // 1_000_000
