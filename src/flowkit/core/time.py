from __future__ import annotations

"""
flowkit.core.time
=================

Clock abstractions:
- Clock Protocol for dependency-injection and testing.
- SystemClock: production default implementation.
- ManualClock: deterministic time control for tests.
"""

import asyncio
import time
from datetime import UTC, datetime
from typing import Protocol

from .types import Millis, MonotonicMs, TimestampMs


class Clock(Protocol):
    """Minimal clock protocol used across the project."""

    def now_dt(self) -> datetime: ...
    def now_ms(self) -> TimestampMs: ...
    def mono_ms(self) -> MonotonicMs: ...
    async def sleep_ms(self, ms: Millis) -> None: ...


class SystemClock:
    """Default production/test clock backed by system time."""

    def now_dt(self) -> datetime:
        """UTC datetime for wall-clock timestamps (persistable)."""
        return datetime.now(UTC)

    def now_ms(self) -> TimestampMs:
        """Epoch milliseconds from system clock (persistable)."""
        return time.time_ns() // 1_000_000

    def mono_ms(self) -> MonotonicMs:
        """Process-local monotonic milliseconds (not related to wall clock)."""
        return time.monotonic_ns() // 1_000_000

    async def sleep_ms(self, ms: Millis) -> None:
        """Async sleep for the given number of milliseconds."""
        await asyncio.sleep(max(0.0, ms / 1000.0))


class ManualClock(SystemClock):
    """
    Controllable clock for tests.

    - Wall time (`now_ms`) starts at `start_ms` and advances only when you call `sleep_ms`.
    - Monotonic time (`mono_ms`) mirrors wall time for simplicity.
    """

    def __init__(self, start_ms: Millis = 0) -> None:
        self._wall: Millis = start_ms
        self._mono: Millis = start_ms

    def now_dt(self) -> datetime:
        return datetime.fromtimestamp(self._wall / 1000.0, tz=UTC)

    def now_ms(self) -> TimestampMs:
        return self._wall

    def mono_ms(self) -> MonotonicMs:
        return self._mono

    async def sleep_ms(self, ms: Millis) -> None:
        # In tests you can fast-forward immediately; no actual sleeping.
        inc = max(0, int(ms))
        self._wall += inc
        self._mono += inc
