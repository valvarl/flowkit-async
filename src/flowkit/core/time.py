from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now_dt(self) -> datetime: ...
    def now_ms(self) -> int: ...
    def mono_ms(self) -> int: ...
    async def sleep_ms(self, ms: int) -> None: ...


class SystemClock:
    """Default production/test clock."""

    def now_dt(self) -> datetime:
        return datetime.now(UTC)

    def now_ms(self) -> int:
        # wall clock epoch ms (persistable)
        return time.time_ns() // 1_000_000

    def mono_ms(self) -> int:
        # monotonic ms (process-local)
        return time.monotonic_ns() // 1_000_000

    async def sleep_ms(self, ms: int) -> None:
        await asyncio.sleep(max(0.0, ms / 1000.0))


class ManualClock(SystemClock):
    """
    Simple controllable clock for tests.
    - Wall time is advanced manually (affects persistence fields).
    - Monotonic time mirrors wall unless overridden.
    """

    def __init__(self, start_ms: int = 0) -> None:
        self._wall = start_ms
        self._mono = start_ms

    def now_dt(self) -> datetime:
        return datetime.fromtimestamp(self._wall / 1000.0, tz=UTC)

    def now_ms(self) -> int:
        return self._wall

    def mono_ms(self) -> int:
        return self._mono

    async def sleep_ms(self, ms: int) -> None:
        # in tests you can fast-forward immediately
        self._wall += max(0, int(ms))
        self._mono += max(0, int(ms))
