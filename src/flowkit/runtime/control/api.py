# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Minimal in-process control API for smoke tests.

This is intentionally tiny: just a health check and a "poke" to ensure the
process loop is alive (returns an incrementing counter).
"""

import asyncio
from dataclasses import dataclass, field


@dataclass
class ControlAPI:
    _counter: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def health(self) -> dict[str, str]:
        return {"status": "ok"}

    async def poke(self) -> dict[str, int]:
        async with self._lock:
            self._counter += 1
            return {"poke": self._counter}
