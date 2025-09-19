# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Optional


TaskFn = Callable[[], Awaitable[None]]


@dataclass
class _Lane:
    limit: int
    sem: asyncio.Semaphore = field(init=False)

    def __post_init__(self) -> None:
        self.sem = asyncio.Semaphore(self.limit)


class WorkerPool:
    """
    Local worker pool with per-role concurrency and fair scheduling.

    Usage:
      pool = WorkerPool(default_limit=4, per_role={"extract": 2, "load": 4})
      await pool.start()
      pool.submit("extract", my_coro)
      ...
      await pool.stop()

    Notes:
      - Fairness: role queues are served in round-robin order.
      - Backpressure: submit() awaits until the role has capacity.
    """

    def __init__(self, *, default_limit: int = 4, per_role: Optional[Dict[str, int]] = None) -> None:
        self._default = default_limit
        self._per_role: Dict[str, _Lane] = {}
        for r, lim in (per_role or {}).items():
            self._per_role[r] = _Lane(limit=max(1, int(lim)))
        self._queues: Dict[str, asyncio.Queue[TaskFn]] = {}
        self._loop_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        self._loop_task = None

    async def submit(self, role: str, fn: TaskFn) -> None:
        q = self._queues.setdefault(role, asyncio.Queue())
        await q.put(fn)

    # ---- internal

    def _lane(self, role: str) -> _Lane:
        ln = self._per_role.get(role)
        if ln is None:
            ln = _Lane(limit=self._default)
            self._per_role[role] = ln
        return ln

    async def _loop(self) -> None:
        roles: list[str] = list(self._queues.keys())
        idx = 0
        try:
            while self._running:
                if not self._queues:
                    await asyncio.sleep(0.01)
                    continue
                if idx >= len(roles):
                    roles = list(self._queues.keys())
                    idx = 0
                    if not roles:
                        await asyncio.sleep(0.01)
                        continue

                role = roles[idx]
                idx += 1

                q = self._queues.get(role)
                if not q:
                    continue
                if q.empty():
                    await asyncio.sleep(0)  # yield
                    continue

                lane = self._lane(role)
                await lane.sem.acquire()
                fn = await q.get()

                async def _run():
                    try:
                        await fn()
                    finally:
                        lane.sem.release()

                asyncio.create_task(_run())
        except asyncio.CancelledError:
            return
