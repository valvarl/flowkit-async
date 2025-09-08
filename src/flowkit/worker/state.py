from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from typing import Any

from ..core.time import Clock


@dataclass
class ActiveRun:
    task_id: str
    node_id: str
    step_type: str
    attempt_epoch: int
    lease_id: str
    cancel_token: str
    started_at_ms: int
    state: str  # running|finishing|cancelling
    checkpoint: dict


class LocalStateManager:
    """DB-backed state manager for crash-resume and cross-host takeover."""

    def __init__(self, *, db, clock: Clock, worker_id: str) -> None:
        self.db = db
        self.clock = clock
        self.worker_id = worker_id
        self._lock = asyncio.Lock()
        self._cache: dict[str, Any] = {}

    async def refresh(self) -> None:
        """Pull the latest state from DB into in-memory cache."""
        doc = await self.db.worker_state.find_one({"_id": self.worker_id})
        self._cache = doc or {}

    def read_active(self) -> ActiveRun | None:
        """Read active run from cache."""
        d = (self._cache or {}).get("active_run")
        return ActiveRun(**d) if d else None

    async def write_active(self, ar: ActiveRun | None) -> None:
        """Atomically persist active run to DB and update cache."""
        async with self._lock:
            payload = {
                "active_run": asdict(ar) if ar else None,
                "updated_at": self.clock.now_dt(),  # must be datetime for TTL index
            }
            await self.db.worker_state.update_one(
                {"_id": self.worker_id},
                {"$set": payload},
                upsert=True,
            )
            self._cache.update(payload)
