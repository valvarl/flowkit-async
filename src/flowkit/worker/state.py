from __future__ import annotations
import asyncio
import json
import os
from dataclasses import dataclass, asdict
from typing import Any, Optional

from ..core.time import Clock
from ..core.config import WorkerConfig


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


class LocalState:
    """Lightweight JSON persistence for resume-after-crash."""
    def __init__(self, cfg: WorkerConfig, clock: Clock) -> None:
        self.cfg = cfg
        self.clock = clock
        os.makedirs(cfg.state_dir, exist_ok=True)
        wid = cfg.worker_id or "worker"
        self._fp = os.path.join(cfg.state_dir, f"{wid}.json")
        self._lock = asyncio.Lock()
        self.data: dict[str, Any] = {}
        try:
            if os.path.exists(self._fp):
                with open(self._fp, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
        except Exception:
            self.data = {}

    async def write_active(self, ar: Optional[ActiveRun]) -> None:
        async with self._lock:
            if ar is None:
                self.data.pop("active_run", None)
            else:
                self.data["active_run"] = asdict(ar)
            self.data["updated_at_ms"] = self.clock.now_ms()
            tmp = self._fp + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, self._fp)

    def read_active(self) -> Optional[ActiveRun]:
        d = self.data.get("active_run")
        if not d:
            return None
        return ActiveRun(**d)
