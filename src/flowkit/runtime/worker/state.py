# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LeaseState:
    worker_id: str | None = None
    lease_id: str | None = None
    deadline_ts_ms: int | None = None


@dataclass
class ActiveRun:
    """
    Volatile worker-side state of the current run.

    Values are refreshed from the last worker snapshot or coordinator commands.
    """

    task_id: str
    node_id: str
    attempt_epoch: int
    lease: LeaseState = field(default_factory=LeaseState)
    cancel_requested: bool = False
    cancel_deadline_ts_ms: int | None = None
    started_at_ms: int | None = None

    def adopt(self, snap: Mapping[str, Any]) -> None:
        """Adopt fields from a worker snapshot (best-effort)."""
        l = snap.get("lease") or {}
        self.lease.worker_id = l.get("worker_id") or self.lease.worker_id
        self.lease.lease_id = l.get("lease_id") or self.lease.lease_id
        self.lease.deadline_ts_ms = l.get("deadline_ts_ms") or self.lease.deadline_ts_ms
        if "attempt_epoch" in snap:
            self.attempt_epoch = int(snap["attempt_epoch"])

    def request_cancel(self, *, deadline_ts_ms: int | None) -> None:
        self.cancel_requested = True
        self.cancel_deadline_ts_ms = deadline_ts_ms
