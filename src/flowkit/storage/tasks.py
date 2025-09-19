# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Task store interface (DB-agnostic).

Responsibilities:
- Persist TaskDoc (graph slices, params, execution plan).
- Manage leases (worker_id/lease_id/deadline).
- Track run state and progress snapshots.
- Update task-level vars (coordinator-controlled).

This intentionally does not model *all* coordinator data structures; it is the
stable minimum that both coordinator and worker depend on.
"""

from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Optional, Protocol, runtime_checkable

from ..protocol.messages import RunState


__all__ = [
    "TaskStoreError",
    "LeaseInfo",
    "TaskDoc",
    "TaskStore",
]


class TaskStoreError(RuntimeError):
    """Base error for task store operations."""


@dataclass(frozen=True)
class LeaseInfo:
    """
    Task lease granted to a worker.

    Attributes:
        worker_id: Worker that owns the lease.
        lease_id: Opaque token that must be presented to renew/release.
        deadline_ms: Epoch milliseconds when the lease expires.
    """
    worker_id: str
    lease_id: str
    deadline_ms: int


@dataclass
class TaskDoc:
    """
    Minimal task document persisted in the store.

    Attributes:
        task_id: Stable identifier.
        created_ms: Creation timestamp (epoch ms).
        state: Current run state (queued/running/…).
        attempt_epoch: Monotonic attempt counter incremented on retries/resume.
        worker_id: Worker currently owning the lease (if any).
        lease_id: Opaque lease token.
        lease_deadline_ms: Lease expiry timestamp.
        params: Runtime parameters (visible to expressions).
        vars: Coordinator-managed mutable store (small, low-cardinality).
        execution_plan: Compiled plan (subset used by coordinator; workers receive baked snippets).
        runtime_graph: Minimal runtime graph slice (for coordinator).
        progress: Implementation-defined progress snapshot (small).
        artifacts: Optional references to final artifacts summary.
        meta: Free-form metadata (owner, tags, etc).
    """
    task_id: str
    created_ms: int
    state: RunState = RunState.queued
    attempt_epoch: int = 0

    worker_id: str | None = None
    lease_id: str | None = None
    lease_deadline_ms: int | None = None

    params: dict[str, Any] = field(default_factory=dict)
    vars: dict[str, Any] = field(default_factory=dict)

    execution_plan: dict[str, Any] | None = None
    runtime_graph: dict[str, Any] | None = None

    progress: dict[str, Any] | None = None
    artifacts: dict[str, Any] | None = None

    meta: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class TaskStore(Protocol):
    """
    Async CRUD + leasing for TaskDoc.

    Notes:
        - All methods MUST be idempotent where sensible (e.g., create with same doc).
        - Lease methods MUST ensure only one active lease per task.
        - State transitions SHOULD be validated by the coordinator; the store
          MAY perform lightweight guards (e.g., cannot set running without a lease).
    """

    # ---- CRUD ----

    async def create(self, doc: TaskDoc) -> None: ...
    async def get(self, task_id: str) -> TaskDoc | None: ...
    async def update_fields(self, task_id: str, patch: Mapping[str, Any]) -> TaskDoc:
        """
        Apply a shallow patch to the document (atomic where supported).
        Return the updated document.
        """ ...
    async def delete(self, task_id: str) -> None: ...

    # ---- Vars (coordinator-managed) ----

    async def update_vars(self, task_id: str, ops: list[Mapping[str, Any]]) -> Mapping[str, Any]:
        """
        Apply a list of var operations (set/merge/incr) atomically and return the resulting vars map.

        Each operation:
            {"op":"set","key":"k","value":...} |
            {"op":"merge","key":"k","value":{...}} |
            {"op":"incr","key":"k","by":1}

        The store MAY validate types minimally; the coordinator should pre-validate.
        """ ...

    # ---- State / progress ----

    async def set_state(self, task_id: str, state: RunState, *, reason: str | None = None) -> None: ...
    async def update_progress(self, task_id: str, progress: Mapping[str, Any]) -> None: ...

    # ---- Leasing ----

    async def acquire_lease(self, task_id: str, *, worker_id: str, ttl_ms: int) -> LeaseInfo | None: ...
    async def renew_lease(self, task_id: str, *, worker_id: str, lease_id: str, ttl_ms: int) -> LeaseInfo | None: ...
    async def release_lease(self, task_id: str, *, worker_id: str, lease_id: str) -> bool: ...

    # ---- Query helpers (optional) ----

    async def find(
        self,
        *,
        state: RunState | None = None,
        owner: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[list[TaskDoc], str | None]:
        """
        Optional: list tasks by simple filters with paging.
        Return (items, next_cursor).
        """ ...
