# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Task store interface (DB-agnostic) with per-node lifecycle.

Responsibilities:
- Persist TaskDoc (plan/runtime slices, params, minimal status).
- Manage per-node run state, attempt epochs and worker leases.
- Track task-level liveness (last_event_ms) for heartbeat monitors.

This module intentionally models a small, stable contract used by the Coordinator.
It does NOT prescribe the full internal schema of a storage backend.
"""

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from flowkit.protocol.messages import RunState

from .base.errors import StorageError

__all__ = [
    "LeaseInfo",
    "NodeSnapshot",
    "TaskDoc",
    "TaskStore",
    "TaskStoreError",
]


class TaskStoreError(StorageError):
    """Base error for task store operations."""


@dataclass(frozen=True)
class LeaseInfo:
    """
    Worker lease info.

    Attributes:
        worker_id: Worker that currently owns the lease.
        lease_id: Opaque token the worker must present to renew/release.
        deadline_ms: Epoch milliseconds when the lease expires.
    """

    worker_id: str
    lease_id: str
    deadline_ms: int


@dataclass
class NodeSnapshot:
    """
    Minimal per-node state returned to the Coordinator.

    Attributes:
        task_id: Parent task id.
        node_id: Node identifier within the task.
        type: Worker role/type used for dispatch routing.
        parents: Static list of parent node ids.
        io: Minimal runtime IO slice (used for readiness gates on coordinator).
        concurrency: Concurrency policy slice (priority/key/limit).
        status: Current run state (queued/running/deferred/cancelling/finished/failed) as string.
        attempt_epoch: Monotonic attempt counter; incremented on every "begin_node".
        lease: Current worker lease (if any).
    """

    task_id: str
    node_id: str
    type: str
    parents: list[str] = field(default_factory=list)
    io: dict[str, Any] | None = None
    concurrency: dict[str, Any] | None = None
    status: str = RunState.queued.value
    attempt_epoch: int = 0
    lease: LeaseInfo | None = None


@dataclass
class TaskDoc:
    """
    Minimal task document persisted in the store.

    Coordinator expects these fields on reads:
        task_id, schema_version, params, plan, runtime, status

    Backends MAY store additional bookkeeping fields (created_ms, last_event_ms, etc.).
    """

    task_id: str
    schema_version: str
    params: dict[str, Any] = field(default_factory=dict)
    plan: dict[str, Any] = field(default_factory=dict)
    runtime: dict[str, Any] = field(default_factory=dict)
    status: str = RunState.queued.value

    # Optional, used by backends and monitors
    created_ms: int = 0
    last_event_ms: int = 0
    meta: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class TaskStore(Protocol):
    """
    Async task and per-node lifecycle API expected by the Coordinator.
    """

    # ---- Task CRUD ----

    async def create(self, doc: TaskDoc) -> None: ...
    async def get(self, task_id: str) -> TaskDoc | None: ...
    async def update_fields(self, task_id: str, patch: Mapping[str, Any]) -> TaskDoc: ...
    async def delete(self, task_id: str) -> None: ...

    # ---- Task scanning / liveness ----

    async def iter_active(self) -> AsyncIterator[TaskDoc]:
        """
        Yield tasks in non-terminal states (i.e., not 'finished' and not 'failed').
        Ordering is backend-defined but SHOULD be stable (e.g., by _id asc).
        """
        ...

    async def touch_event(self, task_id: str) -> None:
        """
        Update task's 'last_event_ms' to current time. Called on every worker event
        so the heartbeat monitor can detect liveness issues.
        """
        ...

    async def ms_since_last_event(self, task_id: str) -> int:
        """
        Return milliseconds elapsed since the last event was observed for the task.
        If unknown, return a large value to trigger soft/hard timeouts.
        """
        ...

    # ---- High-level task transitions ----

    async def bump_running(self, task_id: str) -> None:
        """
        Idempotently set task.status='running' if it is currently 'queued'.
        No-op for other states.
        """
        ...

    async def is_task_complete(self, task_id: str) -> bool:
        """
        Return True if there are no nodes in non-terminal states.
        """
        ...

    async def finish_task(self, task_id: str) -> None: ...
    async def fail_task(self, task_id: str, *, reason: str) -> None: ...
    async def defer_task(self, task_id: str, *, reason: str) -> None: ...

    # ---- Node listing / lookup ----

    async def iter_task_nodes(self, task_id: str) -> AsyncIterator[NodeSnapshot]: ...
    async def get_node(self, task_id: str, node_id: str) -> NodeSnapshot | None: ...

    # ---- Node lifecycle ----

    async def begin_node(self, task_id: str, node_id: str) -> tuple[bool, int, LeaseInfo | None]:
        """
        Attempt to transition a node to 'running'.
        Returns (begin_ok, new_epoch, lease_info_or_none).
        Implementations MUST ensure this is atomic and single-writer (CAS).
        """
        ...

    async def finish_node(self, task_id: str, node_id: str) -> None: ...
    async def defer_node(self, task_id: str, node_id: str, *, reason: str) -> None: ...
    async def fail_node(self, task_id: str, node_id: str, *, reason: str) -> None: ...
    async def mark_cancelling(self, task_id: str, node_id: str, *, reason: str) -> None: ...

    async def update_lease(
        self,
        *,
        task_id: str,
        node_id: str,
        worker_id: str,
        lease_id: str,
        deadline_ts_ms: int,
    ) -> None:
        """
        Update current worker lease on the node. Backends SHOULD set the embedded
        'lease' object and MAY also update a simple 'lease_deadline_ms' field.
        """
        ...
