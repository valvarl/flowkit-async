# src/flowkit/protocol/messages.py
from __future__ import annotations

"""
FlowKit protocol messages (v2)
==============================

This module defines **wire-level** envelopes and payloads exchanged between
Coordinator and Workers via the bus (e.g., Kafka). The protocol is intentionally
minimal and stable; business-heavy structures (full graph/spec) are NOT sent
over the wire.

Design principles:
- Clear separation between the **Envelope** (routing/metadata) and **payload**.
- Typed payload models for commands/events/queries/replies.
- Pydantic v2 models with `extra="forbid"` to fail fast on unknown fields.
- All timestamps are **epoch milliseconds** (UTC).
- `Envelope.v` denotes the **protocol schema version** (here `2`).
"""

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #


class MsgType(str, Enum):
    """Top-level message kind on the wire."""

    cmd = "cmd"
    event = "event"
    heartbeat = "heartbeat"
    query = "query"
    reply = "reply"


class Role(str, Enum):
    """Originator role of the message, used for diagnostics/routing."""

    coordinator = "coordinator"
    worker = "worker"


class RunState(str, Enum):
    """High-level task/node run-state transitions."""

    queued = "queued"
    running = "running"
    deferred = "deferred"
    cancelling = "cancelling"
    finished = "finished"
    failed = "failed"


class EventKind(str, Enum):
    """Domain events emitted by workers (and some by coordinator)."""

    WORKER_ONLINE = "WORKER_ONLINE"
    WORKER_OFFLINE = "WORKER_OFFLINE"

    TASK_ACCEPTED = "TASK_ACCEPTED"
    TASK_HEARTBEAT = "TASK_HEARTBEAT"

    BATCH_OK = "BATCH_OK"
    BATCH_FAILED = "BATCH_FAILED"

    TASK_RESUMED = "TASK_RESUMED"
    TASK_DONE = "TASK_DONE"
    TASK_FAILED = "TASK_FAILED"
    CANCELLED = "CANCELLED"


class CommandKind(str, Enum):
    """Commands sent by coordinator to workers."""

    TASK_START = "TASK_START"
    TASK_CANCEL = "TASK_CANCEL"
    TASK_PAUSE = "TASK_PAUSE"
    TASK_RESUME = "TASK_RESUME"


class QueryKind(str, Enum):
    """Point-in-time queries over worker state."""

    TASK_DISCOVER = "TASK_DISCOVER"


class ReplyKind(str, Enum):
    """Replies to queries."""

    TASK_SNAPSHOT = "TASK_SNAPSHOT"


# --------------------------------------------------------------------------- #
# Envelope (wire header)
# --------------------------------------------------------------------------- #


class Envelope(BaseModel):
    """
    Transport envelope carrying routing metadata and a payload.

    Fields:
        v: Protocol schema version. **Must be `2`** for this module.
        msg_type: Message high-level category.
        role: Originator role (coordinator/worker).
        corr_id: Correlation ID to match requests/replies (UUID v4 as str).
        dedup_id: Producer-provided id used for at-least-once de-duplication.
        task_id: Logical task identifier (graph run).
        node_id: Node identifier within the task.
        step_type: Worker role/type that should process the command/event stream.
        attempt_epoch: Monotonic attempt index for restart/resume logic.
        ts_ms: Creation timestamp (epoch milliseconds, UTC).
        payload: Message payload; its structure must match the corresponding
                 typed model for the given `msg_type`.
        target_worker_id: Optional direct addressing (sticky routing, resumes).
    """

    model_config = ConfigDict(extra="forbid")

    v: int = Field(default=2)
    msg_type: MsgType
    role: Role

    corr_id: str = Field(default_factory=lambda: __import__("uuid").uuid4().hex)
    dedup_id: str

    task_id: str
    node_id: str
    step_type: str
    attempt_epoch: int
    ts_ms: int

    payload: dict[str, Any] = Field(default_factory=dict)
    target_worker_id: str | None = None


# --------------------------------------------------------------------------- #
# Commands (Coordinator -> Worker)
# --------------------------------------------------------------------------- #


class CmdTaskStart(BaseModel):
    """
    Start a node run on a worker.

    The coordinator bakes input/output plans into `input_inline`/`batching`
    so that workers do not need to understand the full graph spec.

    Fields:
        cmd: must be CommandKind.TASK_START
        input_ref: optional reference for worker-side lazy loading (rare).
        input_inline: compiled input plan slice (ports/merge/sources/ops).
        batching: optional batching hints/config for the handler.
        output_plan: compiled output plan slice (channels, routing, delivery).
        policies: normalized policy bundle (retry/sla/concurrency/resources/worker).
        cancel_token: token the coordinator uses to cancel this run.
    """

    model_config = ConfigDict(extra="forbid")

    cmd: CommandKind
    input_ref: dict[str, Any] | None = None
    input_inline: dict[str, Any] | None = None
    batching: dict[str, Any] | None = None
    output_plan: dict[str, Any] | None = None
    policies: dict[str, Any] | None = None
    cancel_token: str


class CmdTaskCancel(BaseModel):
    """Cooperative cancellation request."""

    model_config = ConfigDict(extra="forbid")

    cmd: CommandKind
    reason: str
    cancel_token: str


# --------------------------------------------------------------------------- #
# Events (Worker -> Coordinator)
# --------------------------------------------------------------------------- #


class EvWorkerOnline(BaseModel):
    """Worker has started and announces its capabilities."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    roles: list[str] = Field(default_factory=list)
    version: str
    capabilities: dict[str, Any] | None = None


class EvWorkerOffline(BaseModel):
    """Worker is going away (graceful shutdown or fatal error)."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    reason: str | None = None


class EvTaskAccepted(BaseModel):
    """Worker accepted a TASK_START and obtained a lease."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    lease_id: str
    lease_deadline_ts_ms: int


class EvHeartbeat(BaseModel):
    """Periodic heartbeat to keep the lease alive."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    lease_id: str
    lease_deadline_ts_ms: int


class EvBatchOk(BaseModel):
    """
    A batch has been processed successfully.

    Notes:
        - `metrics` is a small, low-cardinality dict (e.g., counts, durations).
        - `artifacts_ref` is a reference to produced artifacts (opaque for the bus).
        - `batch_uid` must be stable per batch to enable idempotent handling.
    """

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    batch_uid: str
    metrics: dict[str, Any] = Field(default_factory=dict)
    artifacts_ref: dict[str, Any] | None = None


class EvBatchFailed(BaseModel):
    """A batch has failed (may be retryable or permanent)."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    batch_uid: str
    reason_code: str
    permanent: bool = False
    error: str | None = None
    artifacts_ref: dict[str, Any] | None = None


class EvTaskDone(BaseModel):
    """The node has finished producing output (final event)."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    metrics: dict[str, Any] = Field(default_factory=dict)
    artifacts_ref: dict[str, Any] | None = None
    final_uid: str | None = None


class EvTaskFailed(BaseModel):
    """The node has failed (final event)."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    reason_code: str
    permanent: bool = False
    error: str | None = None


class EvCancelled(BaseModel):
    """Worker confirms cooperative cancellation."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    worker_id: str
    reason: str


# --------------------------------------------------------------------------- #
# Signals (Coordinator -> Worker; out-of-band)
# --------------------------------------------------------------------------- #


class SigCancel(BaseModel):
    """
    Out-of-band CANCEL signal.

    This is different from CmdTaskCancel in that it can be published to a
    dedicated "signals" topic and does not require pausing the command stream.
    """

    model_config = ConfigDict(extra="forbid")

    sig: str = "CANCEL"
    reason: str
    cancel_token: str | None = None
    deadline_ts_ms: int | None = None


# --------------------------------------------------------------------------- #
# Queries & Replies
# --------------------------------------------------------------------------- #


class QTaskDiscover(BaseModel):
    """
    Ask a worker to report whether it holds/knows a task attempt epoch.

    Used at coordinator start/resume to adopt in-flight work.
    """

    model_config = ConfigDict(extra="forbid")

    query: QueryKind
    want_epoch: int


class RTaskSnapshot(BaseModel):
    """
    Snapshot of worker-side attempt state.

    Fields:
        reply: must be ReplyKind.TASK_SNAPSHOT
        worker_id: responder identity.
        run_state: one of RunState (string form for forward-compat).
        attempt_epoch: epoch reported by worker.
        lease: optional lease info.
        progress: optional progress structure (opaque to bus).
        artifacts: optional artifacts structure (opaque to bus).
    """

    model_config = ConfigDict(extra="forbid")

    reply: ReplyKind
    worker_id: str | None = None
    run_state: str
    attempt_epoch: int
    lease: dict[str, Any] | None = None
    progress: dict[str, Any] | None = None
    artifacts: dict[str, Any] | None = None
