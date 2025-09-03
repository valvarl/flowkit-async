from __future__ import annotations

import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---- enums
class MsgType(str, Enum):
    cmd = "cmd"
    event = "event"
    heartbeat = "heartbeat"
    query = "query"
    reply = "reply"


class Role(str, Enum):
    coordinator = "coordinator"
    worker = "worker"


class RunState(str, Enum):
    queued = "queued"
    running = "running"
    deferred = "deferred"
    cancelling = "cancelling"
    finished = "finished"
    failed = "failed"


class EventKind(str, Enum):
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
    TASK_START = "TASK_START"
    TASK_CANCEL = "TASK_CANCEL"
    TASK_PAUSE = "TASK_PAUSE"
    TASK_RESUME = "TASK_RESUME"


class QueryKind(str, Enum):
    TASK_DISCOVER = "TASK_DISCOVER"


class ReplyKind(str, Enum):
    TASK_SNAPSHOT = "TASK_SNAPSHOT"


# ---- envelopes & payloads
class Envelope(BaseModel):
    v: int = 1
    msg_type: MsgType
    role: Role
    corr_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    dedup_id: str
    task_id: str
    node_id: str
    step_type: str
    attempt_epoch: int
    ts_ms: int
    payload: dict[str, Any] = Field(default_factory=dict)
    target_worker_id: str | None = None


class CmdTaskStart(BaseModel):
    cmd: CommandKind
    input_ref: dict[str, Any] | None = None
    input_inline: dict[str, Any] | None = None
    batching: dict[str, Any] | None = None
    cancel_token: str


class CmdTaskCancel(BaseModel):
    cmd: CommandKind
    reason: str
    cancel_token: str


class EvTaskAccepted(BaseModel):
    kind: EventKind
    worker_id: str
    lease_id: str
    lease_deadline_ts_ms: int


class EvHeartbeat(BaseModel):
    kind: EventKind
    worker_id: str
    lease_id: str
    lease_deadline_ts_ms: int


class EvBatchOk(BaseModel):
    kind: EventKind
    worker_id: str
    batch_uid: str
    metrics: dict[str, Any] = {}
    artifacts_ref: dict[str, Any] | None = None


class EvBatchFailed(BaseModel):
    kind: EventKind
    worker_id: str
    batch_uid: str
    reason_code: str
    permanent: bool = False
    error: str | None = None
    artifacts_ref: dict[str, Any] | None = None


class EvTaskDone(BaseModel):
    kind: EventKind
    worker_id: str
    metrics: dict[str, Any] = {}
    artifacts_ref: dict[str, Any] | None = None
    final_uid: str | None = None


class EvTaskFailed(BaseModel):
    kind: EventKind
    worker_id: str
    reason_code: str
    permanent: bool = False
    error: str | None = None


class EvCancelled(BaseModel):
    kind: EventKind
    worker_id: str
    reason: str


class SigCancel(BaseModel):
    sig: str = "CANCEL"
    reason: str
    cancel_token: str | None = None
    deadline_ts_ms: int | None = None


class QTaskDiscover(BaseModel):
    query: QueryKind
    want_epoch: int


class RTaskSnapshot(BaseModel):
    reply: ReplyKind
    worker_id: str | None = None
    run_state: str
    attempt_epoch: int
    lease: dict[str, Any] | None = None
    progress: dict[str, Any] | None = None
    artifacts: dict[str, Any] | None = None


# ---- DAG storage models (kept for compatibility)
class RetryPolicy(BaseModel):
    max: int = 2
    backoff_sec: int = 300
    permanent_on: list[str] = ["bad_input", "schema_mismatch"]


class DagNode(BaseModel):
    node_id: str
    type: str
    status: RunState = RunState.queued
    attempt_epoch: int = 0
    lease: dict[str, Any] = {}
    depends_on: list[str] = []
    fan_in: str = "all"  # "all" | "any" | "count:n"
    retry_policy: RetryPolicy = RetryPolicy()
    routing: dict[str, list[str]] = {"on_success": [], "on_failure": []}
    io: dict[str, Any] = {}
    stats: dict[str, Any] = {}
    started_at: str | None = None
    finished_at: str | None = None
    last_event_recv_ms: int = 0
    next_retry_at_ms: int | None = None
    streaming: dict[str, Any] = {}


class TaskDoc(BaseModel):
    id: str
    pipeline_id: str
    status: RunState = RunState.queued
    params: dict[str, Any] = {}
    graph: dict[str, Any] = {"nodes": [], "edges": [], "edges_ex": []}
    result: dict[str, Any] | None = None
    status_history: list[dict[str, Any]] = []
    coordinator: dict[str, Any] = {"liveness": {"state": "ok"}}
    next_retry_at_ms: int | None = None
    started_at: str | None = None
    finished_at: str | None = None
    last_event_recv_ms: int = 0
