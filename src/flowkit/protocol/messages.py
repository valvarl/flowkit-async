from __future__ import annotations
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

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
    payload: Dict[str, Any] = Field(default_factory=dict)
    target_worker_id: Optional[str] = None

class CmdTaskStart(BaseModel):
    cmd: CommandKind
    input_ref: Optional[Dict[str, Any]] = None
    input_inline: Optional[Dict[str, Any]] = None
    batching: Optional[Dict[str, Any]] = None
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
    metrics: Dict[str, Any] = {}
    artifacts_ref: Optional[Dict[str, Any]] = None

class EvBatchFailed(BaseModel):
    kind: EventKind
    worker_id: str
    batch_uid: str
    reason_code: str
    permanent: bool = False
    error: Optional[str] = None
    artifacts_ref: Optional[Dict[str, Any]] = None

class EvTaskDone(BaseModel):
    kind: EventKind
    worker_id: str
    metrics: Dict[str, Any] = {}
    artifacts_ref: Optional[Dict[str, Any]] = None
    final_uid: Optional[str] = None

class EvTaskFailed(BaseModel):
    kind: EventKind
    worker_id: str
    reason_code: str
    permanent: bool = False
    error: Optional[str] = None

class EvCancelled(BaseModel):
    kind: EventKind
    worker_id: str
    reason: str

class SigCancel(BaseModel):
    sig: str = "CANCEL"
    reason: str
    cancel_token: Optional[str] = None
    deadline_ts_ms: Optional[int] = None

class QTaskDiscover(BaseModel):
    query: QueryKind
    want_epoch: int

class RTaskSnapshot(BaseModel):
    reply: ReplyKind
    worker_id: Optional[str] = None
    run_state: str
    attempt_epoch: int
    lease: Optional[Dict[str, Any]] = None
    progress: Optional[Dict[str, Any]] = None
    artifacts: Optional[Dict[str, Any]] = None


# ---- DAG storage models (kept for compatibility)
class RetryPolicy(BaseModel):
    max: int = 2
    backoff_sec: int = 300
    permanent_on: List[str] = ["bad_input", "schema_mismatch"]

class DagNode(BaseModel):
    node_id: str
    type: str
    status: RunState = RunState.queued
    attempt_epoch: int = 0
    lease: Dict[str, Any] = {}
    depends_on: List[str] = []
    fan_in: str = "all"  # "all" | "any" | "count:n"
    retry_policy: RetryPolicy = RetryPolicy()
    routing: Dict[str, List[str]] = {"on_success": [], "on_failure": []}
    io: Dict[str, Any] = {}
    stats: Dict[str, Any] = {}
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    last_event_recv_ms: int = 0
    next_retry_at_ms: Optional[int] = None
    streaming: Dict[str, Any] = {}

class TaskDoc(BaseModel):
    id: str
    pipeline_id: str
    status: RunState = RunState.queued
    params: Dict[str, Any] = {}
    graph: Dict[str, Any] = {"nodes": [], "edges": [], "edges_ex": []}
    result: Optional[Dict[str, Any]] = None
    status_history: List[Dict[str, Any]] = []
    coordinator: Dict[str, Any] = {"liveness": {"state": "ok"}}
    next_retry_at_ms: Optional[int] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    last_event_recv_ms: int = 0
