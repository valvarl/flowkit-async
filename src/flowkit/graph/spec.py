# src/flowkit/graph/spec.py
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# -------------------------------
# Shared literal enums
# -------------------------------

StartWhen = Literal["true", "false", "all", "any", "expr"]
SelectKind = Literal["batches", "final"]
MergeStrategy = Literal["interleave", "concat", "priority"]
DeliverySemantics = Literal["at_least_once", "exactly_once"]
GatewayKind = Literal["inclusive", "exclusive"]
BarrierKind = Literal["all", "any"]
HookEvent = Literal[
    "node.scheduled",
    "node.started",
    "node.finished",
    "node.failed",
    "on_retry",
    "gate.open",
    "source.opened",
    "source.item",
    "source.eof",
    "source.error",
    "merge.started",
    "merge.progress",
    "merge.finished",
    "output.started",
    "output.sent",
    "output.error",
    "output.finished",
    "preflight.begin",
    "preflight.end",
    "cas_skip",
    "heartbeat.miss",
]

# -------------------------------
# Meta / Defaults / Externals
# -------------------------------


class GraphMeta(BaseModel):
    name: str | None = None
    description: str | None = None
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)
    model_config = {"extra": "forbid"}


class StartDefaults(BaseModel):
    when: StartWhen = "all"
    model_config = {"extra": "forbid"}


class MergeDefaults(BaseModel):
    strategy: MergeStrategy = "interleave"
    fair: bool = True
    max_buffer_per_source: int | None = None
    model_config = {"extra": "forbid"}


class RetryPolicy(BaseModel):
    max: int = 2
    backoff_sec: int = 300
    jitter: str | None = None
    permanent_on: list[str] = Field(default_factory=list)
    compensate: list[dict[str, Any]] = Field(default_factory=list)
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> RetryPolicy:
        if self.max < 0:
            raise ValueError("retry_policy.max must be >= 0")
        if self.backoff_sec < 0:
            raise ValueError("retry_policy.backoff_sec must be >= 0")
        return self


class OutputDelivery(BaseModel):
    semantics: DeliverySemantics = "at_least_once"
    idempotency_key: str | None = None
    retry: RetryPolicy | None = None
    dlq: dict[str, Any] | None = None
    model_config = {"extra": "forbid"}


class ConcurrencyDefaults(BaseModel):
    priority: int = 0
    concurrency_key: str | None = None
    limit: int | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> ConcurrencyDefaults:
        if self.limit is not None and self.limit <= 0:
            raise ValueError("concurrency.limit must be > 0")
        return self


class SlaDefaults(BaseModel):
    node_timeout_ms: int | None = None
    batch_deadline_ms: int | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> SlaDefaults:
        if self.node_timeout_ms is not None and self.node_timeout_ms <= 0:
            raise ValueError("sla.node_timeout_ms must be > 0")
        if self.batch_deadline_ms is not None and self.batch_deadline_ms <= 0:
            raise ValueError("sla.batch_deadline_ms must be > 0")
        return self


class GraphDefaults(BaseModel):
    start: StartDefaults = Field(default_factory=StartDefaults)
    merge: MergeDefaults = Field(default_factory=MergeDefaults)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    output_delivery: OutputDelivery = Field(default_factory=OutputDelivery)
    concurrency: ConcurrencyDefaults = Field(default_factory=ConcurrencyDefaults)
    sla: SlaDefaults = Field(default_factory=SlaDefaults)
    model_config = {"extra": "forbid"}


class ExternalSpec(BaseModel):
    kind: str
    model_config = {"extra": "allow"}

    @field_validator("kind")
    @classmethod
    def _nonempty(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError("external.kind must be a non-empty string")
        return v


# -------------------------------
# Hooks
# -------------------------------


class HookWhen(BaseModel):
    on: HookEvent
    source: str | None = None
    output: str | None = None
    every_n: int | None = None
    throttle: dict[str, int] | None = None  # {"seconds": 5}
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> HookWhen:
        if self.every_n is not None and self.every_n <= 0:
            raise ValueError("hooks.when.every_n must be > 0")
        if self.throttle is not None:
            s = int(self.throttle.get("seconds", 0))
            if s <= 0:
                raise ValueError("hooks.when.throttle.seconds must be > 0")
        return self


class HookAction(BaseModel):
    type: str
    args: dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "forbid"}

    @field_validator("type")
    @classmethod
    def _nonempty(cls, v: str) -> str:
        if not v:
            raise ValueError("hook.action.type must be a non-empty string")
        return v


class HookSpec(BaseModel):
    when: HookWhen
    actions: list[HookAction]
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _nonempty_actions(self) -> HookSpec:
        if not self.actions:
            raise ValueError("hook.actions must not be empty")
        return self


# -------------------------------
# Start / Approval / Gate
# -------------------------------


class ApprovalSpec(BaseModel):
    required: bool = False
    external: str | None = None
    timeout_ms: int | None = None
    on_timeout: Literal["fail", "skip", "auto_approve"] | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> ApprovalSpec:
        if self.required and not self.external:
            raise ValueError("start.approval.external is required when approval.required=true")
        if self.timeout_ms is not None and self.timeout_ms <= 0:
            raise ValueError("start.approval.timeout_ms must be > 0")
        return self


class GateSpec(BaseModel):
    expr: str
    model_config = {"extra": "forbid"}

    @field_validator("expr")
    @classmethod
    def _expr_nonempty(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError("start.gate.expr must be a non-empty string")
        return v


class StartSpec(BaseModel):
    when: StartWhen = "all"
    expr: str | None = None
    gate: GateSpec | None = None
    approval: ApprovalSpec | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> StartSpec:
        if self.when == "expr" and not self.expr:
            raise ValueError("start.expr is required when start.when='expr'")
        if self.when in ("true", "false") and self.expr:
            raise ValueError("start.expr is not allowed when start.when is 'true' or 'false'")
        return self


# -------------------------------
# I/O
# -------------------------------


class OriginSpec(BaseModel):
    parent: str | None = None
    external: str | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _exactly_one(self) -> OriginSpec:
        cnt = int(bool(self.parent)) + int(bool(self.external))
        if cnt != 1:
            raise ValueError("exactly one of origin.parent or origin.external must be provided")
        return self


class AdapterSpec(BaseModel):
    name: str
    args: dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "forbid"}

    @field_validator("name")
    @classmethod
    def _nonempty(cls, v: str) -> str:
        if not v:
            raise ValueError("adapter.name must be a non-empty string")
        return v


class OpSpec(BaseModel):
    op: str
    args: dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "forbid"}

    @field_validator("op")
    @classmethod
    def _nonempty(cls, v: str) -> str:
        if not v:
            raise ValueError("op.op must be a non-empty string")
        return v


class InputSourceSpec(BaseModel):
    alias: str
    origin: OriginSpec
    select: SelectKind = "batches"
    adapter: AdapterSpec
    ops: list[OpSpec] = Field(default_factory=list)
    model_config = {"extra": "forbid"}

    @field_validator("alias")
    @classmethod
    def _alias_ok(cls, v: str) -> str:
        if not v:
            raise ValueError("input.sources.alias must be non-empty")
        return v


class MergeSpec(BaseModel):
    strategy: MergeStrategy = "interleave"
    fair: bool = True
    max_buffer_per_source: int | None = None
    priority: list[str] | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _priority_rules(self) -> MergeSpec:
        if self.max_buffer_per_source is not None and self.max_buffer_per_source <= 0:
            raise ValueError("merge.max_buffer_per_source must be > 0")
        if self.strategy != "priority" and self.priority is not None:
            raise ValueError("merge.priority is only allowed when strategy='priority'")
        return self


class InputSpec(BaseModel):
    merge: MergeSpec = Field(default_factory=MergeSpec)
    sources: list[InputSourceSpec]
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _unique_aliases(self) -> InputSpec:
        aliases = [s.alias for s in self.sources]
        if len(aliases) != len(set(aliases)):
            raise ValueError("input.sources.alias must be unique within a node")
        return self


class OutputChannelSpec(BaseModel):
    name: str
    adapter: AdapterSpec
    model_config = {"extra": "forbid"}

    @field_validator("name")
    @classmethod
    def _nonempty(cls, v: str) -> str:
        if not v:
            raise ValueError("output.channels.name must be non-empty")
        return v


class OutputSpec(BaseModel):
    channels: list[OutputChannelSpec]
    delivery: OutputDelivery | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _unique_channels(self) -> OutputSpec:
        names = [c.name for c in self.channels]
        if len(names) != len(set(names)):
            raise ValueError("output.channels.name must be unique within a node")
        if self.delivery and self.delivery.dlq:
            ch = self.delivery.dlq.get("channel")
            if ch and ch not in names:
                raise ValueError("output.delivery.dlq.channel must reference an existing output channel")
        return self


class IoSpec(BaseModel):
    input: InputSpec | None = None
    output: OutputSpec | None = None
    model_config = {"extra": "forbid"}


# -------------------------------
# Vars ops
# -------------------------------


class VarOp(BaseModel):
    op: Literal["set", "merge", "incr"]
    key: str
    value: Any | None = None
    by: int | None = None
    when: str | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> VarOp:
        if self.op == "incr":
            if self.by is None:
                raise ValueError("vars.incr requires 'by'")
            if self.value is not None:
                raise ValueError("vars.incr must not include 'value'")
        if self.op in ("set", "merge") and self.value is None:
            raise ValueError(f"vars.{self.op} requires 'value'")
        return self


# -------------------------------
# Policies / Resources / Worker hints
# -------------------------------


class ConcurrencySpec(BaseModel):
    priority: int = 0
    concurrency_key: str | None = None
    limit: int | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> ConcurrencySpec:
        if self.limit is not None and self.limit <= 0:
            raise ValueError("concurrency.limit must be > 0")
        return self


class SlaSpec(BaseModel):
    node_timeout_ms: int | None = None
    batch_deadline_ms: int | None = None
    on_violation: list[dict[str, Any]] = Field(default_factory=list)
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> SlaSpec:
        if self.node_timeout_ms is not None and self.node_timeout_ms <= 0:
            raise ValueError("sla.node_timeout_ms must be > 0")
        if self.batch_deadline_ms is not None and self.batch_deadline_ms <= 0:
            raise ValueError("sla.batch_deadline_ms must be > 0")
        return self


class ResourceSpec(BaseModel):
    cpu_quota: str | None = None
    mem_watermark_mb: int | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> ResourceSpec:
        if self.mem_watermark_mb is not None and self.mem_watermark_mb <= 0:
            raise ValueError("resources.mem_watermark_mb must be > 0")
        return self


class WorkerHints(BaseModel):
    prefetch: int | None = None
    inflight_queue: int | None = None
    autotune_chunk: bool = False
    checkpointing: dict[str, Any] | None = None
    tracing: dict[str, Any] | None = None
    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> WorkerHints:
        if self.prefetch is not None and self.prefetch <= 0:
            raise ValueError("worker.prefetch must be > 0")
        if self.inflight_queue is not None and self.inflight_queue <= 0:
            raise ValueError("worker.inflight_queue must be > 0")
        return self


# -------------------------------
# Dynamic edges / foreach
# -------------------------------


class EdgeEx(BaseModel):
    from_: str = Field(alias="from")
    to: str
    on: Literal["on_done", "on_failed", "on_batch"] = "on_done"
    when: str | None = None
    gateway: GatewayKind = "inclusive"
    model_config = {"extra": "forbid", "populate_by_name": True}


class ForeachSpec(BaseModel):
    from_: str = Field(alias="from")
    select: Literal["batches", "final"] = "final"
    items_expr: str
    spawn: dict[str, Any]
    gather: dict[str, Any]
    model_config = {"extra": "forbid", "populate_by_name": True}


# -------------------------------
# NodeSpec v2
# -------------------------------


class NodeSpecV2(BaseModel):
    node_id: str
    type: str
    parents: list[str] = Field(default_factory=list)

    start: StartSpec = Field(default_factory=StartSpec)
    io: IoSpec = Field(default_factory=IoSpec)
    vars: list[VarOp] = Field(default_factory=list)

    retry_policy: RetryPolicy | None = None
    sla: SlaSpec | None = None
    concurrency: ConcurrencySpec | None = None
    resources: ResourceSpec | None = None

    externals: dict[str, ExternalSpec] = Field(default_factory=dict)
    hooks: list[HookSpec] = Field(default_factory=list)
    worker: WorkerHints | None = None

    model_config = {"extra": "forbid"}

    @field_validator("node_id")
    @classmethod
    def _nid_nonempty(cls, v: str) -> str:
        if not v:
            raise ValueError("node_id must be a non-empty string")
        return v

    @field_validator("type")
    @classmethod
    def _type_nonempty(cls, v: str) -> str:
        if not v:
            raise ValueError("type must be a non-empty string")
        return v

    @model_validator(mode="after")
    def _io_refs_sanity(self) -> NodeSpecV2:
        # Input origins must reference listed parents or known externals (checked later at graph level).
        if self.io and self.io.input and self.io.input.sources:
            pset = set(self.parents)
            aliases: set[str] = set()
            for s in self.io.input.sources:
                if s.alias in aliases:
                    raise ValueError(f"duplicate input source alias: {s.alias}")
                aliases.add(s.alias)
                if s.origin.parent and s.origin.parent not in pset:
                    raise ValueError(
                        f"input source '{s.alias}' references parent '{s.origin.parent}' "
                        f"which is not listed in node.parents"
                    )
            if self.io.input.merge.strategy == "priority":
                pr = self.io.input.merge.priority or []
                for a in pr:
                    if a not in aliases:
                        raise ValueError(f"merge.priority references unknown alias '{a}'")
        # Hooks selectors must exist
        if self.hooks:
            src_aliases = set(a.alias for a in (self.io.input.sources if self.io and self.io.input else []))
            out_names = set(ch.name for ch in (self.io.output.channels if self.io and self.io.output else []))
            for h in self.hooks:
                if h.when.source and h.when.source not in src_aliases:
                    raise ValueError(f"hook.when.source '{h.when.source}' not found among input sources")
                if h.when.output and h.when.output not in out_names:
                    raise ValueError(f"hook.when.output '{h.when.output}' not found among output channels")
        # Approval external must be specified if required (existence checked at graph level)
        if self.start and self.start.approval and self.start.approval.required and not self.start.approval.external:
            raise ValueError("start.approval.external is required when approval.required=true")
        return self


# -------------------------------
# GraphSpec v2.1
# -------------------------------


class GraphSpecV21(BaseModel):
    schema_version: Literal["2.1"] = "2.1"
    meta: GraphMeta | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    externals: dict[str, ExternalSpec] = Field(default_factory=dict)
    hooks: list[HookSpec] = Field(default_factory=list)
    defaults: GraphDefaults = Field(default_factory=GraphDefaults)

    edges_ex: list[EdgeEx] = Field(default_factory=list)
    foreach: list[ForeachSpec] = Field(default_factory=list)

    nodes: list[NodeSpecV2]

    feature_flags: dict[str, bool] = Field(default_factory=dict)
    obs: dict[str, Any] | None = None
    templates: dict[str, Any] | None = None

    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> GraphSpecV21:
        # Unique node ids
        ids = [n.node_id for n in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate node_id in graph")

        known = set(ids)

        # Parents must reference existing nodes
        for n in self.nodes:
            unknown = [p for p in n.parents if p not in known]
            if unknown:
                raise ValueError(f"{n.node_id}: unknown parent(s): {unknown}")

        # DAG acyclicity over static parents
        indeg = {nid: 0 for nid in known}
        parents_of = {n.node_id: set(n.parents) for n in self.nodes}
        for child, parents in parents_of.items():
            indeg[child] = len(parents)

        queue = [nid for nid, deg in indeg.items() if deg == 0]
        visited = 0
        while queue:
            x = queue.pop()
            visited += 1
            for child, parents in parents_of.items():
                if x in parents:
                    indeg[child] -= 1
                    if indeg[child] == 0:
                        queue.append(child)
        if visited < len(known):
            raise ValueError("graph contains a cycle in static parents")

        # edges_ex node refs
        for e in self.edges_ex:
            if e.from_ not in known:
                raise ValueError(f"edges_ex.from references unknown node '{e.from_}'")
            if e.to not in known:
                raise ValueError(f"edges_ex.to references unknown node '{e.to}'")

        # foreach: template existence + node refs
        if self.foreach:
            tmpl = set((self.templates or {}).keys())
            for f in self.foreach:
                if f.from_ not in known:
                    raise ValueError(f"foreach.from references unknown node '{f.from_}'")
                gto = str((f.gather or {}).get("to", ""))
                if gto and gto not in known:
                    raise ValueError(f"foreach.gather.to references unknown node '{gto}'")
                tname = str((f.spawn or {}).get("template_node", ""))
                if not tname:
                    raise ValueError("foreach.spawn.template_node must be provided")
                if tname not in tmpl:
                    raise ValueError(f"foreach.spawn.template_node '{tname}' not found in graph.templates")

        # node-level externals must be consistent with graph.externals when referenced by name
        # ( deeper per-node checks performed in compiler after expression parsing)
        return self
