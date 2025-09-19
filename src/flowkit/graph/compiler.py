# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Compiler: GraphSpec (v2.2) -> ExecutionPlan.

This pass performs:
- strict Pydantic validation (via GraphSpecV22.model_validate)
- expression parsing for start/gate and dynamic edges
- normalization of IO plans, outputs (channels + delivery + routing)
- merging of global & node hooks
- collection of per-node policies

Workers do not read ExecutionPlan; the coordinator will slice required parts
into runtime commands (CmdTaskStart).
"""

from typing import Any

from pydantic import BaseModel, Field

from .expr import Expr, ExprError, parse_expr
from .spec import GraphSpecV22, NodeSpecV2

# -------------------------------
# ExecutionPlan models
# -------------------------------


class StartPolicy(BaseModel):
    """Normalized start policy with compiled ASTs and dependency summaries."""

    mode: str  # "all" | "any" | "true" | "false" | "expr" | "count:k"
    expr: str | None = None
    expr_ast: dict[str, Any] | None = None
    gate_expr: str | None = None
    gate_ast: dict[str, Any] | None = None

    # for "count:k" we capture the k value for scheduler convenience
    count_k: int | None = None

    parents: list[str] = Field(default_factory=list)
    signals_by_parent: dict[str, list[str]] = Field(default_factory=dict)  # {"A":["batch","done"]}
    needs_externals: list[str] = Field(default_factory=list)


class InputPlan(BaseModel):
    """Input plan for a node (merge-mode only in v2.2 baseline)."""

    merge: dict[str, Any]
    sources: list[dict[str, Any]]  # normalized sources with origin kind/value
    compiled_ops: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)


class OutputPlan(BaseModel):
    """Output plan with channels, delivery, and (optional) routing."""

    channels: list[dict[str, Any]]
    delivery: dict[str, Any] | None = None
    routing: list[dict[str, Any]] = Field(default_factory=list)


class Policies(BaseModel):
    """Collected policy bag (retry, SLA, concurrency, resources, worker hints)."""

    retry: dict[str, Any] | None = None
    sla: dict[str, Any] | None = None
    concurrency: dict[str, Any] | None = None
    resources: dict[str, Any] | None = None
    worker: dict[str, Any] | None = None


class ExecutionPlan(BaseModel):
    """Top-level execution plan used by the coordinator."""

    children_by_parent: dict[str, list[str]] = Field(default_factory=dict)
    parents_by_child: dict[str, list[str]] = Field(default_factory=dict)

    start_policy: dict[str, StartPolicy] = Field(default_factory=dict)
    inputs: dict[str, InputPlan] = Field(default_factory=dict)
    outputs: dict[str, OutputPlan] = Field(default_factory=dict)
    hooks: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    vars_hooks: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    policies: dict[str, Policies] = Field(default_factory=dict)

    graph_externals: dict[str, dict[str, Any]] = Field(default_factory=dict)
    graph_hooks: list[dict[str, Any]] = Field(default_factory=list)

    dynamic_edges: list[dict[str, Any]] = Field(default_factory=list)
    foreach: list[dict[str, Any]] = Field(default_factory=list)


# -------------------------------
# Helpers
# -------------------------------


def _invert_parents(parents_by_child: dict[str, list[str]]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for child, parents in parents_by_child.items():
        for p in parents:
            out.setdefault(p, []).append(child)
    for k, v in out.items():
        out[k] = sorted(list(dict.fromkeys(v)))
    return out


def _expr_to_ast(e: Expr) -> dict[str, Any]:
    return {"kind": e.kind, "value": e.value, "children": [_expr_to_ast(c) for c in e.children]}


def _compile_start(n: NodeSpecV2, graph_externals: dict[str, dict[str, Any]]) -> StartPolicy:
    sp = StartPolicy(mode=n.start.when, parents=list(n.parents))
    # Count:k
    if n.start.when.startswith("count:"):
        k_str = n.start.when[6:]
        k_val = int(k_str)
        sp.count_k = k_val
        if n.parents:
            sp.signals_by_parent = {p: ["done"] for p in n.parents}
        return sp

    # Compile main expr
    if n.start.when == "expr":
        try:
            ast = parse_expr(n.start.expr or "")
        except ExprError as ex:
            raise ValueError(f"{n.node_id}: invalid start.expr: {ex}") from ex
        _validate_expr_refs(n, ast, graph_externals, strict_parents=True)
        sp.expr = n.start.expr
        sp.expr_ast = _expr_to_ast(ast)
        sig_map = {k: sorted(list(v)) for k, v in ast.collect_parent_signals().items()}
        sp.signals_by_parent = sig_map
        sp.needs_externals = sorted(list(ast.collect_external_names()))
    else:
        # Default signals for simple modes
        if n.parents and n.start.when in ("all", "any"):
            sp.signals_by_parent = {p: ["done"] for p in n.parents}

    # Compile gate if present
    if n.start.gate and n.start.gate.expr:
        try:
            gate_ast = parse_expr(n.start.gate.expr)
        except ExprError as ex:
            raise ValueError(f"{n.node_id}: invalid start.gate.expr: {ex}") from ex
        _validate_expr_refs(n, gate_ast, graph_externals, strict_parents=False)
        sp.gate_expr = n.start.gate.expr
        sp.gate_ast = _expr_to_ast(gate_ast)
        # gate may reference extra signals/externals
        for k, vs in gate_ast.collect_parent_signals().items():
            sp.signals_by_parent.setdefault(k, [])
            for v in vs:
                if v not in sp.signals_by_parent[k]:
                    sp.signals_by_parent[k].append(v)
        need_ext = gate_ast.collect_external_names()
        sp.needs_externals = sorted(list(set(sp.needs_externals) | set(need_ext)))
    return sp


def _normalize_inputs(n: NodeSpecV2, g_externals: dict[str, dict[str, Any]]) -> InputPlan | None:
    if not n.io or not n.io.input:
        return None
    ip = n.io.input
    merge = {
        "strategy": ip.merge.strategy,
        "fair": ip.merge.fair,
        "max_buffer_per_source": ip.merge.max_buffer_per_source,
    }
    if ip.merge.strategy == "priority":
        merge["priority"] = list(ip.merge.priority or [])

    sources_norm: list[dict[str, Any]] = []
    ops_by_alias: dict[str, list[dict[str, Any]]] = {}
    for s in ip.sources:
        origin_kind = "parent" if s.origin.parent else "external"
        origin_value = s.origin.parent or s.origin.external  # non-empty by validation
        if origin_kind == "external":
            if origin_value not in (n.externals or {}) and origin_value not in g_externals:
                raise ValueError(f"{n.node_id}: input source '{s.alias}' references unknown external '{origin_value}'")
        entry = {
            "alias": s.alias,
            "origin_kind": origin_kind,
            "origin": origin_value,
            "select": s.select,
            "adapter": {"name": s.adapter.name, "args": dict(s.adapter.args or {})},
        }
        sources_norm.append(entry)
        ops_by_alias[s.alias] = [{"op": op.op, "args": dict(op.args or {})} for op in (s.ops or [])]

    if ip.merge.strategy == "priority" and not ip.merge.priority:
        merge["priority"] = [s["alias"] for s in sources_norm]

    return InputPlan(merge=merge, sources=sources_norm, compiled_ops=ops_by_alias)


def _normalize_outputs(n: NodeSpecV2, g_externals: dict[str, dict[str, Any]]) -> OutputPlan | None:
    if not n.io or not n.io.output:
        return None
    out = n.io.output
    channels_norm = []
    for c in out.channels:
        channels_norm.append({"name": c.name, "adapter": {"name": c.adapter.name, "args": dict(c.adapter.args or {})}})
    delivery = None
    if out.delivery:
        delivery = out.delivery.model_dump(mode="json")
    routing = [{"when": r.when, "to": list(r.to)} for r in (out.routing or [])]
    return OutputPlan(channels=channels_norm, delivery=delivery, routing=routing)


def _merge_hooks(global_hooks: list[Any], node_hooks: list[Any]) -> list[dict[str, Any]]:
    def to_dict(h) -> dict[str, Any]:
        return {
            "when": h.when.model_dump(mode="json"),
            "actions": [a.model_dump(mode="json") for a in h.actions],
        }

    merged = [to_dict(h) for h in (global_hooks or [])] + [to_dict(h) for h in (node_hooks or [])]
    return merged


def _parents_by_child(nodes: list[NodeSpecV2]) -> dict[str, list[str]]:
    return {n.node_id: list(n.parents) for n in nodes}


def _validate_expr_refs(
    n: NodeSpecV2, expr_ast: Expr, graph_externals: dict[str, dict[str, Any]], *, strict_parents: bool
) -> None:
    # Parent signals (enforce parent membership if strict_parents)
    ps = expr_ast.collect_parent_signals()
    for pid in ps.keys():
        if strict_parents and pid not in set(n.parents):
            raise ValueError(f"{n.node_id}: expression references parent '{pid}' not listed in node.parents")
    # Externals
    for ename in expr_ast.collect_external_names():
        if ename not in (n.externals or {}) and ename not in graph_externals:
            raise ValueError(f"{n.node_id}: expression references unknown external '{ename}'")


# -------------------------------
# Public API
# -------------------------------


def compile_execution_plan(spec: GraphSpecV22) -> ExecutionPlan:
    """Compile a declarative GraphSpec v2.2 into a normalized ExecutionPlan."""
    graph_externals = {name: e.model_dump(mode="json") for name, e in (spec.externals or {}).items()}
    graph_hooks = [
        {"when": h.when.model_dump(mode="json"), "actions": [a.model_dump(mode="json") for a in h.actions]}
        for h in (spec.hooks or [])
    ]

    pbc = _parents_by_child(spec.nodes)
    cbp = _invert_parents(pbc)

    start_policy: dict[str, StartPolicy] = {}
    inputs: dict[str, InputPlan] = {}
    outputs: dict[str, OutputPlan] = {}
    hooks: dict[str, list[dict[str, Any]]] = {}
    vars_hooks: dict[str, list[dict[str, Any]]] = {}
    policies: dict[str, Policies] = {}
    dynamic_edges: list[dict[str, Any]] = []
    foreach_compiled: list[dict[str, Any]] = []

    # Per-node compile
    for n in spec.nodes:
        # Approval external existence (if specified)
        if n.start and n.start.approval and n.start.approval.external:
            ename = n.start.approval.external
            if ename not in (n.externals or {}) and ename not in graph_externals:
                raise ValueError(f"{n.node_id}: start.approval.external '{ename}' not found in externals")

        # Start
        sp = _compile_start(n, graph_externals)
        start_policy[n.node_id] = sp

        # Inputs
        ip = _normalize_inputs(n, graph_externals)
        if ip:
            inputs[n.node_id] = ip

        # Outputs
        op = _normalize_outputs(n, graph_externals)
        if op:
            outputs[n.node_id] = op

        # Hooks (global + local)
        hooks[n.node_id] = _merge_hooks(spec.hooks or [], n.hooks or [])

        # Vars
        vars_hooks[n.node_id] = [v.model_dump(mode="json") for v in (n.vars or [])]

        # Policies
        pol = Policies()
        pol.retry = (
            (n.retry_policy or spec.defaults.retry_policy).model_dump(mode="json")
            if (n.retry_policy or spec.defaults.retry_policy)
            else None
        )
        pol.sla = (n.sla or spec.defaults.sla).model_dump(mode="json") if (n.sla or spec.defaults.sla) else None
        pol.concurrency = (
            (n.concurrency or spec.defaults.concurrency).model_dump(mode="json")
            if (n.concurrency or spec.defaults.concurrency)
            else None
        )
        pol.resources = (n.resources).model_dump(mode="json") if (n.resources) else None
        pol.worker = (n.worker).model_dump(mode="json") if (n.worker) else None
        policies[n.node_id] = pol

    # Dynamic edges compile (parse 'when' if provided)
    for e in spec.edges_ex or []:
        entry: dict[str, Any] = {
            "from": e.from_,
            "to": e.to,
            "on": e.on,
            "gateway": e.gateway,
            "when": None,
            "when_ast": None,
            "needs_externals": [],
            "signals_by_node": {},
        }
        if e.when:
            try:
                ast = parse_expr(e.when)
            except ExprError as ex:
                raise ValueError(f"edges_ex[{e.from_}->{e.to}]: invalid when-expr: {ex}") from ex
            # Externals must exist on graph level (node externals do not apply here)
            for ename in ast.collect_external_names():
                if ename not in graph_externals:
                    raise ValueError(f"edges_ex[{e.from_}->{e.to}]: unknown external '{ename}'")
            # Node ids referenced in signals must exist
            node_signals = ast.collect_parent_signals()
            for nid in node_signals.keys():
                if nid not in pbc:
                    raise ValueError(f"edges_ex[{e.from_}->{e.to}]: unknown node '{nid}' in when-expr")
            entry["when"] = e.when
            entry["when_ast"] = _expr_to_ast(ast)
            entry["needs_externals"] = sorted(list(ast.collect_external_names()))
            entry["signals_by_node"] = {k: sorted(list(v)) for k, v in node_signals.items()}
        dynamic_edges.append(entry)

    # Foreach copy-through (structure is validated at spec level; coordinator will interpret)
    for f in spec.foreach or []:
        foreach_compiled.append(
            {
                "from": f.from_,
                "select": f.select,
                "items_expr": f.items_expr,
                "spawn": dict(f.spawn or {}),
                "gather": dict(f.gather or {}),
            }
        )

    return ExecutionPlan(
        children_by_parent=cbp,
        parents_by_child=pbc,
        start_policy=start_policy,
        inputs=inputs,
        outputs=outputs,
        hooks=hooks,
        vars_hooks=vars_hooks,
        policies=policies,
        graph_externals=graph_externals,
        graph_hooks=graph_hooks,
        dynamic_edges=dynamic_edges,
        foreach=foreach_compiled,
    )


def build_runtime_graph(spec: GraphSpecV22) -> dict[str, Any]:
    """Produce a minimal runtime slice which may be embedded into Task docs."""
    nodes = []
    for n in spec.nodes:
        nodes.append(
            {
                "node_id": n.node_id,
                "type": n.type,
                "parents": list(n.parents),
                "io": n.io.model_dump(mode="json") if n.io else {},
            }
        )
    return {"schema_version": spec.schema_version, "nodes": nodes}


def prepare_for_task_create_v22(graph_input: dict[str, Any]) -> tuple[GraphSpecV22, ExecutionPlan, dict[str, Any]]:
    """
    Strictly validate and compile a v2.2 graph.

    Returns (spec, execution_plan, graph_runtime).
    """
    spec = GraphSpecV22.model_validate(graph_input)
    plan = compile_execution_plan(spec)
    runtime = build_runtime_graph(spec)
    return spec, plan, runtime
