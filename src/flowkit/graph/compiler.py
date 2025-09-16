from __future__ import annotations

from typing import Any

from ..protocol.messages import RunState
from .spec import GraphSpec, NodeSpec


def _children_from_parents(parents_by_child: dict[str, set[str]]) -> dict[str, list[str]]:
    """Invert {child -> {parents}} into {parent -> [children]} and de-duplicate/sort."""
    out: dict[str, list[str]] = {}
    for child, parents in parents_by_child.items():
        for p in parents:
            out.setdefault(p, []).append(child)
    for k, v in out.items():
        # stable order, no duplicates
        out[k] = sorted(list(dict.fromkeys(v)))
    return out


def compile_plan(spec: GraphSpec) -> dict[str, Any]:
    """Compile a node-centric GraphSpec into an ExecutionPlan (plain dict).

    Returns:
      - children_by_parent / parents_by_child
      - triggers: per-(child,parent) trigger kind
      - fan_in: join policy per child

    Default trigger is derived from node.io.start_when:
      * "first_batch" -> "on_batch"  (low-latency streaming start)
      * otherwise     -> "on_done"   (start after parents are finished)

    Explicit `NodeSpec.triggers` always override the default for listed parents.
    """
    node_by_id: dict[str, NodeSpec] = {n.node_id: n for n in spec.nodes}
    parents_by_child_set: dict[str, set[str]] = {n.node_id: set(n.depends_on) for n in spec.nodes}

    # Triggers: explicit per-node map wins, otherwise derived from io.start_when
    triggers: dict[str, dict[str, str]] = {}
    for child, parents in parents_by_child_set.items():
        if not parents:
            continue
        n = node_by_id[child]
        default_trigger = (
            "on_batch" if str((n.io or {}).get("start_when", "ready")).lower() == "first_batch" else "on_done"
        )
        tmap: dict[str, str] = {}
        for p in parents:
            tmap[p] = (n.triggers or {}).get(p, default_trigger)
        triggers[child] = tmap

    children_by_parent = _children_from_parents(parents_by_child_set)
    parents_by_child_list: dict[str, list[str]] = {c: sorted(list(ps)) for c, ps in parents_by_child_set.items()}
    fan_in = {n.node_id: n.fan_in for n in spec.nodes if parents_by_child_set.get(n.node_id)}

    return {
        "children_by_parent": children_by_parent,
        "parents_by_child": parents_by_child_list,
        "triggers": triggers,
        "fan_in": fan_in,
    }


def prepare_for_task_create(
    graph_input: dict[str, Any],
    *,
    allowed_roles: list[str] | None = None,
) -> tuple[GraphSpec, dict[str, Any], dict[str, Any]]:
    """Validate/compile user input into (spec, execution_plan, graph_runtime).

    Steps:
      1) Strictly validate GraphSpec (unknown fields -> ValidationError).
      2) Compile an ExecutionPlan from the declarative spec.
      3) Build a normalized runtime graph slice for the coordinator
         (workers never read it).

    Args:
      graph_input: user-supplied graph dict (declarative only).
      allowed_roles: optional whitelist of worker roles.

    Raises:
      ValueError: when a node type is not allowed.

    Returns:
      (spec, execution_plan, graph_runtime) where graph_runtime contains only
      coordinator-owned runtime fields (status/attempt_epoch/lease/stats).
    """
    spec = GraphSpec.model_validate(graph_input)

    if allowed_roles:
        allowed = set(allowed_roles) | {"coordinator_fn"}
        for n in spec.nodes:
            if n.type not in allowed:
                raise ValueError(f"{n.node_id}: unknown role type={n.type!r}; allowed={sorted(allowed)}")

    plan = compile_plan(spec)

    # Normalized runtime graph (no edges/edges_ex)
    graph_runtime: dict[str, Any] = {
        "schema_version": spec.schema_version,
        "nodes": [],
    }
    for n in spec.nodes:
        graph_runtime["nodes"].append(
            {
                "node_id": n.node_id,
                "type": n.type,
                "depends_on": list(n.depends_on),
                "fan_in": n.fan_in,
                "io": n.io,  # keep io hints; trigger semantics live in the compiled plan
                # ---- runtime ----
                "status": RunState.queued,
                "attempt_epoch": 0,
                "stats": {},
                "lease": {},
            }
        )

    return spec, plan, graph_runtime
