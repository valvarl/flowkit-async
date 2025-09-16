from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class NodeSpec(BaseModel):
    """Strict node-centric schema (no runtime fields). Unknown keys are forbidden."""

    node_id: str
    type: str  # worker role or 'coordinator_fn'
    depends_on: list[str] = Field(default_factory=list)
    fan_in: str = "all"  # 'all'|'any'|'count:k'
    io: dict[str, Any] = Field(default_factory=dict)
    # per-parent triggers: {"parent": "on_batch" | "on_done"}
    triggers: dict[str, Literal["on_batch", "on_done"]] = Field(default_factory=dict)
    retry_policy: dict[str, Any] = Field(default_factory=dict)

    # Pydantic v2 — forbid unknown fields
    model_config = {"extra": "forbid"}


class GraphSpec(BaseModel):
    """Declarative graph without edges/edges_ex and without runtime fields. Unknown keys are forbidden."""

    schema_version: str = "1.0"
    nodes: list[NodeSpec]

    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _sanity(self) -> GraphSpec:
        ids = [n.node_id for n in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate node_id")

        known = set(ids)

        for n in self.nodes:
            # fan_in
            fan = (n.fan_in or "").lower()
            if not (fan == "all" or fan == "any" or fan.startswith("count:")):
                raise ValueError(f"{n.node_id}: bad fan_in={n.fan_in!r}")

            # io.start_when
            sw = (n.io or {}).get("start_when", "ready")
            if str(sw).lower() not in ("ready", "first_batch"):
                raise ValueError(f"{n.node_id}: bad io.start_when={sw!r}")

            # depends_on -> must reference known nodes
            unknown = [p for p in n.depends_on if p not in known]
            if unknown:
                raise ValueError(f"{n.node_id}: depends_on unknown node(s): {unknown}")

            # triggers must be subset of depends_on
            extra_keys = set(n.triggers.keys()) - set(n.depends_on)
            if extra_keys:
                raise ValueError(f"{n.node_id}: triggers contain non-parent(s): {sorted(extra_keys)}")

        # DAG check over depends_on (Kahn)
        indeg = {nid: 0 for nid in known}
        parents_of = {n.node_id: set(n.depends_on) for n in self.nodes}
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
            raise ValueError("graph contains a cycle")

        return self
