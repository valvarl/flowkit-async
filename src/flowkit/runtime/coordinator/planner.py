# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Planner: evaluates node start policies, optional gates/approvals and dynamic edges.

This is a pure function layer over the compiled ExecutionPlan:
- evaluate if a node is allowed to start (start.when + gate)
- check if human approval is required (start.approval)
- evaluate dynamic edges (edges_ex.when)
- (optional) foreach spawn planning is a separate concern (not implemented here)

The planner never mutates storage; it produces decisions for the scheduler.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from ...core.time import Clock, SystemClock
from ...graph.compiler import ExecutionPlan
from ...graph.expr import Expr, parse_expr


@dataclass(frozen=True)
class ParentSnapshot:
    """Minimal status used by expressions: done/failed/batch/existence."""

    done: bool = False
    failed: bool = False
    batch: bool = False
    exists: bool = True


@dataclass(frozen=True)
class ExternalSnapshot:
    """External readiness fact for expressions."""

    ready: bool = False


@dataclass(frozen=True)
class EvalContext:
    """Environment for expression evaluation."""

    params: Mapping[str, Any]
    vars: Mapping[str, Any]
    parents: Mapping[str, ParentSnapshot]
    externals: Mapping[str, ExternalSnapshot]


@dataclass(frozen=True)
class StartDecision:
    """Result of start policy/gate evaluation for a node."""

    ready: bool
    need_approval: bool
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class EdgeDecision:
    """Result for a single dynamic edge."""

    enabled: bool
    reason: str | None = None


class Planner:
    """Stateless evaluator over an ExecutionPlan."""

    def __init__(self, *, plan: ExecutionPlan, clock: Clock | None = None) -> None:
        self.plan = plan
        self.clock = clock or SystemClock()

    # ---- start policy

    def evaluate_start(self, node_id: str, ctx: EvalContext) -> StartDecision:
        sp = self.plan.start_policy.get(node_id)
        if not sp:
            # default to 'all parents done'
            if all(ctx.parents.get(p, ParentSnapshot()).done for p in self.plan.parents_by_child.get(node_id, [])):
                return StartDecision(True, False)
            return StartDecision(False, False, reasons=("waiting:parents",))

        # simple modes
        if sp.mode == "true":
            return StartDecision(True, False)
        if sp.mode == "false":
            return StartDecision(False, False, reasons=("disabled",))
        if sp.mode in ("all", "any"):
            preds = [ctx.parents.get(p, ParentSnapshot()).done for p in (sp.parents or [])]
            ok = all(preds) if sp.mode == "all" else any(preds)
            if not ok:
                return StartDecision(False, False, reasons=("waiting:parents",))

        if sp.mode == "expr":
            ok = bool(self._eval(sp.expr or "", ctx))
            if not ok:
                return StartDecision(False, False, reasons=("waiting:expr",))

        # optional gate
        if sp.gate_expr:
            gate_ok = bool(self._eval(sp.gate_expr, ctx))
            if not gate_ok:
                return StartDecision(False, False, reasons=("waiting:gate",))

        # optional human approval (coordinator can implement separate workflow)
        need_approval = False
        # spec provides approval config only in GraphSpec; compiler currently passes through in node policies
        # for now signal "need_approval=False" (hook in your coordinator if you wire ApprovalSpec in plan)
        return StartDecision(True, need_approval)

    # ---- dynamic edges

    def evaluate_edge(self, entry: Mapping[str, Any], ctx: EvalContext) -> EdgeDecision:
        """
        entry: a dict compiled in ExecutionPlan.dynamic_edges with keys:
            from, to, on, when (optional), gateway, ...
        """
        cond = entry.get("when")
        if not cond:
            return EdgeDecision(True)
        try:
            ok = bool(self._eval(cond, ctx))
        except Exception as e:
            return EdgeDecision(False, reason=f"expr_error:{e}")
        return EdgeDecision(ok)

    def iter_enabled_edges(self, ctx: EvalContext) -> Iterable[Mapping[str, Any]]:
        for e in self.plan.dynamic_edges or []:
            if self.evaluate_edge(e, ctx).enabled:
                yield e

    # ---- helpers

    def _eval(self, text: str, ctx: EvalContext) -> Any:
        """Evaluate expression text using the graph's safe expression engine."""
        ast: Expr = parse_expr(text)
        env: dict[str, Any] = {
            # parents.<id>.<predicate>
            **{
                pid: {"done": p.done, "failed": p.failed, "batch": p.batch, "exists": p.exists}
                for pid, p in ctx.parents.items()
            },
            # external.<name>.ready
            "external": {name: {"ready": v.ready} for name, v in ctx.externals.items()},
            "params": dict(ctx.params),
            "vars": dict(ctx.vars),
        }
        return ast.eval(env)
