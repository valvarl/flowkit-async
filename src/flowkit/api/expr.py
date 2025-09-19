# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Public expression engine surface.

We re-export the internal AST type and provide compile/eval helpers so external
code can validate expressions in isolation (e.g., custom hook conditions).
"""

from collections.abc import Mapping
from typing import Any

# Import from internal engine (graph.expr) but keep public API stable here.
from ..graph.expr import Expr, ExprError  # type: ignore[attr-defined]
from ..graph.expr import parse_expr as _parse

__all__ = ["Expr", "ExprError", "compile_expr", "eval_expr"]


def compile_expr(text: str) -> Expr:
    """Parse & type-check expression, return an AST object."""
    return _parse(text)


def eval_expr(expr: Expr, env: Mapping[str, Any]) -> Any:
    """
    Evaluate a precompiled expression against an environment.

    The environment must contain only JSON-serializable primitives and small
    dicts/lists. Functions are restricted to a safe allowlist.
    """
    return expr.eval(dict(env))
