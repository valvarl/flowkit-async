# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .ast import Expr

__all__ = ["eval_expr"]


def eval_expr(expr: Expr, env: Mapping[str, Any]) -> Any:
    """
    Convenience helper mirroring flowkit.api.expr.eval_expr.
    """
    return expr.eval(dict(env))
