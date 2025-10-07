# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Filter transform based on FlowKit expression language (best-effort).

Arguments:
  - expr: str   (required) boolean-like expression

Environment available to the expression:
  - item:    {"payload": <payload>, "meta": <meta>}
  - payload: <payload>
  - meta:    <meta>
  - params:  ctx.params
  - vars:    ctx.vars

Behavior:
  - If the expression evaluates to truthy → keep the element.
  - Falsy/exception → drop the element (return None).
  - Batch: filter per-item and return a Batch or None if empty.

Implementation notes:
  - We try to use the project expression runtime if available:
      `flowkit.expr.runtime` with `eval_bool` or `eval_expr`.
  - If not available, fall back to a very small and safe `eval` over the env.
"""

from collections.abc import Callable
from typing import Any

from ....api.adapters import AdapterContext, Capabilities, TransformOp
from ....api.streams import Batch, ContentKind, Item

# ---- expression engine -------------------------------------------------------


def _resolve_engine() -> Callable[[str, dict[str, Any]], Any]:
    try:
        # Prefer dedicated boolean evaluator if present.
        import flowkit.expr.runtime as ex  # type: ignore

        if hasattr(ex, "eval_bool"):
            return lambda s, env: bool(ex.eval_bool(s, env))  # type: ignore[attr-defined]
        if hasattr(ex, "eval_expr"):
            return lambda s, env: ex.eval_expr(s, env)  # type: ignore[attr-defined]
    except Exception:
        pass

    # Fallback: restricted eval. This is intentionally tiny and should only be
    # used in development/testing. Production should ship the real expr runtime.
    def _fallback_eval(s: str, env: dict[str, Any]) -> Any:
        allowed_builtins = {"len": len, "min": min, "max": max, "bool": bool}
        return eval(
            s,
            {"__builtins__": allowed_builtins},
            env,
        )

    return _fallback_eval


_ENGINE = _resolve_engine()


class FilterExpr(TransformOp):
    name = "filter"
    capabilities = Capabilities(
        accepts=frozenset({ContentKind.RECORD}),
        emits=frozenset({ContentKind.RECORD}),
        bounded_memory=True,
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._expr = str(kwargs.get("expr") or "").strip()
        if not self._expr:
            raise ValueError("filter: 'expr' is required")

    async def open(self, ctx: AdapterContext, **args: Any) -> None:
        return

    async def process(self, x: Item | Batch):
        if isinstance(x, Batch) and x.items is not None:
            kept: list[Item] = []
            for it in x.items:
                if not isinstance(it, Item):
                    it = Item(payload=getattr(it, "payload", it), meta={})
                if self._eval_item(it):
                    kept.append(it)
            if not kept:
                return None
            return Batch(items=kept, frame=None)

        if isinstance(x, Item):
            return x if self._eval_item(x) else None

        # non-record batches are not handled here
        return None

    async def close(self) -> None:
        return

    # ---- helpers -------------------------------------------------------------

    def _eval_item(self, it: Item) -> bool:
        env = {
            "item": {"payload": it.payload, "meta": it.meta},
            "payload": it.payload,
            "meta": it.meta,
            "params": self._ctx.params,
            "vars": self._ctx.vars,
        }
        try:
            res = _ENGINE(self._expr, env)
            return bool(res)
        except Exception:
            # Best-effort: on expression error, drop the element.
            return False
