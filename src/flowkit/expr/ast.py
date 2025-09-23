# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .funcs import FunctionRegistry, get_default_functions

__all__ = ["Expr", "ExprError"]


class ExprError(ValueError):
    """User-facing expression error (no internals leaked)."""


@dataclass(frozen=True)
class Expr:
    """Immutable AST node with small helper methods."""

    kind: str
    value: Any = None
    children: tuple[Expr, ...] = ()

    # --- analysis helpers -------------------------------------------------

    def collect_parent_signals(self) -> dict[str, set]:
        """Collect references like <parent>.done / <parent>.batch."""
        acc: dict[str, set] = {}
        self._walk_collect_parent_signals(self, acc)
        return acc

    def _walk_collect_parent_signals(self, node: Expr, acc: dict[str, set]) -> None:
        if node.kind == "PATH" and isinstance(node.value, tuple) and len(node.value) >= 2:
            a, b = node.value[0], node.value[1]
            if isinstance(a, str) and isinstance(b, str) and b in {"done", "batch", "failed", "exists"}:
                acc.setdefault(a, set()).add(b)
        for ch in node.children:
            self._walk_collect_parent_signals(ch, acc)

    def collect_external_names(self) -> set:
        """Collect references like external.<name>.*"""
        acc: set = set()
        self._walk_collect_externals(self, acc)
        return acc

    def _walk_collect_externals(self, node: Expr, acc: set) -> None:
        if node.kind == "PATH" and isinstance(node.value, tuple) and len(node.value) >= 2:
            parts = node.value
            if parts[0] == "external" and isinstance(parts[1], str):
                acc.add(parts[1])
        for ch in node.children:
            self._walk_collect_externals(ch, acc)

    # --- evaluation -------------------------------------------------------

    def eval(self, env: dict[str, Any], *, fns: FunctionRegistry | None = None) -> Any:
        """Evaluate the AST against an environment mapping."""
        if fns is None:
            fns = get_default_functions()

        k = self.kind
        if k == "LIT":
            return self.value
        if k == "PATH":
            cur: Any = env
            for seg in self.value:
                if isinstance(cur, dict) and seg in cur:
                    cur = cur[seg]
                else:
                    return None
            return cur
        if k in {"==", "!=", ">", "<", ">=", "<="}:
            a = self.children[0].eval(env, fns=fns)
            b = self.children[1].eval(env, fns=fns)
            return {
                "==": lambda x, y: x == y,
                "!=": lambda x, y: x != y,
                ">": lambda x, y: (x is not None and y is not None and x > y),
                "<": lambda x, y: (x is not None and y is not None and x < y),
                ">=": lambda x, y: (x is not None and y is not None and x >= y),
                "<=": lambda x, y: (x is not None and y is not None and x <= y),
            }[k](a, b)
        if k == "&&":
            return bool(self.children[0].eval(env, fns=fns)) and bool(self.children[1].eval(env, fns=fns))
        if k == "||":
            return bool(self.children[0].eval(env, fns=fns)) or bool(self.children[1].eval(env, fns=fns))
        if k == "!":
            return not bool(self.children[0].eval(env, fns=fns))
        if k == "CALL":
            fn_name = self.value
            fn = fns.get(fn_name)
            args = [c.eval(env, fns=fns) for c in self.children]
            return fn(*args)
        raise ExprError(f"unsupported node kind: {k}")
