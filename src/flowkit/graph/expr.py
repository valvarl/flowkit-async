# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

r"""
Minimal, safe, deterministic boolean expression engine.

Supported grammar (EBNF-ish):
  expr       := or_expr
  or_expr    := and_expr { "||" and_expr }*
  and_expr   := not_expr { "&&" not_expr }*
  not_expr   := [ "!" ] primary
  primary    := literal | "(" expr ")" | comparison | function_call | path
  comparison := value ( "==" | "!=" | ">" | "<" | ">=" | "<=" ) value
  value      := identifier | literal
  literal    := "true" | "false" | number | string | null
  identifier := [a-zA-Z_][a-zA-Z0-9_\.]*
  function_call := name "(" [arglist] ")"
  arglist    := expr { "," expr }*

Name resolution:
  - paths are resolved against `env` dict; missing paths return None
  - helpers: collect_parent_signals(), collect_external_names() for compile-time checks

This module is intentionally minimal; templates like {{...}} are resolved outside.
"""

import re
from dataclasses import dataclass
from typing import Any

from .expr_funcs import FunctionRegistry, get_default_functions

__all__ = ["Expr", "ExprError", "parse_expr"]


# ---- tokenizer

_TOKEN_SPEC = [
    ("WS", r"[ \t\r\n]+"),
    ("NUMBER", r"(?:\d+\.\d+|\d+)"),
    ("STRING", r"'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\""),
    ("AND", r"&&"),
    ("OR", r"\|\|"),
    ("NOT", r"!"),
    ("GE", r">="),
    ("LE", r"<="),
    ("GT", r">"),
    ("LT", r"<"),
    ("NE", r"!="),
    ("EQ", r"=="),
    ("LPAREN", r"\("),
    ("RPAREN", r"\)"),
    ("COMMA", r","),
    ("DOT", r"\."),
    ("IDENT", r"[A-Za-z_][A-Za-z0-9_]*"),
]
_TOKEN_RE = re.compile("|".join(f"(?P<{name}>{pattern})" for name, pattern in _TOKEN_SPEC))


class ExprError(ValueError):
    """User-facing expression error (no internals leaked)."""


class _Tok:
    def __init__(self, kind: str, value: Any = None):
        self.kind = kind
        self.value = value

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"{self.kind}:{self.value!r}"


def _lex(s: str) -> list["._Tok"]:
    out: list[_Tok] = []
    pos = 0
    while pos < len(s):
        m = _TOKEN_RE.match(s, pos)
        if not m:
            raise ExprError(f"bad token at position {pos}")
        kind = m.lastgroup or ""
        text = m.group(kind)
        pos = m.end()
        if kind == "WS":
            continue
        if kind == "NUMBER":
            out.append(_Tok(kind, float(text) if "." in text else int(text)))
        elif kind == "STRING":
            # keep simple: unescape using Python string escape rules
            val = bytes(text[1:-1], "utf-8").decode("unicode_escape")
            out.append(_Tok(kind, val))
        else:
            out.append(_Tok(kind, text))
    return out


# ---- AST


@dataclass(frozen=True)
class Expr:
    """Immutable AST node with small helper methods."""

    kind: str
    value: Any = None
    children: tuple[Expr, ...] = ()

    # --- analysis helpers

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

    # --- evaluation

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


# ---- recursive descent parser


class _Parser:
    def __init__(self, tokens: list[_Tok]):
        self.toks = tokens
        self.i = 0

    def peek(self) -> _Tok:
        if self.i >= len(self.toks):
            return _Tok("EOF")
        return self.toks[self.i]

    def eat(self, kind: str | None = None) -> _Tok:
        t = self.peek()
        if kind and t.kind != kind:
            raise ExprError(f"expected {kind}, got {t.kind}")
        self.i += 1
        return t

    def parse(self) -> Expr:
        node = self.parse_or()
        if self.peek().kind != "EOF":
            raise ExprError("unexpected tokens after expression")
        return node

    def parse_or(self) -> Expr:
        node = self.parse_and()
        while self.peek().kind == "OR":
            self.eat("OR")
            rhs = self.parse_and()
            node = Expr("||", children=(node, rhs))
        return node

    def parse_and(self) -> Expr:
        node = self.parse_not()
        while self.peek().kind == "AND":
            self.eat("AND")
            rhs = self.parse_not()
            node = Expr("&&", children=(node, rhs))
        return node

    def parse_not(self) -> Expr:
        if self.peek().kind == "NOT":
            self.eat("NOT")
            node = self.parse_compare()
            return Expr("!", children=(node,))
        return self.parse_compare()

    def parse_compare(self) -> Expr:
        node = self.parse_primary()
        while self.peek().kind in {"EQ", "NE", "GT", "LT", "GE", "LE"}:
            op = self.eat().kind
            rhs = self.parse_primary()
            node = Expr(
                {"EQ": "==", "NE": "!=", "GT": ">", "LT": "<", "GE": ">=", "LE": "<="}[op], children=(node, rhs)
            )
        return node

    def parse_primary(self) -> Expr:
        t = self.peek()
        if t.kind == "LPAREN":
            self.eat("LPAREN")
            node = self.parse_or()
            self.eat("RPAREN")
            return node
        if t.kind == "NUMBER":
            self.eat("NUMBER")
            return Expr("LIT", t.value)
        if t.kind == "STRING":
            self.eat("STRING")
            return Expr("LIT", t.value)
        if t.kind == "IDENT":
            return self.parse_ident_based()
        if t.kind in {"AND", "OR", "RPAREN", "COMMA", "EOF"}:
            raise ExprError("unexpected token")
        raise ExprError(f"unsupported token {t.kind}")

    def parse_ident_based(self) -> Expr:
        # path or function call
        parts: list[str] = [self.eat("IDENT").value]
        while self.peek().kind == "DOT":
            self.eat("DOT")
            seg = self.eat("IDENT").value
            parts.append(seg)

        if self.peek().kind == "LPAREN":
            # function call
            self.eat("LPAREN")
            args: list[Expr] = []
            if self.peek().kind != "RPAREN":
                args.append(self.parse_or())
                while self.peek().kind == "COMMA":
                    self.eat("COMMA")
                    args.append(self.parse_or())
            self.eat("RPAREN")
            return Expr("CALL", parts[0], tuple(args))
        # path
        # special literals true/false/null
        if parts == ["true"]:
            return Expr("LIT", True)
        if parts == ["false"]:
            return Expr("LIT", False)
        if parts == ["null"]:
            return Expr("LIT", None)
        return Expr("PATH", tuple(parts))


def parse_expr(text: str) -> Expr:
    """Parse user expression into AST, raising ExprError on invalid syntax."""
    tokens = _lex(text)
    return _Parser(tokens).parse()
