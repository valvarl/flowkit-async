# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Deterministic, side-effect free function whitelist for the expression engine.

Notes:
- Functions MUST raise ValueError on invalid input (no internal details).
- All functions are expected to be pure (no IO, no global state).
"""

import re
from collections.abc import Callable
from typing import Any

__all__ = ["FunctionRegistry", "get_default_functions"]

Func = Callable[..., Any]


class FunctionRegistry:
    """Pluggable function registry used by the expression evaluator."""

    def __init__(self) -> None:
        self._fn: dict[str, Func] = {}

    def register(self, name: str, fn: Func) -> None:
        if not name or not callable(fn):
            raise ValueError("invalid function registration")
        # last-wins to allow test overrides
        self._fn[name] = fn

    def get(self, name: str) -> Func:
        try:
            return self._fn[name]
        except KeyError:
            raise ValueError(f"function {name!r} is not allowed")

    def names(self) -> list[str]:
        return sorted(self._fn.keys())


# ---- Built-ins


def _fn_in(value: Any, *options: Any) -> bool:
    return value in options


def _fn_len(value: Any) -> int:
    try:
        return len(value)  # type: ignore[arg-type]
    except Exception as e:
        raise ValueError("len() argument is not sized") from e


def _fn_starts_with(s: Any, prefix: Any) -> bool:
    if not isinstance(s, str) or not isinstance(prefix, str):
        raise ValueError("starts_with() expects strings")
    return s.startswith(prefix)


_re_cache: dict[str, re.Pattern[str]] = {}


def _fn_regex_match(s: Any, pattern: Any) -> bool:
    if not isinstance(s, str) or not isinstance(pattern, str):
        raise ValueError("regex_match() expects strings")
    rx = _re_cache.get(pattern)
    if rx is None:
        rx = re.compile(pattern)
        _re_cache[pattern] = rx
    return rx.search(s) is not None


def _fn_coalesce(*args: Any) -> Any:
    for a in args:
        if a is not None:
            return a
    return None


def _fn_contains(container: Any, item: Any) -> bool:
    try:
        return item in container  # type: ignore[operator]
    except Exception as e:
        raise ValueError("contains() invalid arguments") from e


def get_default_functions() -> FunctionRegistry:
    """Return a registry pre-populated with production-safe built-ins."""
    reg = FunctionRegistry()
    reg.register("in", _fn_in)
    reg.register("len", _fn_len)
    reg.register("starts_with", _fn_starts_with)
    reg.register("regex_match", _fn_regex_match)
    reg.register("coalesce", _fn_coalesce)
    reg.register("contains", _fn_contains)
    return reg
