# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Lightweight compatibility checks for IO adapters.

This module is intentionally best-effort and non-fatal: it validates the
`select` capability of input adapters against a capabilities registry.
Future: content/frame compatibility once adapters declare richer capabilities.
"""

from typing import Any

from ..io.capabilities import get_caps  # expected to exist in flowkit/io/capabilities.py

__all__ = ["validate_io_compat"]


def _ok_select(adapter_name: str, select: str) -> tuple[bool, str | None]:
    caps = get_caps(adapter_name)
    if not caps:
        # unknown adapters are allowed (userland), skip strict validation
        return True, None
    sup = set(caps.get("supports_select", []) or [])
    if sup and select not in sup:
        return False, f"adapter {adapter_name!r} does not support select={select!r} (supports: {sorted(sup)})"
    return True, None


def validate_io_compat(node_io: dict[str, Any]) -> list[str]:
    """Return a list of human-readable warnings/errors (non-fatal)."""
    problems: list[str] = []
    if not node_io:
        return problems

    # inputs
    inp = node_io.get("input")
    if inp and isinstance(inp, dict):
        for src in inp.get("sources", []) or []:
            alias = src.get("alias")
            select = (src.get("select") or "batches").lower()
            adapter = (src.get("adapter") or {}).get("name")
            ok, err = _ok_select(str(adapter), str(select))
            if not ok and err:
                problems.append(f"[input:{alias}] {err}")

    # outputs: placeholder for content checks (requires end-to-end inference)
    return problems
