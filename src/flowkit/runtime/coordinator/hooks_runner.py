# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Coordinator-side hooks runner.

Matches HookSpec selectors (on/source/output/every_n/throttle) and executes
registered HookAction implementations from the PluginRegistry.

This module focuses on low-cardinality, side-effect-safe hooks such as metrics,
audit events, notifications, or vars operations.
"""

import time
from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from typing import Any

from ...api.hooks import HookAction, HookContext, HookEvent
from ...api.registry import PluginRegistry
from ...core.logging import get_logger


@dataclass(frozen=True)
class HookSelector:
    on: str
    source: str | None = None
    output: str | None = None
    every_n: int | None = None
    throttle_sec: int | None = None


@dataclass
class _CompiledHook:
    selector: HookSelector
    actions: list[HookAction]


class HooksRunner:
    """
    Loads hook specs from the compiled plan for a node, resolves action
    implementations via the PluginRegistry and executes matching actions.
    """

    def __init__(self, registry: PluginRegistry) -> None:
        self._registry = registry
        self._log = get_logger("coordinator.hooks")
        # state for every_n / throttle
        self._counters: MutableMapping[str, int] = {}
        self._last_ts: MutableMapping[str, float] = {}

    def compile_node_hooks(self, hooks_spec: Iterable[Mapping[str, Any]]) -> list[_CompiledHook]:
        compiled: list[_CompiledHook] = []
        for h in hooks_spec:
            when = h.get("when") or {}
            selector = HookSelector(
                on=when.get("on", ""),
                source=when.get("source"),
                output=when.get("output"),
                every_n=when.get("every_n"),
                throttle_sec=int((when.get("throttle") or {}).get("seconds", 0)) or None,
            )
            actions: list[HookAction] = []
            for a in h.get("actions", []):
                t = a.get("type", "")
                cls = self._registry.hook_action(t)  # may raise if unknown
                actions.append(cls())  # actions are assumed to be lightweight
            compiled.append(_CompiledHook(selector=selector, actions=actions))
        return compiled

    async def emit(
        self,
        node_id: str,
        compiled_hooks: Iterable[_CompiledHook],
        *,
        on: str,
        event_meta: Mapping[str, Any] | None = None,
        source: str | None = None,
        output: str | None = None,
        ctx_params: Mapping[str, Any] | None = None,
        ctx_vars: Mapping[str, Any] | None = None,
        ctx_externals: Mapping[str, Any] | None = None,
        ctx_logger: Any | None = None,
        ctx_clock: Any | None = None,
    ) -> None:
        """Emit one event, executing matching hook actions."""
        ev = HookEvent(
            on=on, node_id=node_id, task_id=str(event_meta.get("task_id")) if event_meta else "", meta=event_meta or {}
        )
        cctx = HookContext(
            params=ctx_params or {},
            vars=ctx_vars or {},
            externals=ctx_externals or {},
            logger=ctx_logger or self._log,
            clock=ctx_clock,
        )
        for ch in compiled_hooks:
            if not self._matches(ch.selector, on, source, output, node_id):
                continue
            key = self._key(node_id, ch.selector)
            # throttle
            if ch.selector.throttle_sec:
                last = self._last_ts.get(key, 0.0)
                now = time.monotonic()
                if now - last < ch.selector.throttle_sec:
                    continue
                self._last_ts[key] = now
            # every_n
            if ch.selector.every_n:
                cnt = self._counters.get(key, 0) + 1
                self._counters[key] = cnt
                if cnt % ch.selector.every_n != 0:
                    continue
            # run actions
            for a in ch.actions:
                await a.run(cctx, ev, args=self._find_action_args(ch.selector, on, source, output, node_id))

    # ---- helpers

    def _matches(self, sel: HookSelector, on: str, source: str | None, output: str | None, node_id: str) -> bool:
        if sel.on and sel.on != on:
            return False
        if sel.source and sel.source != source:
            return False
        if sel.output and sel.output != output:
            return False
        return True

    def _key(self, node_id: str, sel: HookSelector) -> str:
        return f"{node_id}:{sel.on}:{sel.source or '-'}:{sel.output or '-'}"

    def _find_action_args(
        self, sel: HookSelector, on: str, source: str | None, output: str | None, node_id: str
    ) -> dict[str, Any]:
        # Coordinator-side hooks usually keep args in the spec per action; since the compiled form
        # already resolved them into the action instance (via registry), action classes can take
        # their own defaults. This helper exists for future extension if you want to feed per-event args.
        return {}
