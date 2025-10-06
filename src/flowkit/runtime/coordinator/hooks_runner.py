# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Coordinator-side hooks runner.

Matches HookSpec selectors (on/source/output/every_n/throttle) and executes
registered HookAction implementations from the PluginRegistry.

Focus: low-cardinality, side-effect-safe hooks (metrics, audit, notify, HTTP).
"""

import time
from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from typing import Any

from ...api.hooks import HookAction, HookContext, HookEvent
from ...ext.registry import PluginRegistry
from ..common.observability import Observatory


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
    Loads hook specs from a node's compiled plan, resolves actions via PluginRegistry,
    and executes matching actions with best-effort throttling.
    """

    def __init__(self, registry: PluginRegistry | None = None, *, observatory: Observatory | None = None) -> None:
        self._registry = registry
        self._obs = observatory
        # state for every_n / throttle
        self._counters: MutableMapping[str, int] = {}
        self._last_ts: MutableMapping[str, float] = {}

    def compile_node_hooks(self, hooks_spec: Iterable[Mapping[str, Any]]) -> list[_CompiledHook]:
        if not hooks_spec:
            return []
        if self._registry is None:
            # No registry provided — nothing to resolve
            return []
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
                actions.append(cls())  # action instances are assumed lightweight
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
        if not compiled_hooks:
            return

        ev = HookEvent(
            on=on, node_id=node_id, task_id=str(event_meta.get("task_id")) if event_meta else "", meta=event_meta or {}
        )
        cctx = HookContext(
            params=ctx_params or {},
            vars=ctx_vars or {},
            externals=ctx_externals or {},
            logger=ctx_logger or (self._obs.logger if self._obs else None),
            clock=ctx_clock,
            observatory=self._obs,
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
            # run actions (best effort, hook is responsible for its own errors)
            for a in ch.actions:
                try:
                    await a.run(cctx, ev, args={})
                except Exception:
                    if self._obs:
                        self._obs.logger.debug(
                            "hook.failed", action=getattr(a, "name", "hook"), on=on, node_id=node_id, exc_info=True
                        )

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
