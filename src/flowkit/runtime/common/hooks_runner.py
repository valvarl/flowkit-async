# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Best-effort hook bus with lightweight rate limiting.

This is a generic runner shared by coordinator- and worker-side modules.
It executes HookAction implementations with:
  - optional HookSelector checks,
  - per-key min-interval rate limit,
  - error swallowing (hooks must never break the pipeline).

Higher-level modules can "compile" their specs into tuples of
  (action, args, selector) and call `emit(...)` here.
"""

import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from ...api.hooks import HookAction, HookContext, HookEvent, HookSelector
from ...core.logging import get_logger


@dataclass(frozen=True)
class _ActionEntry:
    action: HookAction
    args: Mapping[str, Any] | None
    selector: HookSelector | None


class HooksBus:
    """
    Generic hooks runner with per-key rate limiting.

    Keys are derived from (action.name, event.on, selector.source/output).
    """

    def __init__(self, *, min_interval_ms: int | None = None) -> None:
        self._min_interval_ms = int(min_interval_ms or 0)
        self._last_fired: dict[str, int] = {}
        self._log = get_logger("hooks")

    async def emit(
        self,
        event: HookEvent,
        ctx: HookContext,
        actions: Iterable[tuple[HookAction, Mapping[str, Any] | None, HookSelector | None]],
        *,
        min_interval_ms: int | None = None,
    ) -> None:
        """
        Execute matching hook actions best-effort.

        Args:
            event: HookEvent describing the occurrence.
            ctx: HookContext with params/vars/logger/clock.
            actions: iterable of (action, args, selector).
            min_interval_ms: optional rate limit override (per bus call).
        """
        entries = [_ActionEntry(a, (args or {}), sel) for (a, args, sel) in actions]
        limiter = int(min_interval_ms if min_interval_ms is not None else self._min_interval_ms)

        for ent in entries:
            if ent.selector and not ent.selector.matches(event):
                continue
            key = self._key(ent, event)
            if limiter and not self._can_fire(key, limiter):
                continue
            try:
                await ent.action.run(ctx, event, args=ent.args or {})
                self._touch(key)
            except Exception:
                # hooks are best-effort; never raise
                self._log.debug(
                    "hook.failed", event=event.on, action=getattr(ent.action, "name", "hook"), exc_info=True
                )

    # ---- rate limit helpers --------------------------------------------------

    def _key(self, ent: _ActionEntry, ev: HookEvent) -> str:
        src = ent.selector.source if ent.selector else "-"
        out = ent.selector.output if ent.selector else "-"
        return f"{getattr(ent.action, 'name', 'hook')}|{ev.on}|{src}|{out}"

    def _can_fire(self, key: str, limiter_ms: int) -> bool:
        last = self._last_fired.get(key)
        now = self._now_ms()
        return last is None or (now - last) >= limiter_ms

    def _touch(self, key: str) -> None:
        self._last_fired[key] = self._now_ms()

    def _now_ms(self) -> int:
        return int(time.time() * 1000)
