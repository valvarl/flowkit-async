# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from flowkit.api.hooks import HookAction, HookContext, HookEvent
from flowkit.core.logging import get_logger


class AuditLogAction(HookAction):  # type: ignore[misc]
    """
    Structured audit log.

    Args (optional):
      - level: "DEBUG"|"INFO"|"WARNING"|"ERROR" (default: INFO)
      - message: string template (optional)
      - tags: dict[str,str] of extra tags (low cardinality only!)

    The action never raises; it logs best-effort.
    """

    name = "audit.log"
    _LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}

    async def run(self, ctx: HookContext, event: HookEvent, args: Mapping[str, Any]) -> None:
        logger = getattr(ctx, "logger", None) or get_logger("hooks.audit")
        level_name = str(args.get("level", "INFO")).upper()
        level = self._LEVELS.get(level_name, 20)

        msg = args.get("message") or "hook event"
        tags = dict(args.get("tags") or {})

        # Keep payload compact & safe for logs
        payload = {
            "event": event.on,
            "node_id": event.node_id,
            "task_id": event.task_id,
            "tags": tags,
        }
        # Minimal meta echo (avoid dumping large values)
        if event.meta:
            payload["meta_keys"] = sorted(list(event.meta.keys()))

        # Use adapter to accept arbitrary keyword fields
        logger.log(level, msg, **payload)
