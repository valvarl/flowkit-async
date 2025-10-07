# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Built-in hook actions & plugin registration.

This module exposes a Plugin that registers coordinator/worker hook actions:
- audit.log
- emit.metrics
- http.post
- notify (generic), notify.slack, notify.telegram
"""

from collections.abc import Mapping
from typing import Any, Dict

from flowkit.api.adapters import Plugin

from .audit_log import AuditLogAction
from .emit_metrics import EmitMetricsAction
from .http_post import HttpPostAction
from .notify import NotifyAction, NotifySlackAction, NotifyTelegramAction

__all__ = [
    "AuditLogAction",
    "EmitMetricsAction",
    "HooksPlugin",
    "HttpPostAction",
    "NotifyAction",
    "NotifySlackAction",
    "NotifyTelegramAction",
]


class HooksPlugin(Plugin):
    """Registers only hook actions; other plugin groups are empty."""

    # Adapters / transforms / handlers / externals — not provided here
    def sources(self) -> Mapping[str, Any] | None:  # type: ignore[override]
        return {}

    def sinks(self) -> Mapping[str, Any] | None:  # type: ignore[override]
        return {}

    def transforms(self) -> Mapping[str, Any] | None:  # type: ignore[override]
        return {}

    def handlers(self) -> Mapping[str, Any] | None:  # type: ignore[override]
        return {}

    def externals(self) -> Mapping[str, Any] | None:  # type: ignore[override]
        return {}

    # Hook actions we contribute
    def hook_actions(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            AuditLogAction.name: AuditLogAction,
            EmitMetricsAction.name: EmitMetricsAction,
            HttpPostAction.name: HttpPostAction,
            NotifyAction.name: NotifyAction,
            NotifySlackAction.name: NotifySlackAction,
            NotifyTelegramAction.name: NotifyTelegramAction,
        }
