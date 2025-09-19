# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Hook model and action protocol for FlowKit.

Hooks allow users to run side effects (emit metrics, HTTP POST, Kafka publish,
vars operations, etc.) on lifecycle events produced by Coordinator/Worker.
"""

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class HookEvent:
    """
    Emitted by coordinator/worker on lifecycle transitions.

    Attributes:
        on: Event name (e.g., "node.started", "source.item", "output.sent").
        node_id: Node identifier.
        task_id: Task identifier.
        meta: Event metadata (low-cardinality).
    """

    on: str
    node_id: str
    task_id: str
    meta: Mapping[str, Any]


@dataclass
class HookContext:
    """
    Execution context for hook actions.
    """

    params: Mapping[str, Any]
    vars: Mapping[str, Any]
    externals: Mapping[str, Any]
    logger: Any
    clock: Any


@dataclass(frozen=True)
class HookSelector:
    """
    Predicate definition that decides if an action should fire for a given event.

    Supported fields:
        on: required event name.
        source: optional source alias (source.* events).
        output: optional output channel name (output.* events).
    """

    on: str
    source: Optional[str] = None
    output: Optional[str] = None

    def matches(self, event: HookEvent) -> bool:
        if self.on != event.on:
            return False
        # Optional: callers may put 'source'/'output' inside event.meta
        if self.source is not None and event.meta.get("source") != self.source:
            return False
        if self.output is not None and event.meta.get("output") != self.output:
            return False
        return True


@runtime_checkable
class HookAction(Protocol):
    """
    Action executed upon matching HookEvent.

    The action must be idempotent or protected by delivery policies when used in
    at-least-once flows.
    """

    name: str

    async def run(self, ctx: HookContext, event: HookEvent, args: Mapping[str, Any]) -> None: ...
