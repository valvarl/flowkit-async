# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Handlers registry + protocol for "handler" role.

Contract:

    class Handler(Protocol):
        async def open(self, ctx: TaskContext, **kwargs): ...
        async def run(self, ctx: TaskContext, **kwargs) -> HandlerResult: ...
        async def close(self): ...

    result = HandlerResult(metrics: dict, artifacts_ref: Any | None)

Resolution rules in worker:
  - cmd.input_inline may contain:
        {"handler": "handlers.echo", "args": {...}}
    If no handler name is provided, default is "handlers.echo".
  - The registry can be pre-populated (e.g. by importing modules under
    flowkit.plugins.handlers.* that call register()).
  - If a name is not found in the registry, we try dynamic import:
        "handlers.foo" → import "flowkit.plugins.handlers.foo".
"""

from dataclasses import dataclass
from typing import Any, Protocol, Type, runtime_checkable

__all__ = [
    "Handler",
    "HandlerResult",
    "HandlersRegistry",
    "TaskContext",
    "handlers_registry",
    "register",
    "resolve_handler",
]

# ──────────────────────────────────────────────────────────────────────────────
# Protocol & DTOs
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class TaskContext:
    task_id: str
    node_id: str
    attempt_epoch: int
    worker_id: str
    cancel_flag: Any | None = None
    logger: Any | None = None
    observability: Any | None = None
    extra: dict[str, Any] | None = None


@dataclass
class HandlerResult:
    metrics: dict[str, Any] | None = None
    artifacts_ref: Any | None = None
    # future: partial callbacks, progress, etc.


@runtime_checkable
class Handler(Protocol):
    async def open(self, ctx: TaskContext, **kwargs: Any) -> Any: ...
    async def run(self, ctx: TaskContext, **kwargs: Any) -> HandlerResult: ...
    async def close(self) -> Any: ...


# ──────────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────────


class HandlersRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, type[Handler]] = {}

    def register(self, name: str, cls: type[Handler]) -> None:
        key = str(name).strip()
        if not key:
            raise ValueError("handler name cannot be empty")
        if key in self._handlers:
            raise ValueError(f"handler already registered: {key}")
        self._handlers[key] = cls

    def get(self, name: str) -> type[Handler]:
        try:
            return self._handlers[name]
        except KeyError as e:
            raise LookupError(f"unknown handler: {name}") from e

    def has(self, name: str) -> bool:
        return name in self._handlers

    def list(self) -> list[str]:
        return sorted(self._handlers.keys())


handlers_registry = HandlersRegistry()


def register(name: str, cls: type[Handler]) -> None:
    handlers_registry.register(name, cls)


def resolve_handler(name: str) -> type[Handler]:
    """
    Best-effort resolve:
      - direct registry hit
      - dynamic import "flowkit.plugins.<name>" or "flowkit.plugins.handlers.<leaf>"
    """
    if handlers_registry.has(name):
        return handlers_registry.get(name)

    import importlib

    # Support "handlers.echo" → "flowkit.plugins.handlers.echo"
    mod_name = name
    if name.startswith("handlers."):
        mod_name = f"flowkit.plugins.{name}"
    elif "." not in name:
        # bare name "echo" → flowkit.plugins.handlers.echo
        mod_name = f"flowkit.plugins.handlers.{name}"

    try:
        importlib.import_module(mod_name)
    except Exception:
        # attempt as-is if user passed a fully qualified module path under flowkit.plugins.*
        if not mod_name.startswith("flowkit.plugins."):
            try:
                importlib.import_module(f"flowkit.plugins.{name}")
            except Exception:
                pass

    if handlers_registry.has(name):
        return handlers_registry.get(name)

    # Try last-chance: if they imported module by leaf, also check leaf key
    leaf = name.split(".")[-1]
    if handlers_registry.has(leaf):
        return handlers_registry.get(leaf)

    raise LookupError(f"handler not found after dynamic import: {name}")
