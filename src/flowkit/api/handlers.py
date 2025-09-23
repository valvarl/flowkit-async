# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Role handler contracts for Worker runtime.

Handlers encapsulate role-specific business logic that processes batches produced
by the stream engine (inputs → transforms → merge/ports). A handler is selected
by its `role` name declared in the graph node's `type` (or a similar mapping
done by the Coordinator).

You may either:
  - subclass `BaseHandler` and override its methods, or
  - implement the `RoleHandler` Protocol directly.

Handlers are discovered/registered via `PluginRegistry.register_handler(...)`
or bundled in a Plugin (see `api.adapters.Plugin.handlers()`).
"""

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from .errors import (
    CancelledError,
    DeadlineExceeded,
    FlowkitError,
    PermanentError,
    RetryableError,
)
from .streams import Batch

# --------------------------------------------------------------------------- #
# Context & results
# --------------------------------------------------------------------------- #


@dataclass
class HandlerContext:
    """
    Execution context passed to handler callbacks.

    Attributes:
        task_id: Logical task identifier.
        node_id: Node identifier within the task.
        attempt_epoch: Monotonic attempt number (>= 1).
        params: Immutable runtime parameters (from graph.params).
        vars: Mutable task-level variables (read-only view for the handler).
        externals: Resolved external resource handles (kafka/http/etc).
        cancel_flag: asyncio.Event-like; set when cancellation was requested.
        deadline_ms: Optional absolute wall-clock deadline (epoch ms) for the node.
        logger: Logger adapter configured by the runtime.
        clock: Clock abstraction (see core.time).
    """

    task_id: str
    node_id: str
    attempt_epoch: int
    params: Mapping[str, Any]
    vars: Mapping[str, Any]
    externals: Mapping[str, Any]
    cancel_flag: Any
    deadline_ms: int | None
    logger: Any
    clock: Any

    # Optional scratchpad for handler-local state (small, low-cardinality).
    scratch: MutableMapping[str, Any] = field(default_factory=dict)


@dataclass
class HandlerResult:
    """
    Per-batch processing result emitted by a handler.

    The runtime converts this into protocol events (BATCH_OK / BATCH_FAILED)
    and applies delivery policies (retries, DLQ) if needed.
    """

    success: bool
    metrics: MutableMapping[str, Any] = field(default_factory=dict)
    artifacts_ref: Mapping[str, Any] | None = None
    reason_code: str | None = None
    permanent: bool = False
    error: str | None = None


@dataclass
class FinalizeResult:
    """
    Optional result produced at the end of the run (aggregation/final artifacts).
    """

    metrics: MutableMapping[str, Any] = field(default_factory=dict)
    artifacts_ref: Mapping[str, Any] | None = None


# --------------------------------------------------------------------------- #
# Contracts
# --------------------------------------------------------------------------- #


@runtime_checkable
class RoleHandler(Protocol):
    """
    Protocol for role handlers.

    Implementations should be stateless between *different* tasks. Use the
    provided `HandlerContext.scratch` to keep small per-run state if necessary.
    """

    role: str  # stable role name used for registration/lookup

    async def open(self, ctx: HandlerContext, config: Mapping[str, Any]) -> None: ...
    async def handle(self, batch: Batch, ctx: HandlerContext) -> HandlerResult: ...
    async def close(self, ctx: HandlerContext) -> FinalizeResult | None: ...

    def classify_error(self, exc: BaseException) -> tuple[str, bool]: ...

    # returns (reason_code, permanent) where permanent=True disables retries


class BaseHandler:
    """
    Convenience base class that implements sensible defaults:

      - no-op `open` and `close`
      - a conservative `classify_error` mapping runtime/public errors to
        (reason_code, permanent) pairs.

    Subclasses must implement `role` and `handle(...)`.
    """

    role: str  # must be overridden by subclasses

    async def open(self, ctx: HandlerContext, config: Mapping[str, Any]) -> None:
        """Called once at the beginning of the run (before first batch)."""
        return None

    async def handle(self, batch: Batch, ctx: HandlerContext) -> HandlerResult:  # pragma: no cover - abstract
        """Process a single batch. Must be overridden."""
        raise NotImplementedError

    async def close(self, ctx: HandlerContext) -> FinalizeResult | None:
        """Called once when the run finishes (successfully or after failure)."""
        return None

    # --- error policy ----------------------------------------------------- #

    def classify_error(self, exc: BaseException) -> tuple[str, bool]:
        """
        Map an exception to (reason_code, permanent). The runtime uses this to
        decide retry vs. fast-fail.

        Override for domain-specific policies; call `super()` for defaults and
        post-process if needed.
        """
        if isinstance(exc, CancelledError):
            return ("cancelled", True)
        if isinstance(exc, DeadlineExceeded):
            return ("deadline_exceeded", True)
        if isinstance(exc, PermanentError):
            return ("permanent_error", True)
        if isinstance(exc, RetryableError):
            return ("retryable_error", False)
        if isinstance(exc, FlowkitError):
            # Unknown FlowKit error → assume permanent to avoid retry storms
            return ("flowkit_error", True)
        # Non-FlowKit exceptions default to retryable (transient) to be safer
        return ("unclassified_error", False)
