# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
FlowKit public extension API.

This module re-exports stable contracts for building custom adapters, transforms,
outputs (destinations), externals, hook actions, and expression-based integrations.
"""

# Errors
# Adapter & transform contracts
from .adapters import (
    AdapterContext,
    AdapterKind,
    Capabilities,
    OutputAdapter,
    Plugin,
    SourceAdapter,
    TransformOp,
)
from .errors import (
    Backpressure,
    CancelledError,
    DeadlineExceeded,
    FlowkitError,
    NotReady,
    PermanentError,
    RetryableError,
    SchemaMismatch,
)

# Expressions
from .expr import Expr, ExprError, compile_expr, eval_expr

# Externals
from .externals import (
    DistributedLock,
    ExternalError,
    ExternalProvider,
    ExternalReady,
    HttpEndpoint,
    KafkaTopic,
    RateLimiter,
    SecretStore,
)

# Hooks
from .hooks import HookAction, HookContext, HookEvent, HookSelector

# Registry
from .registry import PluginRegistry

# Streams / content model
from .streams import (
    Ack,
    Batch,
    Checkpoint,
    ContentKind,
    FrameDescriptor,
    Item,
    Nack,
)

__all__ = [
    # errors
    "FlowkitError",
    "RetryableError",
    "PermanentError",
    "CancelledError",
    "DeadlineExceeded",
    "SchemaMismatch",
    "NotReady",
    "Backpressure",
    # streams
    "ContentKind",
    "FrameDescriptor",
    "Checkpoint",
    "Item",
    "Batch",
    "Ack",
    "Nack",
    # adapters
    "AdapterKind",
    "Capabilities",
    "AdapterContext",
    "SourceAdapter",
    "OutputAdapter",
    "TransformOp",
    "Plugin",
    # externals
    "ExternalError",
    "ExternalReady",
    "ExternalProvider",
    "KafkaTopic",
    "HttpEndpoint",
    "SecretStore",
    "DistributedLock",
    "RateLimiter",
    # hooks
    "HookEvent",
    "HookContext",
    "HookAction",
    "HookSelector",
    # expr
    "Expr",
    "ExprError",
    "compile_expr",
    "eval_expr",
    # registry
    "PluginRegistry",
]
