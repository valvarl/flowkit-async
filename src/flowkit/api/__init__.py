# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
FlowKit public extension API.

This module re-exports stable contracts for building custom adapters, transforms,
outputs (destinations), externals, hook actions, and expression-based integrations.
"""

# Errors
from .errors import (
    FlowkitError,
    RetryableError,
    PermanentError,
    CancelledError,
    DeadlineExceeded,
    SchemaMismatch,
    NotReady,
    Backpressure,
)

# Streams / content model
from .streams import (
    ContentKind,
    FrameDescriptor,
    Checkpoint,
    Item,
    Batch,
    Ack,
    Nack,
)

# Adapter & transform contracts
from .adapters import (
    AdapterKind,
    Capabilities,
    AdapterContext,
    SourceAdapter,
    OutputAdapter,
    TransformOp,
    Plugin,
)

# Externals
from .externals import (
    ExternalError,
    ExternalReady,
    ExternalProvider,
    KafkaTopic,
    HttpEndpoint,
    SecretStore,
    DistributedLock,
    RateLimiter,
)

# Hooks
from .hooks import HookEvent, HookContext, HookAction, HookSelector

# Expressions
from .expr import Expr, ExprError, compile_expr, eval_expr

# Registry
from .registry import PluginRegistry

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
