# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Adapter & Transform extension interfaces (public, stable).

This file defines the minimal contracts that input (sources), output adapters
(destinations) and transform operators must implement to be discovered and
executed by FlowKit.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncIterator, Iterable, Mapping, Optional, Protocol, runtime_checkable

from .errors import FlowkitError
from .streams import Batch, Item, FrameDescriptor, Checkpoint, ContentKind


class AdapterKind(str, Enum):
    """Declared adapter kind to aid diagnostics and registry tooling."""

    SOURCE = "source"
    OUTPUT = "output"
    TRANSFORM = "transform"


@dataclass(frozen=True)
class Capabilities:
    """
    Capabilities advertised by adapters/transforms for compile-time checks.

    Attributes:
        accepts: Content kinds the component accepts as input.
        emits: Content kinds the component can produce (output).
        supports_select: {"batches","final"} for sources.
        pause_resume: True if the source supports cooperative pause/resume.
        bounded_memory: True if memory usage is bounded by design (e.g., streaming).
    """

    accepts: frozenset[ContentKind]
    emits: frozenset[ContentKind]
    supports_select: frozenset[str] = frozenset({"batches", "final"})
    pause_resume: bool = True
    bounded_memory: bool = True


@dataclass
class AdapterContext:
    """
    Runtime context handed to adapters/transforms/outputs.

    Contains task/node identity, resolved externals, cancellation and timing.
    """

    task_id: str
    node_id: str
    attempt_epoch: int
    params: Mapping[str, Any]
    vars: Mapping[str, Any]
    externals: Mapping[str, Any]
    cancel_flag: Any  # asyncio.Event-like
    deadline_ms: Optional[int]
    checkpoint_store: Any  # runtime/worker/checkpoints.CheckpointsStore
    logger: Any
    clock: Any


@runtime_checkable
class SourceAdapter(Protocol):
    """
    Pull/notify/cursor source.

    Lifecycle:
        await open(ctx, spec)
        async for x in self: ...
        await ack(checkpoint) / await nack(error)
        await close()
    """

    name: str
    capabilities: Capabilities

    async def open(self, ctx: AdapterContext, spec: Mapping[str, Any]) -> None: ...
    def __aiter__(self) -> AsyncIterator[Item | Batch]: ...
    async def ack(self, checkpoint: Optional[Checkpoint]) -> None: ...
    async def nack(self, error: Optional[BaseException] = None) -> None: ...
    async def close(self) -> None: ...


@runtime_checkable
class TransformOp(Protocol):
    """
    Streaming-friendly transform.

    Input and output content kinds must match `capabilities`. Transform may emit
    zero, one, or many outputs per input element.
    """

    name: str
    capabilities: Capabilities

    async def open(self, ctx: AdapterContext, args: Mapping[str, Any]) -> None: ...
    async def process(self, x: Item | Batch) -> Iterable[Item | Batch] | Item | Batch | None: ...
    async def close(self) -> None: ...


@runtime_checkable
class OutputAdapter(Protocol):
    """
    Durable output (destination) with delivery semantics orchestrated by the runtime.

    The engine provides retries and idempotency keys; the output should be
    idempotent when the key is provided.
    """

    name: str
    capabilities: Capabilities

    async def open(self, ctx: AdapterContext, spec: Mapping[str, Any]) -> None: ...
    async def send(self, x: Item | Batch, *, idempotency_key: Optional[str] = None) -> None: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...


class Plugin(ABC):
    """
    Optional bundle of components to simplify distribution.

    A plugin can register multiple sources, outputs, transforms, externals and
    hook actions. It is discovered either via entry points or manually.
    """

    api_version: str = "3.0"

    @abstractmethod
    def sources(self) -> Mapping[str, type[SourceAdapter]]: ...
    @abstractmethod
    def outputs(self) -> Mapping[str, type[OutputAdapter]]: ...
    @abstractmethod
    def transforms(self) -> Mapping[str, type[TransformOp]]: ...

    # Optional parts
    def externals(self) -> Mapping[str, Any]:
        return {}

    def hook_actions(self) -> Mapping[str, Any]:
        return {}


__all__ = [
    "AdapterKind",
    "Capabilities",
    "AdapterContext",
    "SourceAdapter",
    "OutputAdapter",
    "TransformOp",
    "Plugin",
]
