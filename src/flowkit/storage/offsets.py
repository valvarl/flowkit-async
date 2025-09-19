# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Checkpoint/offset storage interfaces.

Two flavors:
- CheckpointStore: generic scope+key -> Checkpoint mapping.
- OffsetStore: task/node/source/partition keyed offsets for streaming inputs.

Both return/accept `flowkit.api.streams.Checkpoint`, which is a JSON-serializable
token controlled by adapters (Kafka offsets, HTTP cursors, etc).
"""

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Protocol, runtime_checkable

from ..api.streams import Checkpoint


__all__ = [
    "OffsetError",
    "CheckpointStore",
    "OffsetKey",
    "OffsetStore",
]


class OffsetError(RuntimeError):
    """Base error for checkpoint/offset operations."""


@runtime_checkable
class CheckpointStore(Protocol):
    """
    Simple scope/key checkpoint store.

    Examples:
        scope="secrets.scan", key="task:123|node:index|src:from_kafka|p:5"
    """

    async def get(self, scope: str, key: str) -> Checkpoint | None: ...
    async def put(self, scope: str, key: str, cp: Checkpoint) -> None: ...
    async def delete(self, scope: str, key: str) -> None: ...


@dataclass(frozen=True)
class OffsetKey:
    """
    Canonical offset key for streaming sources bound to a task/node.

    Attributes:
        task_id: Task identifier.
        node_id: Node within the task.
        source_alias: Input source alias declared in the node spec.
        partition: Logical partition identifier (stringified).
    """

    task_id: str
    node_id: str
    source_alias: str
    partition: str

    def as_scope_key(self) -> tuple[str, str]:
        """
        Convert to (scope, key) pair suitable for CheckpointStore implementations.

        Format (stable, human-readable):
            scope = "task:{task_id}|node:{node_id}|src:{source_alias}"
            key   = "p:{partition}"
        """
        scope = f"task:{self.task_id}|node:{self.node_id}|src:{self.source_alias}"
        key = f"p:{self.partition}"
        return scope, key


@runtime_checkable
class OffsetStore(Protocol):
    """
    Specialised offset API keyed by OffsetKey.

    Notes:
        - `commit` MUST be idempotent (last write wins).
        - `commit_many` MAY be implemented as a transaction/batch where supported.
        - Implementations MAY compress tokens or apply TTLs where appropriate.
    """

    async def read(self, key: OffsetKey) -> Checkpoint | None: ...
    async def commit(self, key: OffsetKey, cp: Checkpoint) -> None: ...
    async def delete(self, key: OffsetKey) -> None: ...

    async def read_many(self, keys: Iterable[OffsetKey]) -> list[Checkpoint | None]: ...
    async def commit_many(self, pairs: Iterable[tuple[OffsetKey, Checkpoint]]) -> None: ...
