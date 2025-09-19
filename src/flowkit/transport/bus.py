# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Transport abstraction over a message bus.

This module defines:
- `Bus` protocol: lifecycle + send + consumer factory.
- `Consumer` protocol: async-iterable stream of `Received` messages and manual commits.
- `Received`: normalized envelope + delivery metadata for downstream handlers.

Concrete implementations (e.g., Kafka) live in their own modules and may offer
additional knobs via their constructors, but must satisfy these protocols.
"""

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ..protocol.messages import Envelope


@dataclass(frozen=True)
class Received:
    """
    A single message fetched from the bus.

    Attributes:
        topic: Source topic/stream name.
        partition: Zero-based partition index (if the backend supports partitions).
        offset: Monotonic offset within the partition (if applicable).
        key: Optional message key (raw bytes; encoding is transport-specific).
        headers: Optional map of headers (decoded as bytes where possible).
        envelope: Parsed FlowKit `Envelope`.
        raw: Optional backend-specific record object to allow advanced commit semantics.
    """

    topic: str
    partition: int | None
    offset: int | None
    key: bytes | None
    headers: Mapping[str, bytes] | None
    envelope: Envelope
    raw: object | None = None


@runtime_checkable
class Consumer(Protocol):
    """
    An async-iterable message consumer.

    Notes:
        - When `manual_commit=True` was requested at creation time, the caller
          should call `commit()` once it is safe to advance offsets.
        - Implementations may provide `commit_message(msg)` to support per-message
          commits. The generic protocol keeps only `commit()` for portability.
    """

    async def stop(self) -> None: ...
    async def commit(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[Received]: ...


@runtime_checkable
class Bus(Protocol):
    """
    Abstract bus for Cmd/Events/Query/Reply/Signals.

    Implementations should:
      - provide idempotent `start()`/`stop()`,
      - serialize/deserialize `Envelope` safely,
      - expose a consumer factory with manual commit support,
      - MAY implement a simple `reply` correlator keyed by `Envelope.corr_id`.
    """

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def send(self, topic: str, key: bytes | None, env: Envelope) -> None: ...

    async def new_consumer(
        self,
        topics: list[str],
        group_id: str,
        *,
        manual_commit: bool = True,
    ) -> Consumer: ...
