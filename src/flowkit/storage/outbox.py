# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class OutboxRecord:
    """A pending/retry outbox record fetched for dispatch."""

    rec_id: Any  # store-specific primary key
    topic: str
    key: str | None
    envelope: Mapping[str, Any]
    attempts: int


@runtime_checkable
class OutboxStore(Protocol):
    """
    Minimal persistence contract for the coordinator's outbox.

    Implementations may use Mongo, Postgres, Redis, etc. The store is
    responsible for durable state transitions and selection ordering.
    """

    async def enqueue(self, *, topic: str, key: str | None, envelope: Mapping[str, Any], fp: str | None) -> None:
        """Insert a new 'pending' record (idempotent if fp is provided)."""
        ...

    async def next_batch(self, *, now_ms: int, limit: int) -> AsyncIterator[OutboxRecord]:
        """
        Yield pending/retry records with next_attempt_at_ms <= now_ms in a stable order.
        Must ensure each yielded record is 'locked' (or otherwise not yielded twice concurrently).
        """
        ...

    async def mark_sent(self, rec_id: Any) -> None:
        """Transition record to 'sent' with timestamp."""
        ...

    async def mark_failed(self, rec_id: Any, *, attempts: int, error: str) -> None:
        """Transition record to 'failed' and store the final error."""
        ...

    async def schedule_retry(self, rec_id: Any, *, attempts: int, next_attempt_at_ms: int, error: str) -> None:
        """Transition record to 'retry' with incremented attempts and backoff time."""
        ...
