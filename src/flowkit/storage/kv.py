# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Key/Value storage and distributed locks (DB-agnostic).

This module defines a minimal, async KV interface suitable for:
- task-level variables (small JSON-like maps),
- lightweight coordination (CAS/incr),
- coarse-grained distributed locks with TTL.

Implementations may use Redis, Mongo, Postgres, etc. The interface purposefully
does not prescribe persistence details.
"""

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Optional, Protocol, runtime_checkable


__all__ = [
    "KVError",
    "LockError",
    "LockInfo",
    "KVStore",
]


class KVError(RuntimeError):
    """Base error for KV operations."""


class LockError(KVError):
    """Raised on lock acquisition/renew/release failures."""


@dataclass(frozen=True)
class LockInfo:
    """
    Introspective lock info for observability and debugging.

    Attributes:
        name: Lock name (global namespace within the store).
        owner: Owner identifier (e.g., worker_id or coordinator instance id).
        token: Opaque token returned to the owner on acquire; must be presented to release.
        deadline_ms: Epoch milliseconds when the lock expires (best-effort).
        meta: Optional metadata provided by the owner.
    """
    name: str
    owner: str
    token: str
    deadline_ms: int
    meta: Mapping[str, Any] | None = None


@runtime_checkable
class KVStore(Protocol):
    """
    Minimal async KV interface.

    Notes:
        - All keys are strings; values are JSON-serializable objects.
        - Implementations SHOULD apply reasonable size limits (e.g., < 1MB).
        - CAS is compare-by-value semantics; implementations may use hashing.
    """

    # -------- Plain KV --------

    async def get(self, key: str) -> Any | None: ...
    async def mget(self, keys: list[str]) -> list[Any | None]: ...
    async def set(self, key: str, value: Any, *, ttl_ms: int | None = None) -> None: ...
    async def delete(self, key: str) -> None: ...
    async def incr(self, key: str, *, by: int = 1, ttl_ms: int | None = None) -> int: ...
    async def expire(self, key: str, ttl_ms: int) -> bool: ...
    async def ttl(self, key: str) -> int | None:
        """
        Return remaining TTL in milliseconds, None if key has no TTL, or -1 if not found.
        """ ...

    async def cas(self, key: str, expect: Any, update: Any, *, ttl_ms: int | None = None) -> bool:
        """
        Compare-and-set: if current value equals `expect`, set to `update` and return True;
        otherwise return False without modifying the key.
        """ ...

    # -------- Locks --------

    async def acquire_lock(
        self,
        name: str,
        *,
        owner: str,
        ttl_ms: int,
        meta: Mapping[str, Any] | None = None,
    ) -> LockInfo | None:
        """
        Try to acquire a lock. Return LockInfo on success, or None if held by someone else.
        MUST be safe to call repeatedly (idempotent for the same owner if implementation chooses so).
        """ ...

    async def renew_lock(self, name: str, *, owner: str, token: str, ttl_ms: int) -> bool:
        """
        Extend the lock TTL if the caller owns it (by matching token). Return True if renewed.
        """ ...

    async def release_lock(self, name: str, *, owner: str, token: str) -> bool:
        """
        Release the lock if owned. Return True if the lock was released.
        """ ...

    async def get_lock(self, name: str) -> LockInfo | None:
        """Return current lock info, if any (best-effort).""" ...
