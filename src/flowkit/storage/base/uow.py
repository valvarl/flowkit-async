# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import AsyncContextManager, Protocol


class UnitOfWork(Protocol, AsyncContextManager["UnitOfWork"]):
    """
    Provider-specific transactional scope.

    Implementations may be true database transactions (Mongo session/SQL txn),
    or best-effort scopes for non-transactional stores (Redis).
    """

    def require_tx(self) -> None:
        """
        Optional hint for providers to ensure a transactional session is present,
        or raise if not supported.
        """
        ...

    async def __aenter__(self) -> UnitOfWork: ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...

    async def close(self) -> None:
        """Optional cleanup hook for providers owning clients/sessions."""
        ...
