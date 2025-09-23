# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Mongo Unit-of-Work: wraps a session/transaction lifecycle.
"""

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from flowkit.storage.base.uow import UnitOfWork


class MongoUoW(UnitOfWork):  # type: ignore[misc]
    def __init__(self, client: AsyncIOMotorClient, db: AsyncIOMotorDatabase) -> None:
        self._client = client
        self._db = db
        self._session = None  # type: Optional[any]

    @property
    def session(self):
        return self._session

    def require_tx(self) -> None:
        if self._session is None:
            raise RuntimeError("MongoUoW: transaction/session is required but not started")

    async def __aenter__(self) -> MongoUoW:
        # Start a session and a transaction (requires replica set)
        self._session = await self._client.start_session()
        self._session.start_transaction()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                await self._session.commit_transaction()
            else:
                await self._session.abort_transaction()
        finally:
            await self._session.end_session()
            self._session = None

    async def close(self) -> None:
        # Motor client close() is sync, but it's OK to call here.
        self._client.close()
