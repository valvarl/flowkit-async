# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

from flowkit.api import PLUGIN_REGISTRY

from .artifacts import ArtifactStore
from .base.uow import UnitOfWork
from .kv import KVStore
from .offsets import CheckpointStore, OffsetStore
from .outbox import OutboxStore
from .tasks import TaskStore


@dataclass
class StorageFacade:
    """
    A resolved set of stores provided by a concrete backend.
    """

    uow: UnitOfWork
    kv: KVStore
    checkpoints: CheckpointStore
    offsets: OffsetStore
    outbox: OutboxStore
    artifacts: ArtifactStore
    tasks: TaskStore

    async def close(self) -> None:
        if hasattr(self.uow, "close"):
            await self.uow.close()  # type: ignore[func-returns-value]


async def build_storage(dsn: str, **kwargs) -> StorageFacade:
    """
    Resolve provider by DSN scheme and construct a StorageFacade.
    Example DSNs: mongodb://..., postgresql://..., redis://..., sqlite://...
    """
    scheme = urlparse(dsn).scheme
    provider = PLUGIN_REGISTRY.storage_provider(scheme)
    return await provider.factory(dsn=dsn, **kwargs)
