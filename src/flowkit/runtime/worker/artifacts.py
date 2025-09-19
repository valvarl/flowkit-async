# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing import Any, Mapping, Optional

from ...storage.artifacts import ArtifactStore


class ArtifactsWriter:
    """
    Idempotent artifact writer for worker outputs.

    It delegates to the DB-agnostic `ArtifactStore`:
      - partial upserts (by batch_uid)
      - final upsert (no batch_uid)
      - failure marks
    """

    def __init__(self, store: ArtifactStore, *, task_id: str, node_id: str, attempt_epoch: int) -> None:
        self._store = store
        self._task = task_id
        self._node = node_id
        self._epoch = int(attempt_epoch)

    async def upsert_partial(self, *, batch_uid: str, ref: Any, meta: Mapping[str, Any] | None = None) -> None:
        await self._store.upsert_partial(
            task_id=self._task, node_id=self._node, batch_uid=batch_uid, ref=ref, meta=dict(meta or {})
        )

    async def upsert_final(self, *, ref: Any, meta: Mapping[str, Any] | None = None) -> None:
        await self._store.upsert_final(task_id=self._task, node_id=self._node, ref=ref, meta=dict(meta or {}))

    async def mark_failed(self, *, batch_uid: str, reason: str) -> None:
        await self._store.mark_failed(task_id=self._task, node_id=self._node, batch_uid=batch_uid, reason=reason)
