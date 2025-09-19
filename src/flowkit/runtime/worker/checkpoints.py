# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing import Any, Optional

from ...api.streams import Checkpoint
from ...storage.offsets import CheckpointStore as OffsetsStoreProtocol


class CheckpointsStore:
    """
    Thin wrapper over a DB-agnostic offsets store.

    Scope:
      - Task + Node + Attempt.
      - Keys are adapter-defined (e.g., topic/partition, pagination path).
      - Values are `api.streams.Checkpoint` (JSON-serializable).

    The StreamEngine should persist checkpoints ONLY after durable delivery to sinks.
    """

    def __init__(
        self,
        *,
        offsets: OffsetsStoreProtocol,
        task_id: str,
        node_id: str,
        attempt_epoch: int,
    ) -> None:
        self._offsets = offsets
        self._task_id = task_id
        self._node_id = node_id
        self._attempt = int(attempt_epoch)

    async def read(self, key: str) -> Optional[Checkpoint]:
        """
        Load the last committed checkpoint for a given external key.
        Returns None if absent.
        """
        data = await self._offsets.get(
            task_id=self._task_id,
            node_id=self._node_id,
            attempt_epoch=self._attempt,
            key=key,
        )
        if data is None:
            return None
        # Accept both raw dicts and already-wrapped Checkpoint
        if isinstance(data, Checkpoint):
            return data
        return Checkpoint(token=data)

    async def save(self, key: str, ckpt: Checkpoint) -> None:
        """
        Persist a checkpoint for the given key. Overwrites previous value.
        """
        await self._offsets.put(
            task_id=self._task_id,
            node_id=self._node_id,
            attempt_epoch=self._attempt,
            key=key,
            token=dict(ckpt.token),
        )
