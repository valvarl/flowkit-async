# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Worker-side checkpoint helpers.

This module provides two tiny adapters around the generic
`storage.offsets.CheckpointStore` (scope/key API):

1) WorkerCheckpointStoreAdapter — stateless bridge that exposes
   get/put/delete with explicit (task_id, node_id, attempt_epoch, key).

2) CheckpointsStore — stateful convenience wrapper bound to a single
   (task_id, node_id, attempt_epoch) so the engine can just read/save by `key`.

Both adapters accept/return `api.streams.Checkpoint` and gracefully
coerce plain token dicts into Checkpoint where implementations choose to
store raw tokens.
"""

from typing import Any

from ...api.streams import Checkpoint
from ...storage.offsets import CheckpointStore as CheckpointStoreProtocol

# ---- low-level stateless bridge ---------------------------------------------


class WorkerCheckpointStoreAdapter:
    """
    Bridge from generic (scope, key) CheckpointStore to worker-style keys.

    Scope format (stable, human-readable):
        "{ns}|task:{task_id}|node:{node_id}|attempt:{attempt_epoch}"

    Default namespace is "worker.ckpt" to avoid collisions with other users
    of the same CheckpointStore.
    """

    def __init__(self, base: CheckpointStoreProtocol, *, scope_ns: str = "worker.ckpt") -> None:
        self._base = base
        self._ns = scope_ns

    def _scope(self, *, task_id: str, node_id: str, attempt_epoch: int) -> str:
        return f"{self._ns}|task:{task_id}|node:{node_id}|attempt:{int(attempt_epoch)}"

    async def get(self, *, task_id: str, node_id: str, attempt_epoch: int, key: str) -> Checkpoint | None:
        scope = self._scope(task_id=task_id, node_id=node_id, attempt_epoch=attempt_epoch)
        cp = await self._base.get(scope, key)
        if cp is None:
            return None
        return cp if isinstance(cp, Checkpoint) else Checkpoint(token=cp)  # tolerant to raw tokens

    async def put(
        self, *, task_id: str, node_id: str, attempt_epoch: int, key: str, token: Checkpoint | dict[str, Any]
    ) -> None:
        scope = self._scope(task_id=task_id, node_id=node_id, attempt_epoch=attempt_epoch)
        cp = token if isinstance(token, Checkpoint) else Checkpoint(token=token)
        await self._base.put(scope, key, cp)

    async def delete(self, *, task_id: str, node_id: str, attempt_epoch: int, key: str) -> None:
        scope = self._scope(task_id=task_id, node_id=node_id, attempt_epoch=attempt_epoch)
        if hasattr(self._base, "delete"):
            await self._base.delete(scope, key)  # type: ignore[attr-defined]


# ---- stateful convenience wrapper -------------------------------------------


class CheckpointsStore:
    """
    Stateful wrapper bound to a specific (task_id, node_id, attempt_epoch).

    Usage:
        ckpts = CheckpointsStore(store, task_id=..., node_id=..., attempt_epoch=...)
        last = await ckpts.read("kafka:topic:p=0")
        await ckpts.save("kafka:topic:p=0", cp)

    The StreamEngine should persist checkpoints ONLY after durable delivery to sinks.
    """

    def __init__(
        self,
        *,
        offsets: CheckpointStoreProtocol,
        task_id: str,
        node_id: str,
        attempt_epoch: int,
        scope_ns: str = "worker.ckpt",
    ) -> None:
        self._store = offsets
        self._task_id = task_id
        self._node_id = node_id
        self._attempt = int(attempt_epoch)
        self._ns = scope_ns

    def _scope(self) -> str:
        return f"{self._ns}|task:{self._task_id}|node:{self._node_id}|attempt:{self._attempt}"

    async def read(self, key: str) -> Checkpoint | None:
        """
        Load the last committed checkpoint for a given external key.
        Returns None if absent.
        """
        cp = await self._store.get(self._scope(), key)
        if cp is None:
            return None
        return cp if isinstance(cp, Checkpoint) else Checkpoint(token=cp)

    async def save(self, key: str, ckpt: Checkpoint | dict[str, Any]) -> None:
        """
        Persist a checkpoint for the given key. Overwrites previous value.
        """
        cp = ckpt if isinstance(ckpt, Checkpoint) else Checkpoint(token=ckpt)
        await self._store.put(self._scope(), key, cp)

    async def delete(self, key: str) -> None:
        """Delete a checkpoint for the given key (best-effort; optional)."""
        if hasattr(self._store, "delete"):
            await self._store.delete(self._scope(), key)  # type: ignore[attr-defined]
