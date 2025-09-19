# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Task/node variables store (coordinator side).

The goal is to provide simple, idempotent operations (set/merge/incr) on a
small JSON-like dict of task-scoped variables, with optimistic CAS semantics.
This module intentionally hides the underlying persistence (KV/Mongo/PG/etc).

Usage:
    vars = VarsStore(backend=KVVarsBackend(kv))
    await vars.apply_ops(task_id, [
        {"op": "set", "key": "stage", "value": "online"},
        {"op": "merge", "key": "stats", "value": {"batches": 10}},
        {"op": "incr", "key": "retries", "by": 1},
    ])
"""

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Mapping, MutableMapping, Protocol, Optional


# ---- Backend protocol --------------------------------------------------------


class VarsBackend(Protocol):
    """
    Minimal backend for task variables. Implementations must ensure that
    read-modify-write is atomic (CAS) or retry-safe to preserve idempotency.
    """

    async def load(self, task_id: str) -> Mapping[str, Any]:
        """Return the current variables map (empty if missing)."""
        ...

    async def cas_update(
        self,
        task_id: str,
        updater: Callable[[Dict[str, Any]], Dict[str, Any]],
    ) -> bool:
        """
        Perform optimistic update: read current value, call `updater` with a
        mutable copy, and write back iff the value has not changed.
        Returns True when the write succeeded, False if contention was detected.

        Implementations can use version fields, etags, SELECT FOR UPDATE, etc.
        """
        ...


# ---- Public API --------------------------------------------------------------


@dataclass
class VarsStore:
    """
    Coordinator-facing API over task-level variables.

    Operations:
      - set(key, value)
      - merge(key, mapping)
      - incr(key, by)

    Bulk application uses a single CAS update with retries to avoid partial
    application and to keep per-op idempotency.
    """

    backend: VarsBackend
    max_retries: int = 8

    async def get_all(self, task_id: str) -> Dict[str, Any]:
        return dict(await self.backend.load(task_id))

    async def set(self, task_id: str, key: str, value: Any) -> None:
        await self.apply_ops(task_id, [{"op": "set", "key": key, "value": value}])

    async def merge(self, task_id: str, key: str, value: Mapping[str, Any]) -> None:
        await self.apply_ops(task_id, [{"op": "merge", "key": key, "value": dict(value)}])

    async def incr(self, task_id: str, key: str, by: int = 1) -> int:
        out = await self.apply_ops(task_id, [{"op": "incr", "key": key, "by": by}])
        # return new value if present
        return int(out.get(key, 0)) if isinstance(out.get(key), int) else 0

    async def apply_ops(self, task_id: str, ops: list[dict[str, Any]]) -> Dict[str, Any]:
        """
        Apply a list of VarOps atomically (best effort with CAS retries).
        Returns the resulting variables map (as seen by the successful write).
        """
        retries = 0
        while True:
            latest: Dict[str, Any] = {}

            def _updater(cur: Dict[str, Any]) -> Dict[str, Any]:
                # We mutate a copy provided by backend.
                latest.clear()
                latest.update(cur)
                for op in ops:
                    kind = op.get("op")
                    key = op.get("key")
                    if not key or not isinstance(key, str):
                        raise ValueError("VarOp.key must be a non-empty string")
                    if kind == "set":
                        if "value" not in op:
                            raise ValueError("VarOp.set requires 'value'")
                        latest[key] = op["value"]
                    elif kind == "merge":
                        val = op.get("value")
                        if not isinstance(val, Mapping):
                            raise ValueError("VarOp.merge requires 'value' as a mapping")
                        base = latest.get(key)
                        if not isinstance(base, dict):
                            base = {}
                        merged = dict(base)
                        merged.update(dict(val))
                        latest[key] = merged
                    elif kind == "incr":
                        by = int(op.get("by", 1))
                        cur_val = latest.get(key, 0)
                        if not isinstance(cur_val, int):
                            cur_val = 0
                        latest[key] = int(cur_val) + by
                    else:
                        raise ValueError(f"Unsupported VarOp.op: {kind!r}")
                return latest

            ok = await self.backend.cas_update(task_id, _updater)
            if ok:
                return latest
            retries += 1
            if retries > self.max_retries:
                raise RuntimeError("VarsStore CAS retries exceeded")
