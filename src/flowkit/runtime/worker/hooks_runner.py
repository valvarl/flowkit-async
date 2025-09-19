# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing import Any, Mapping, Optional

from ...api.registry import PluginRegistry
from ...api.streams import Item, Batch


class WorkerHooksRunner:
    """
    Worker-side hook dispatcher (best-effort, non-fatal).

    Hook names (suggested):
      - "source.item"   (args: item, source_alias, node, task)
      - "output.sent"   (args: element, sink, node, task)
      - "engine.error"  (args: error, stage, node, task)
    """

    def __init__(self, registry: Optional[PluginRegistry] = None) -> None:
        self._registry = registry

    async def emit_source_item(self, *, item: Item | Batch, source_alias: str, node_id: str, task_id: str) -> None:
        await self._emit("source.item", item=item, source_alias=source_alias, node_id=node_id, task_id=task_id)

    async def emit_output_sent(self, *, element: Item | Batch, sink: str, node_id: str, task_id: str) -> None:
        await self._emit("output.sent", element=element, sink=sink, node_id=node_id, task_id=task_id)

    async def emit_engine_error(self, *, error: BaseException, stage: str, node_id: str, task_id: str) -> None:
        await self._emit("engine.error", error=str(error), stage=stage, node_id=node_id, task_id=task_id)

    # ---- internal

    async def _emit(self, name: str, **kwargs: Any) -> None:
        if not self._registry:
            return
        try:
            if hasattr(self._registry, "emit"):
                res = self._registry.emit(name, **kwargs)  # type: ignore[attr-defined]
                if hasattr(res, "__await__"):
                    await res
        except Exception:
            # hooks are best-effort; never fail the pipeline
            return
