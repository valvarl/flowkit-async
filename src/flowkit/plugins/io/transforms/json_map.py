# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
JSON mapping transform.

Two complementary modes are supported:

1) Expression mode (recommended):
   Args:
     - expr: str
     - merge: bool = False  (merge result into existing payload instead of replace)

   The expression is evaluated with the same environment as FilterExpr and is
   expected to return a JSON-serializable object. If `merge=True` and the
   current payload is a dict, shallow-merge is applied.

2) Mapping mode (simple projection/rename):
   Args:
     - set: dict[str, str]
     - merge: bool = False

   `set` maps output keys to dotted paths or small expressions (resolved with
   the same engine as FilterExpr). Example:
     {"id": "payload.id", "score": "payload.meta.score or 0"}

Behavior:
  - For Items: payload is transformed according to the chosen mode.
  - For Batches: per-item mapping; output Batch is returned.
  - Non-record batches are not handled by this operator.

Notes:
  - On evaluation errors, the element is passed through unchanged (best-effort).
"""

from collections.abc import Mapping
from typing import Any

from ....api.adapters import AdapterContext, Capabilities, TransformOp
from ....api.streams import Batch, ContentKind, Item
from .filter_expr import _ENGINE  # reuse the same expression engine


def _get_dotted(obj: Any, path: str) -> Any:
    cur: Any = obj
    for p in path.split("."):
        if isinstance(cur, Mapping) and p in cur:
            cur = cur[p]
        else:
            return None
    return cur


class JsonMap(TransformOp):
    name = "json.map"
    capabilities = Capabilities(
        accepts=frozenset({ContentKind.RECORD}),
        emits=frozenset({ContentKind.RECORD}),
        bounded_memory=True,
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._expr: str | None = kwargs.get("expr") or None
        self._set: Mapping[str, Any] | None = kwargs.get("set")
        self._merge: bool = bool(kwargs.get("merge", False))
        if not self._expr and not self._set:
            raise ValueError("json.map requires either 'expr' or 'set'")

    async def open(self, ctx: AdapterContext, **args: Any) -> None:
        return

    async def process(self, x: Item | Batch):
        if isinstance(x, Batch) and x.items is not None:
            out: list[Item] = []
            for it in x.items:
                if not isinstance(it, Item):
                    it = Item(payload=getattr(it, "payload", it), meta={})
                out.append(self._map_item(it))
            return Batch(items=out, frame=None)

        if isinstance(x, Item):
            return self._map_item(x)

        return None

    async def close(self) -> None:
        return

    # ---- helpers -------------------------------------------------------------

    def _env(self, it: Item) -> dict[str, Any]:
        return {
            "item": {"payload": it.payload, "meta": it.meta},
            "payload": it.payload,
            "meta": it.meta,
            "params": self._ctx.params,
            "vars": self._ctx.vars,
        }

    def _map_item(self, it: Item) -> Item:
        try:
            if self._expr:
                res = _ENGINE(self._expr, self._env(it))
                if self._merge and isinstance(it.payload, dict) and isinstance(res, dict):
                    new_payload = {**it.payload, **res}
                else:
                    new_payload = res
                it.payload = new_payload
                return it

            # mapping mode
            if self._set is not None:
                out: dict[str, Any] = {}
                env = self._env(it)
                for k, v in self._set.items():
                    if isinstance(v, str):
                        # First try expression; if it fails, fallback to dotted path
                        try:
                            out[k] = _ENGINE(v, env)
                            continue
                        except Exception:
                            pass
                        if v.startswith("payload."):
                            out[k] = _get_dotted(it.payload, v.removeprefix("payload."))
                            continue
                        if v.startswith("meta."):
                            out[k] = _get_dotted(it.meta, v.removeprefix("meta."))
                            continue
                        # plain key lookup
                        out[k] = _get_dotted({"payload": it.payload, "meta": it.meta}, v)
                    else:
                        out[k] = v
                if self._merge and isinstance(it.payload, dict):
                    it.payload = {**it.payload, **out}
                else:
                    it.payload = out
                return it

        except Exception:
            # Best-effort: pass through unchanged on errors
            return it

        return it
