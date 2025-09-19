# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any, Iterable, Iterator, Mapping, Optional, Union, AsyncIterator

from ...api.adapters import TransformOp, AdapterContext
from ...api.registry import PluginRegistry
from ...api.streams import Item, Batch


StreamEl = Union[Item, Batch]
MaybeMany = Union[None, StreamEl, Iterable[StreamEl], AsyncIterator[StreamEl]]


def _iter_flat(x: MaybeMany) -> Iterator[StreamEl]:
    if x is None:
        return
        yield  # pragma: no cover
    if isinstance(x, (Item, Batch)):
        yield x
        return
    if hasattr(x, "__aiter__"):
        # Async iterator flattening (turn into sync generator via small buffer).
        # Engine uses this only inside async contexts anyway.
        raise TypeError("AsyncIterator is not allowed directly in OpsChain.process; use engine pipe")
    for el in x:  # type: ignore[assignment]
        yield el  # type: ignore[misc]


class OpsChain:
    """
    Compile & execute a transform chain for streaming pipelines.

    Contract (best effort, adapters may choose to differ):
      - Each op gets a single element (Item or Batch) and returns:
          * None                → drop     (filter)
          * Item|Batch          → one out  (map)
          * Iterable[Item|Batch]→ many out (flat_map)
      - Ops must be stateless or manage their state internally (close-safe).
      - Backpressure is handled by the engine via bounded queues.
    """

    def __init__(self, ctx: AdapterContext) -> None:
        self._ctx = ctx
        self._ops: list[TransformOp] = []

    async def build(self, ops_specs: list[Mapping[str, Any]], *, registry: PluginRegistry) -> None:
        """
        Instantiate ops via the plugin registry and open them.

        ops_specs examples (both forms supported):
          - {"name": "filter", "args": {"expr": "x.foo > 0"}}
          - {"op": {"name": "filter", "args": {...}}}
        """
        self._ops.clear()
        for spec in ops_specs or []:
            spec2 = spec.get("op", spec)
            name = spec2.get("name")
            args = spec2.get("args") or {}

            op = _create_transform(registry, name, self._ctx, args)
            if hasattr(op, "open"):
                maybe = op.open(self._ctx, **args)  # type: ignore[attr-defined]
                if hasattr(maybe, "__await__"):
                    await maybe  # async open
            self._ops.append(op)

    async def process(self, x: StreamEl) -> MaybeMany:
        """
        Run x through the chain. Short-circuits on None (drop).
        For fan-out ops, the caller should iterate the returned iterable.
        """
        current: MaybeMany = x
        for op in self._ops:
            if current is None:
                return None
            if isinstance(current, (Item, Batch)):
                current = await _op_process(op, current)
            else:
                # Fan-out: feed each element through the remaining chain
                out_buf: list[StreamEl] = []
                for el in _iter_flat(current):  # type: ignore[arg-type]
                    res = await _op_process(op, el)
                    out_buf.extend(list(_iter_flat(res)))
                current = out_buf
        return current

    async def close(self) -> None:
        for op in reversed(self._ops):
            if hasattr(op, "close"):
                maybe = op.close()  # type: ignore[attr-defined]
                if hasattr(maybe, "__await__"):
                    await maybe
        self._ops.clear()


# ---- helpers ----------------------------------------------------------------


def _create_transform(registry: PluginRegistry, name: str, ctx: AdapterContext, args: Mapping[str, Any]) -> TransformOp:
    # Common patterns supported:
    #  - registry.create(kind="transform", name, ctx=..., **args)
    #  - registry.get_transform(name)(ctx, **args)
    #  - callable factory returning TransformOp
    if hasattr(registry, "create"):
        try:
            op = registry.create(kind="transform", name=name, ctx=ctx, **args)
            if isinstance(op, TransformOp) or hasattr(op, "process"):
                return op  # type: ignore[return-value]
        except Exception:
            pass
    if hasattr(registry, "get_transform"):
        factory = registry.get_transform(name)  # type: ignore[attr-defined]
        return factory(ctx, **args)  # type: ignore[call-arg, return-value]
    # Fallback: name is a callable in args?
    factory = args.get("_factory")
    if callable(factory):
        return factory(ctx, **args)  # type: ignore[call-arg, return-value]
    raise LookupError(f"transform op '{name}' is not registered")


async def _op_process(op: TransformOp, x: StreamEl) -> MaybeMany:
    fn = getattr(op, "process", None)
    if fn is None:
        raise TypeError(f"transform op {op!r} has no 'process' method")
    res = fn(x)
    if hasattr(res, "__await__"):
        return await res  # type: ignore[no-any-return]
    return res  # type: ignore[no-any-return]
