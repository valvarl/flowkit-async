# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import random
from typing import Any, Iterable, Mapping, Optional

from ...api.adapters import SinkAdapter, AdapterContext
from ...api.registry import PluginRegistry
from ...api.streams import Item, Batch


class SinksRunner:
    """
    Delivery policy orchestrator for one output stage:
      - at-least-once semantics: retry on transient errors
      - idempotency keys: forwarded to sinks when supported
      - optional DLQ channel: last resort if retries exhausted
    """

    def __init__(self, ctx: AdapterContext) -> None:
        self._ctx = ctx
        self._sinks: list[tuple[str, SinkAdapter]] = []
        self._delivery: Mapping[str, Any] = {}
        self._dlq: Optional[SinkAdapter] = None

    async def build(self, output_spec: Mapping[str, Any], *, registry: PluginRegistry) -> None:
        """
        output_spec example:
        {
          "channels": [
             {"name": "emit.kafka", "args": {...}},
             {"name": "push.to_artifacts", "args": {...}}
          ],
          "delivery": {"retries": 5, "backoff_ms": 200, "backoff_max_ms": 5000, "jitter": 0.25},
          "dlq": {"name": "emit.s3", "args": {...}}
        }
        """
        self._sinks.clear()
        self._delivery = output_spec.get("delivery") or {}

        for ch in output_spec.get("channels") or []:
            name = ch.get("name")
            args = ch.get("args") or {}
            sink = _create_sink(registry, name, self._ctx, args)
            if hasattr(sink, "open"):
                maybe = sink.open(self._ctx, **args)  # type: ignore[attr-defined]
                if hasattr(maybe, "__await__"):
                    await maybe
            self._sinks.append((name, sink))

        dlq_spec = output_spec.get("dlq")
        if dlq_spec:
            self._dlq = _create_sink(registry, dlq_spec.get("name"), self._ctx, dlq_spec.get("args") or {})
            if hasattr(self._dlq, "open"):
                maybe = self._dlq.open(self._ctx, **(dlq_spec.get("args") or {}))  # type: ignore[attr-defined]
                if hasattr(maybe, "__await__"):
                    await maybe

    async def deliver(self, x: Item | Batch, *, idempotency_key: Optional[str]) -> None:
        """
        Deliver to all sinks with best-effort retries. If any sink exhausts retries,
        DLQ (if present) gets the element; otherwise the exception is propagated.
        """
        for name, sink in self._sinks:
            try:
                await _send_with_retry(
                    sink,
                    x,
                    idempotency_key=idempotency_key,
                    **_delivery_params(self._delivery),
                )
            except Exception:
                if self._dlq is not None:
                    await _send_with_retry(
                        self._dlq,
                        x,
                        idempotency_key=idempotency_key,
                        **_delivery_params(self._delivery, dlq=True),
                    )
                else:
                    raise

    async def flush(self) -> None:
        for _, sink in self._sinks:
            if hasattr(sink, "flush"):
                maybe = sink.flush()  # type: ignore[attr-defined]
                if hasattr(maybe, "__await__"):
                    await maybe
        if self._dlq and hasattr(self._dlq, "flush"):
            maybe = self._dlq.flush()  # type: ignore[attr-defined]
            if hasattr(maybe, "__await__"):
                await maybe

    async def close(self) -> None:
        for _, sink in reversed(self._sinks):
            if hasattr(sink, "close"):
                maybe = sink.close()  # type: ignore[attr-defined]
                if hasattr(maybe, "__await__"):
                    await maybe
        self._sinks.clear()
        if self._dlq and hasattr(self._dlq, "close"):
            maybe = self._dlq.close()  # type: ignore[attr-defined]
            if hasattr(maybe, "__await__"):
                await maybe
        self._dlq = None


# ---- helpers ----------------------------------------------------------------


def _create_sink(registry: PluginRegistry, name: str, ctx: AdapterContext, args: Mapping[str, Any]) -> SinkAdapter:
    if hasattr(registry, "create"):
        try:
            sink = registry.create(kind="sink", name=name, ctx=ctx, **args)
            return sink  # type: ignore[return-value]
        except Exception:
            pass
    if hasattr(registry, "get_sink"):
        factory = registry.get_sink(name)  # type: ignore[attr-defined]
        return factory(ctx, **args)  # type: ignore[return-value]
    raise LookupError(f"sink '{name}' is not registered")


def _delivery_params(spec: Mapping[str, Any], *, dlq: bool = False) -> Mapping[str, Any]:
    # Use the same policy for DLQ unless user overrides. Simple and predictable.
    return {
        "retries": int(spec.get("retries", 5)),
        "backoff_ms": int(spec.get("backoff_ms", 200)),
        "backoff_max_ms": int(spec.get("backoff_max_ms", 5000)),
        "jitter": float(spec.get("jitter", 0.25)),
    }


async def _send_with_retry(
    sink: SinkAdapter,
    x: Item | Batch,
    *,
    idempotency_key: Optional[str],
    retries: int,
    backoff_ms: int,
    backoff_max_ms: int,
    jitter: float,
) -> None:
    attempt = 0
    while True:
        try:
            fn = getattr(sink, "send", None)
            if fn is None:
                raise TypeError(f"sink {sink!r} has no 'send' method")
            res = fn(x, idempotency_key=idempotency_key)
            if hasattr(res, "__await__"):
                await res
            return
        except Exception:
            attempt += 1
            if attempt > retries:
                raise
            delay = min(backoff_max_ms, backoff_ms * (2 ** (attempt - 1)))
            if jitter:
                # Add jitter to avoid sync retry storms
                jitter_win = int(delay * jitter)
                delay = max(0, delay + random.randint(-jitter_win, jitter_win))
            await asyncio.sleep(delay / 1000.0)
