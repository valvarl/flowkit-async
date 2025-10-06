# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""A tiny echo handler for smoke tests.

Usage (Coordinator sends CmdTaskStart with input_inline like):

    {
      "handler": "handlers.echo",
      "args": {
        "payload": {"hello": "world"},
        "metrics": {"foo": 1}
      }
    }

The handler copies `args.payload` into artifacts_ref, returns metrics if given.
"""

from dataclasses import dataclass
from typing import Any

from . import Handler, HandlerResult, TaskContext, register


@dataclass
class EchoHandler(Handler):  # type: ignore[misc]
    async def open(self, ctx: TaskContext, **kwargs: Any) -> None:  # pragma: no cover - trivially async
        # nothing to init for echo
        return None

    async def run(self, ctx: TaskContext, **kwargs: Any) -> HandlerResult:
        # Optional cooperative cancellation check (fast path)
        if getattr(ctx, "cancel_flag", None) is not None and ctx.cancel_flag.is_set():  # type: ignore[attr-defined]
            # emulate a cancelled run by raising (worker will convert to CANCELLED)
            raise asyncio.CancelledError()  # type: ignore[name-defined]

        payload = kwargs.get("payload")
        metrics = kwargs.get("metrics") or {}
        return HandlerResult(metrics=dict(metrics), artifacts_ref=payload)

    async def close(self) -> None:  # pragma: no cover - trivially async
        return None


# Register under both "handlers.echo" and "echo" for convenience
register("handlers.echo", EchoHandler)
register("echo", EchoHandler)
