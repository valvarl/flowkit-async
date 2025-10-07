# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from flowkit.api.hooks import HookAction, HookContext, HookEvent
from flowkit.core.logging import get_logger


async def _post(
    *,
    url: str,
    body: bytes,
    headers: Mapping[str, str] | None = None,
    timeout_sec: float = 5.0,
) -> tuple[int, bytes] | None:
    """Run blocking urllib in a thread, best-effort."""

    def _sync() -> tuple[int, bytes]:
        req = Request(url=url, data=body, headers=dict(headers or {}), method="POST")
        with urlopen(req, timeout=timeout_sec) as resp:  # nosec - used in controlled envs
            status = int(getattr(resp, "status", 200))
            return status, resp.read(1024)  # limit small

    try:
        return await asyncio.to_thread(_sync)
    except (HTTPError, URLError, TimeoutError):
        return None


class HttpPostAction(HookAction):  # type: ignore[misc]
    """
    Perform a best-effort HTTP POST.

    Args:
      - url: full URL (str)
      - external: name referencing ctx.externals[external]['url'] (if 'url' omitted)
      - json: any JSON-serializable body (preferred)
      - data: str|bytes body (used when 'json' not provided)
      - headers: dict[str,str] (optional)
      - timeout_ms: int (default 5000)

    Returns: none (best-effort). Errors are swallowed; a debug log is emitted.
    """

    name = "http.post"

    async def run(self, ctx: HookContext, event: HookEvent, args: Mapping[str, Any]) -> None:
        logger = getattr(ctx, "logger", None) or get_logger("hooks.http")

        url = str(args.get("url") or "")
        if not url:
            ext = args.get("external")
            if ext and isinstance(ctx.externals, Mapping):
                maybe = ctx.externals.get(str(ext), {})
                url = str((maybe or {}).get("url") or "")
        if not url:
            logger.debug("http.post.skip", reason="no_url", node_id=event.node_id, on=event.on)
            return

        # Validate URL quickly
        if not urlparse(url).scheme:
            logger.debug("http.post.skip", reason="bad_url", url=url)
            return

        headers: dict[str, str] = dict(args.get("headers") or {})
        timeout_ms = int(args.get("timeout_ms") or 5000)

        # Prefer JSON payload
        if "json" in args and args.get("json") is not None:
            body = json.dumps(args.get("json")).encode("utf-8")
            headers.setdefault("content-type", "application/json")
        else:
            data = args.get("data")
            if isinstance(data, bytes):
                body = data
            elif isinstance(data, str):
                body = data.encode("utf-8")
            else:
                body = b"{}"
                headers.setdefault("content-type", "application/json")

        res = await _post(url=url, body=body, headers=headers, timeout_sec=max(0.1, timeout_ms / 1000.0))
        if res is None:
            logger.debug("http.post.failed", url=url, on=event.on, node_id=event.node_id)
        else:
            status, _ = res
            logger.debug("http.post.ok", status=status, url=url, on=event.on, node_id=event.node_id)
