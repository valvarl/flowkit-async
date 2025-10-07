# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
HTTP polling/pagination source (async, aiohttp-based).

Key features:
- Supports classic page-based pagination (?page=N) OR cursor-based/URL-based
  pagination (response contains "next" pointer).
- Extracts items from JSON payload using a dotted-path selector.
- Emits Items (ContentKind.RECORD) by default; can emit raw bytes for non-JSON
  payloads (ContentKind.BYTES).
- Emits a Checkpoint with the last seen cursor or page to allow resume.
- Respects cancellation via ctx.cancel_flag and optional deadline_ms.

Minimal args:
    {"url": "https://api.example.com/resources"}

Useful args:
    {
      "method": "GET",
      "headers": {"Authorization": "Bearer ..."},
      "params": {"status": "open"},
      "items_path": "data.items",          # dotted path within JSON payload
      "next_path": "data.paging.next",     # dotted path for next cursor/URL
      "page_param": "page",                # when classic pagination is used
      "page_start": 1,
      "poll_ms": 5000,                     # delay between polls when no "next"
      "json": true                         # treat response as JSON (default: true)
    }

Idempotency key:
- If `id_key` is provided (dotted path within each item), the source emits a
  per-item idempotency key (stringified value).
"""

import asyncio
from collections.abc import AsyncIterator, Iterable, Mapping
from typing import Any

try:  # soft dependency
    import aiohttp
except Exception as _e:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]

from ....api.adapters import AdapterContext, SourceAdapter
from ....api.streams import Checkpoint, ContentKind, FrameDescriptor, Item


def _get_in(obj: Any, dotted: str, default: Any = None) -> Any:
    """Traverse dict-like objects by a dotted path; returns default if not found."""
    cur = obj
    for part in (dotted or "").split("."):
        if not part:
            continue
        if isinstance(cur, Mapping) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def _items_from_payload(payload: Any, items_path: str | None) -> Iterable[Any]:
    if items_path:
        seq = _get_in(payload, items_path, default=[])
        return seq if isinstance(seq, (list, tuple)) else []
    # no items_path specified: if payload is a list — use it, else wrap as single item
    if isinstance(payload, list):
        return payload
    return [payload]


class HttpPollingSource(SourceAdapter):
    """
    A simple yet robust HTTP source with pagination.

    This source is intentionally lightweight. For advanced APIs consider
    specialized sources.
    """

    name = "pull.http.paginate"
    capabilities = None

    def __init__(self) -> None:
        self._ctx: AdapterContext | None = None
        self._args: Mapping[str, Any] = {}
        self._session: Any = None

    async def open(self, ctx: AdapterContext, **spec: Any) -> None:
        if aiohttp is None:
            raise RuntimeError("aiohttp is required for HttpPollingSource; please install it")
        self._ctx = ctx
        self._args = spec or {}
        # Lazily create a session; closed on adapter.close()
        self._session = aiohttp.ClientSession()

    def stream(self, ctx: AdapterContext, *, checkpoint: Checkpoint | None = None) -> AsyncIterator[Any]:
        return self._stream_impl(ctx, checkpoint=checkpoint)

    async def close(self) -> None:
        if self._session:
            try:
                await self._session.close()
            except Exception:  # pragma: no cover - best effort
                pass
        self._session = None
        self._ctx = None

    # ---- internals -----------------------------------------------------------

    async def _stream_impl(self, ctx: AdapterContext, *, checkpoint: Checkpoint | None) -> AsyncIterator[Any]:
        url = str(self._args.get("url") or self._args.get("base_url") or "")
        if not url:
            raise ValueError("HttpPollingSource requires 'url' or 'base_url' argument")

        method = (self._args.get("method") or "GET").upper()
        headers = dict(self._args.get("headers") or {})
        params = dict(self._args.get("params") or {})

        items_path = self._args.get("items_path") or "items"
        next_path = self._args.get("next_path") or "next"
        page_param = self._args.get("page_param") or "page"
        page_start = int(self._args.get("page_start", 1))

        treat_json = bool(self._args.get("json", True))
        poll_ms = int(self._args.get("poll_ms", 2000))
        id_key = self._args.get("id_key")

        # restore cursor/page from checkpoint
        cursor = None
        page = None
        if checkpoint and checkpoint.token:
            cursor = checkpoint.token.get("cursor") or checkpoint.token.get("next_url")
            page = checkpoint.token.get("page")

        # fallback to page-based if there's no cursor and no next field
        if page is None and cursor is None:
            page = page_start if page_param else None

        cancel_ev = getattr(ctx, "cancel_flag", None)

        # pagination loop
        while True:
            if cancel_ev is not None and cancel_ev.is_set():  # type: ignore[attr-defined]
                break

            req_url = cursor or url
            req_params = dict(params)
            if page is not None and page_param:
                req_params[page_param] = page

            async with self._session.request(method, req_url, headers=headers, params=req_params) as resp:
                resp.raise_for_status()
                if treat_json:
                    payload = await resp.json()
                else:
                    payload = await resp.read()

            # Non-JSON: emit one Item (bytes) and stop or poll
            if not treat_json:
                item = Item(
                    payload=payload,
                    frame=FrameDescriptor(kind=ContentKind.BYTES, type="bytes"),
                    meta={"http.url": req_url, "http.status": getattr(resp, "status", None)},
                )
                idem = None
                yield (item, Checkpoint(token={"next_url": cursor or url, "page": page}), idem)
                # In BYTES mode we do not try to paginate, just single-shot
                break

            # JSON: extract items and "next" pointer
            items = list(_items_from_payload(payload, items_path))
            next_val = _get_in(payload, next_path, default=None)

            # Emit items with per-item idempotency key (optional)
            for it in items:
                item = Item(
                    payload=it,
                    frame=FrameDescriptor(kind=ContentKind.RECORD, type="json"),
                    meta={"http.url": req_url},
                )
                idem: str | None = None
                if id_key:
                    val = _get_in(it, id_key)
                    if val is not None:
                        idem = str(val)
                # Persist cursor/page in checkpoint
                yield (item, Checkpoint(token={"next_url": next_val, "page": page}), idem)

            # Advance pagination
            if next_val:
                # next is either a full URL or a cursor token
                if isinstance(next_val, str) and next_val.lower().startswith(("http://", "https://")):
                    cursor = next_val
                else:
                    # if it's a cursor value, try to set as a request param (commonly "cursor")
                    cursor = None
                    params["cursor"] = next_val
                # page-based counters are not used in this mode
                page = None
            elif page is not None and items:
                page += 1
            else:
                # no next and not paginated — optional long polling
                if poll_ms <= 0:
                    break
                try:
                    await asyncio.sleep(poll_ms / 1000.0)
                except asyncio.CancelledError:  # pragma: no cover
                    break
