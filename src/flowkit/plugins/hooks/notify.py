# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from flowkit.api.hooks import HookAction, HookContext, HookEvent
from flowkit.core.logging import get_logger

from .http_post import _post as _http_post


class _NotifyBase(HookAction):  # type: ignore[misc]
    """Common logic for notify-like actions (wraps http.post)."""

    name = "notify"

    async def run(self, ctx: HookContext, event: HookEvent, args: Mapping[str, Any]) -> None:
        logger = getattr(ctx, "logger", None) or get_logger("hooks.notify")

        # Resolve URL
        url = str(args.get("url") or "")
        if not url and args.get("external"):
            ext = str(args["external"])
            maybe = (ctx.externals or {}).get(ext, {})
            url = str((maybe or {}).get("url") or "")
        if not url:
            logger.debug("notify.skip", reason="no_url", action=self.name, on=event.on)
            return

        text = str(args.get("text") or f"{event.on}: node={event.node_id}")
        payload: dict[str, Any] = self._build_payload(text=text, event=event, args=args)

        headers = {"content-type": "application/json"}
        timeout_ms = int(args.get("timeout_ms") or 5000)

        res = await _http_post(
            url=url, body=self._encode(payload), headers=headers, timeout_sec=max(0.1, timeout_ms / 1000.0)
        )
        if res is None:
            logger.debug("notify.failed", url=url, action=self.name, on=event.on, node_id=event.node_id)
        else:
            status, _ = res
            logger.debug("notify.ok", status=status, url=url, action=self.name, on=event.on, node_id=event.node_id)

    # ---- overrides

    def _build_payload(self, *, text: str, event: HookEvent, args: Mapping[str, Any]) -> dict[str, Any]:
        """Override to adapt to target service (Slack/Telegram/etc.)."""
        return {"text": text}

    def _encode(self, obj: dict[str, Any]) -> bytes:
        import json

        return json.dumps(obj).encode("utf-8")


class NotifyAction(_NotifyBase):
    """Generic notify: POST JSON {'text': ...} to the given URL/external.url."""

    name = "notify"


class NotifySlackAction(_NotifyBase):
    """Slack incoming webhook compatible: {'text': ... , 'mrkdwn': true}."""

    name = "notify.slack"

    def _build_payload(self, *, text: str, event: HookEvent, args: Mapping[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {"text": text, "mrkdwn": True}
        if "blocks" in args and isinstance(args["blocks"], list):
            payload["blocks"] = args["blocks"]
        return payload


class NotifyTelegramAction(_NotifyBase):
    """
    Telegram bot API webhook or custom relay.

    Two modes:
      1) If 'url' provided, behaves like generic notify (POST {'text': ...}).
      2) If 'base_url','bot_token','chat_id' provided: POST to
         '{base_url}/bot{bot_token}/sendMessage' with {'chat_id','text','parse_mode'?}.
    """

    name = "notify.telegram"

    async def run(self, ctx: HookContext, event: HookEvent, args: Mapping[str, Any]) -> None:
        # special mode — bot API style
        if args.get("bot_token") and args.get("chat_id"):
            base = str(args.get("base_url") or "https://api.telegram.org")
            url = f"{base}/bot{args['bot_token']}/sendMessage"
            payload = {
                "chat_id": args["chat_id"],
                "text": str(args.get("text") or f"{event.on}: node={event.node_id}"),
            }
            if args.get("parse_mode"):
                payload["parse_mode"] = str(args["parse_mode"])
            res = await _http_post(
                url=url,
                body=self._encode(payload),
                headers={"content-type": "application/json"},
                timeout_sec=max(0.1, int(args.get("timeout_ms") or 5000) / 1000.0),
            )
            logger = getattr(ctx, "logger", None) or get_logger("hooks.notify")
            if res is None:
                logger.debug("notify.failed", url=url, action=self.name, on=event.on, node_id=event.node_id)
            else:
                status, _ = res
                logger.debug("notify.ok", status=status, url=url, action=self.name, on=event.on, node_id=event.node_id)
            return
        # fallback to generic behavior (url/external)
        await super().run(ctx, event, args)
