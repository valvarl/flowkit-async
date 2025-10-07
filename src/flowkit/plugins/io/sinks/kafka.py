# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Kafka sink based on aiokafka producer.

Features:
  - JSON serialization for RECORD payloads (opt-out via json=False).
  - Per-item sending for batches of items; framed blobs sent as a single message.
  - Optional key template: supports "{meta.id}", "{node}", "{task}", "{idem}" placeholders.
  - Headers: pass `headers` as dict[str, bytes|str].

Expected args (typical):
  - bootstrap: str
  - topic: str
  - json: bool = True
  - key_template: Optional[str]
  - headers: Optional[dict[str, str|bytes]]
"""

import json
from collections.abc import Mapping, Sequence
from typing import Any

from aiokafka import AIOKafkaProducer

from ....api.adapters import AdapterContext, Capabilities, SinkAdapter
from ....api.streams import Batch, ContentKind, Item


def _encode_headers(h: Mapping[str, Any] | None) -> Sequence[tuple[str, bytes]] | None:
    if not h:
        return None
    out: list[tuple[str, bytes]] = []
    for k, v in h.items():
        if isinstance(v, bytes):
            out.append((k, v))
        else:
            out.append((k, str(v).encode("utf-8", errors="ignore")))
    return out


class KafkaProducerSink(SinkAdapter):
    name = "emit.kafka"
    capabilities = Capabilities(
        accepts=frozenset({ContentKind.RECORD, ContentKind.BYTES}),
        emits=frozenset(),
    )

    def __init__(self, ctx: AdapterContext, **kwargs: Any) -> None:
        self._ctx = ctx
        self._args = kwargs
        self._producer: AIOKafkaProducer | None = None

    async def open(self, ctx: AdapterContext, **spec: Any) -> None:
        bootstrap = spec.get("bootstrap") or spec.get("bootstrap_servers")
        topic = spec.get("topic")
        if not bootstrap or not topic:
            raise ValueError("emit.kafka requires 'bootstrap' and 'topic'")

        self._args = {**self._args, **spec}
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            enable_idempotence=True,
        )
        await self._producer.start()

    async def send(self, x: Item | Batch, *, idempotency_key: str | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaProducerSink is not opened")

        topic: str = self._args["topic"]
        as_json: bool = bool(self._args.get("json", True))
        headers = _encode_headers(self._args.get("headers"))
        key_tpl: str | None = self._args.get("key_template")

        if isinstance(x, Item):
            key = self._render_key(key_tpl, x.meta, idempotency_key)
            payload = self._encode_item_payload(x, as_json=as_json)
            await self._producer.send_and_wait(topic, payload, key=key, headers=headers)
            return

        # Batch
        if x.items is not None:
            # Per-item publish
            for it in x.items:
                if not isinstance(it, Item):
                    it = Item(payload=getattr(it, "payload", it), meta={})
                key = self._render_key(key_tpl, it.meta, idempotency_key)
                payload = self._encode_item_payload(it, as_json=as_json)
                await self._producer.send_and_wait(topic, payload, key=key, headers=headers)
            return

        # Framed blob (single message)
        if x.blob is not None:
            key = self._render_key(key_tpl, (x.meta or {}), idempotency_key)
            await self._producer.send_and_wait(topic, bytes(x.blob), key=key, headers=headers)
            return

        raise TypeError("unsupported batch shape for emit.kafka")

    async def flush(self) -> None:
        if self._producer:
            await self._producer.flush()

    async def close(self) -> None:
        if self._producer:
            try:
                await self._producer.stop()
            finally:
                self._producer = None

    # ---- helpers -------------------------------------------------------------

    def _render_key(self, tpl: str | None, meta: Mapping[str, Any], idem: str | None) -> bytes | None:
        if not tpl and not idem:
            return None
        if not tpl and idem:
            return idem.encode("utf-8")
        try:
            text = tpl.format(
                node=self._ctx.node_id,
                task=self._ctx.task_id,
                idem=idem or "",
                **{f"meta.{k}": v for k, v in (meta or {}).items()},
            )
            return text.encode("utf-8")
        except Exception:
            return (idem or "").encode("utf-8")

    def _encode_item_payload(self, it: Item, *, as_json: bool) -> bytes:
        if isinstance(it.payload, (bytes, bytearray, memoryview)):
            return bytes(it.payload)
        if as_json:
            return json.dumps(it.payload, ensure_ascii=False).encode("utf-8")
        # Best-effort: stringify
        return str(it.payload).encode("utf-8", errors="ignore")
