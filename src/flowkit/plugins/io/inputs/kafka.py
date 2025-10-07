# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Kafka subscribe/assign source (aiokafka).

Two operating modes:
1) Manual assignment (recommended for FlowKit checkpoints):
    args = {
      "bootstrap": "localhost:9092",
      "assign": {"topicA": [0, 1]},       # explicit partitions
      "json": true                         # decode as JSON (records)
    }
   In this mode we restore per-partition offsets from the `checkpoint` passed
   by the engine and seek before starting the stream.

2) Group subscribe (works but offset commits are *not* performed by this source):
    args = {
      "bootstrap": "localhost:9092",
      "group": "flowkit-consumer",
      "topics": ["topicA", "topicB"],
      "json": false
    }
   We will still *seek* to offsets from the incoming checkpoint on first
   assignment. However, since the engine persists offsets in its own store,
   Kafka commits are intentionally disabled (enable_auto_commit=False).

The source yields:
  (Item|Batch, Checkpoint, idempotency_key)

Checkpoint token is a dict mapping (topic, partition) → offset:
  {"offsets": {"topicA:0": 123, "topicA:1": 456}}

Idempotency key is "topic:partition:offset".
"""

import asyncio
import json
from collections.abc import Mapping
from typing import Any

try:  # soft dependency
    from aiokafka import AIOKafkaConsumer, TopicPartition
except Exception:  # pragma: no cover
    AIOKafkaConsumer = None  # type: ignore[assignment]
    TopicPartition = None  # type: ignore[assignment]

from ....api.adapters import AdapterContext, SourceAdapter
from ....api.streams import Checkpoint, ContentKind, FrameDescriptor, Item


def _make_key(topic: str, partition: int) -> str:
    return f"{topic}:{partition}"


def _parse_checkpoint(ckpt: Checkpoint | None) -> dict[str, int]:
    if not ckpt or not ckpt.token:
        return {}
    offsets = ckpt.token.get("offsets") or {}
    # ensure str->int mapping
    out: dict[str, int] = {}
    for k, v in offsets.items():
        try:
            out[str(k)] = int(v)
        except Exception:
            continue
    return out


class KafkaSubscribeSource(SourceAdapter):
    """
    Minimal Kafka source based on aiokafka.

    Args:
        bootstrap: bootstrap servers string ("host1:9092,host2:9092")
        topics: list of topics (subscribe mode)
        group: consumer group id (subscribe mode)
        assign: {"topic": [0,1,...]} (manual assignment; recommended)
        json: bool (decode value as JSON into RECORD items; else BYTES)
        poll_ms: int (not used; consumer is async streaming)

    Notes:
        - enable_auto_commit is disabled to preserve FlowKit-managed checkpoints.
        - For subscribe mode the adapter will *seek* to offsets from checkpoint
          once partitions are assigned.
    """

    name = "pull.kafka.subscribe"
    capabilities = None

    def __init__(self) -> None:
        self._ctx: AdapterContext | None = None
        self._args: Mapping[str, Any] = {}
        self._consumer: AIOKafkaConsumer | None = None
        self._offsets: dict[str, int] = {}  # "topic:partition" -> offset
        self._json: bool = True

    async def open(self, ctx: AdapterContext, **spec: Any) -> None:
        if AIOKafkaConsumer is None:
            raise RuntimeError("aiokafka is required for KafkaSubscribeSource; please install it")

        self._ctx = ctx
        self._args = spec or {}
        self._json = bool(self._args.get("json", True))

        bootstrap = self._args.get("bootstrap") or (ctx.externals or {}).get("kafka", {}).get("bootstrap")
        if not bootstrap:
            raise ValueError("KafkaSubscribeSource requires 'bootstrap' (or externals.kafka.bootstrap)")

        group = self._args.get("group")
        topics = list(self._args.get("topics") or [])
        assign = dict(self._args.get("assign") or {})

        # Configure common consumer with manual commits
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=str(bootstrap),
            group_id=str(group) if group else None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            # We parse .value on our own to support both JSON/bytes modes
            value_deserializer=None,
        )

        await self._consumer.start()

        # Manual assignment if requested (group must be None)
        if assign:
            if group:
                raise ValueError("KafkaSubscribeSource: 'assign' and 'group' are mutually exclusive")
            tps = [TopicPartition(t, int(p)) for t, parts in assign.items() for p in parts]
            await self._consumer.assign(tps)

    def stream(self, ctx: AdapterContext, *, checkpoint: Checkpoint | None = None):
        # Prepare per-partition offsets map
        self._offsets = _parse_checkpoint(checkpoint)
        return self._stream_impl(ctx)

    async def close(self) -> None:
        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception:  # pragma: no cover
                pass
        self._consumer = None
        self._ctx = None

    # ---- internals -----------------------------------------------------------

    async def _seek_to_checkpoint(self) -> None:
        """
        Seek to offsets supplied in self._offsets after assignment is known.
        Safe to call multiple times.
        """
        if not self._consumer or not TopicPartition:
            return
        assignment = self._consumer.assignment()  # synchronous
        if not assignment:
            # Wait briefly until the assignment is ready (subscribe mode)
            for _ in range(50):
                await asyncio.sleep(0.1)
                assignment = self._consumer.assignment()
                if assignment:
                    break
        for tp in assignment:
            key = _make_key(tp.topic, tp.partition)
            if key in self._offsets:
                try:
                    self._consumer.seek(tp, self._offsets[key] + 1)  # synchronous
                except Exception:
                    # best-effort: ignore seek errors
                    continue

    async def _stream_impl(self, ctx: AdapterContext):
        # Attempt to seek once on start if we have a checkpoint
        if self._offsets:
            await self._seek_to_checkpoint()

        cancel_ev = getattr(ctx, "cancel_flag", None)

        async for rec in self._consumer:  # type: ignore[union-attr]
            if cancel_ev is not None and cancel_ev.is_set():  # type: ignore[attr-defined]
                break

            try:
                raw = rec.value
                if self._json:
                    payload = (
                        json.loads(raw.decode("utf-8")) if isinstance(raw, (bytes, bytearray)) else json.loads(raw)
                    )
                    frame = FrameDescriptor(kind=ContentKind.RECORD, type="json")
                else:
                    payload = raw
                    frame = FrameDescriptor(kind=ContentKind.BYTES, type="bytes")

                item = Item(
                    payload=payload,
                    frame=frame,
                    meta={
                        "kafka.topic": rec.topic,
                        "kafka.partition": rec.partition,
                        "kafka.offset": rec.offset,
                        "kafka.timestamp": getattr(rec, "timestamp", None),
                    },
                )

                key = _make_key(rec.topic, rec.partition)
                self._offsets[key] = int(rec.offset)  # update the latest offset per partition
                ckpt = Checkpoint(token={"offsets": dict(self._offsets)})

                idem_key = f"{rec.topic}:{rec.partition}:{rec.offset}"
                yield (item, ckpt, idem_key)
            except Exception:
                # Skip bad messages but keep streaming
                continue
