from __future__ import annotations

import asyncio
import copy
import json
import uuid
from collections import OrderedDict
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ..core.config import WorkerConfig
from ..core.time import Clock, SystemClock
from ..core.utils import dumps, loads, stable_hash
from ..protocol.messages import (
    CmdTaskCancel,
    CmdTaskStart,
    CommandKind,
    Envelope,
    EventKind,
    MsgType,
    QueryKind,
    ReplyKind,
    Role,
    SigCancel,
)
from .artifacts import ArtifactsWriter
from .context import RunContext
from .handlers.base import Batch, BatchResult, RoleHandler
from .handlers.echo import EchoHandler
from .input.pull_adapters import build_input_adapters
from .state import ActiveRun, LocalState


def _json_log(clock: Clock, **kv: Any) -> None:
    ts = clock.now_dt().isoformat()
    print(json.dumps({"ts": ts, **kv}, ensure_ascii=False), flush=True)


class Worker:
    """
    Stream-aware worker with cooperative cancellation and resilient batching.
    - Kafka I/O (producers/consumers per role)
    - Discovery (TASK_DISCOVER → TASK_SNAPSHOT)
    - Control-plane CANCEL via signals topic
    - Local JSON state for resume hints
    """

    def __init__(
        self,
        *,
        db,
        cfg: WorkerConfig | None = None,
        clock: Clock | None = None,
        roles: list[str] | None = None,
        handlers: dict[str, RoleHandler] | None = None,
    ) -> None:
        self.db = db
        self.cfg = copy.deepcopy(cfg) if cfg is not None else WorkerConfig.load()
        if roles:
            self.cfg.roles = list(roles)
        self.clock: Clock = clock or SystemClock()

        # identity
        self.worker_id = self.cfg.worker_id or f"w-{uuid.uuid4().hex[:8]}"
        self.worker_version = self.cfg.worker_version

        # adapters registry
        self.input_adapters = build_input_adapters(db=db, clock=self.clock, cfg=self.cfg)

        # handlers registry (user injects custom handlers; Echo kept as example)
        self.handlers: dict[str, RoleHandler] = handlers or {}
        if "echo" in (self.cfg.roles or []):
            self.handlers.setdefault("echo", EchoHandler())

        # Kafka
        self._producer: AIOKafkaProducer | None = None
        self._cmd_consumers: dict[str, AIOKafkaConsumer] = {}
        self._query_consumer: AIOKafkaConsumer | None = None
        self._signals_consumer: AIOKafkaConsumer | None = None

        # run-state
        self._busy = False
        self._busy_lock = asyncio.Lock()
        self._cancel_flag = asyncio.Event()
        self._cancel_meta: dict[str, Any] = {"reason": None, "deadline_ts_ms": None}
        self._stopping = False

        self.state = LocalState(self.cfg, self.clock)
        self.active: ActiveRun | None = self.state.read_active()

        # dedup of command envelopes
        self._dedup: OrderedDict[str, int] = OrderedDict()
        self._dedup_lock = asyncio.Lock()

        self._main_tasks: set[asyncio.Task] = set()

    # ---------- lifecycle ----------
    async def start(self) -> None:
        await self._ensure_indexes()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.cfg.kafka_bootstrap, value_serializer=dumps, enable_idempotence=True
        )
        await self._producer.start()

        self.active = self.state.read_active()
        if self.active and self.active.step_type not in self.cfg.roles:
            self.active = None
            await self.state.write_active(None)

        await self._send_announce(
            EventKind.WORKER_ONLINE,
            extra={
                "worker_id": self.worker_id,
                "type": ",".join(self.cfg.roles),
                "capabilities": {"roles": self.cfg.roles},
                "version": self.worker_version,
                "capacity": {"tasks": 1},
                "resume": self.active.__dict__ if self.active else None,
            },
        )

        # command consumers per role
        for role in self.cfg.roles:
            topic = self.cfg.topic_cmd(role)
            c = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.cfg.kafka_bootstrap,
                value_deserializer=loads,
                enable_auto_commit=False,
                auto_offset_reset="latest",
                group_id=f"workers.{role}.v1",
            )
            await c.start()
            self._cmd_consumers[role] = c
            self._spawn(self._cmd_loop(role, c))

        # query consumer (discovery)
        self._query_consumer = AIOKafkaConsumer(
            self.cfg.topic_query,
            bootstrap_servers=self.cfg.kafka_bootstrap,
            value_deserializer=loads,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            group_id="workers.query.v1",
        )
        await self._query_consumer.start()
        self._spawn(self._query_loop(self._query_consumer))

        # signals consumer (control plane; unique group per worker)
        self._signals_consumer = AIOKafkaConsumer(
            self.cfg.topic_signals,
            bootstrap_servers=self.cfg.kafka_bootstrap,
            value_deserializer=loads,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            group_id=f"workers.signals.{self.worker_id}",
        )
        await self._signals_consumer.start()
        self._spawn(self._signals_loop(self._signals_consumer))

        # periodic announce
        self._spawn(self._periodic_announce())

        if self.active:
            _json_log(self.clock, event="recovery_present", task_id=self.active.task_id, node_id=self.active.node_id)

    async def stop(self) -> None:
        self._stopping = True
        for t in list(self._main_tasks):
            t.cancel()
        self._main_tasks.clear()

        if self._query_consumer:
            try:
                await self._query_consumer.stop()
            except Exception:
                pass
        if self._signals_consumer:
            try:
                await self._signals_consumer.stop()
            except Exception:
                pass
        for c in self._cmd_consumers.values():
            try:
                await c.stop()
            except Exception:
                pass
        self._cmd_consumers.clear()

        if self._producer:
            try:
                await self._send_announce(EventKind.WORKER_OFFLINE, extra={"worker_id": self.worker_id})
                await self._producer.stop()
            except Exception:
                pass
        self._producer = None

    def _spawn(self, coro):
        t = asyncio.create_task(coro)
        self._main_tasks.add(t)
        t.add_done_callback(self._main_tasks.discard)

    # ---------- Kafka send helpers ----------
    async def _send_status(self, role: str, env: Envelope) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaBus producer is not initialized")
        topic = self.cfg.topic_status(role)
        key = f"{env.task_id}:{env.node_id}".encode()
        await self._producer.send_and_wait(topic, env.model_dump(mode="json"), key=key)

    async def _send_announce(self, kind: EventKind, extra: dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaBus producer is not initialized")
        payload = {"kind": kind, **extra}
        now_ms = self.clock.now_ms()
        env = Envelope(
            msg_type=MsgType.event,
            role=Role.worker,
            dedup_id=stable_hash({"announce": kind, "worker": self.worker_id, "ts_ms": now_ms}),
            task_id="*",
            node_id="*",
            step_type="*",
            attempt_epoch=0,
            ts_ms=now_ms,
            payload=payload,
        )
        await self._producer.send_and_wait(self.cfg.topic_worker_announce, env.model_dump(mode="json"))

    async def _send_reply(self, env: Envelope) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaBus producer is not initialized")
        key = (env.task_id or "*").encode("utf-8")
        await self._producer.send_and_wait(self.cfg.topic_reply, env.model_dump(mode="json"), key=key)

    # ---------- Dedup for incoming commands ----------
    async def _seen_or_add(self, dedup_id: str) -> bool:
        async with self._dedup_lock:
            now = self.clock.now_ms()
            for k in list(self._dedup.keys()):
                if now - self._dedup[k] > self.cfg.dedup_ttl_ms:
                    self._dedup.pop(k, None)
            if dedup_id in self._dedup:
                return True
            self._dedup[dedup_id] = now
            while len(self._dedup) > self.cfg.dedup_cache_size:
                self._dedup.popitem(last=False)
            return False

    async def _pause_all_cmd_consumers(self) -> None:
        for c in self._cmd_consumers.values():
            parts = c.assignment()
            if parts:
                c.pause(*parts)

    async def _resume_all_cmd_consumers(self) -> None:
        for c in self._cmd_consumers.values():
            parts = c.assignment()
            if parts:
                c.resume(*parts)

    # ---------- Command loop ----------
    async def _cmd_loop(self, role: str, consumer: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await consumer.getone()
                env = Envelope.model_validate(msg.value)
                if await self._seen_or_add(env.dedup_id):
                    await consumer.commit()
                    continue
                if env.msg_type != MsgType.cmd or env.role != Role.coordinator:
                    await consumer.commit()
                    continue
                if env.step_type != role:
                    await consumer.commit()
                    continue

                _k = env.payload.get("cmd")
                try:
                    kind = _k if isinstance(_k, CommandKind) else CommandKind(_k)
                except Exception:
                    kind = None

                if kind == CommandKind.TASK_START:
                    await self._handle_task_start(role, env, consumer)
                elif kind == CommandKind.TASK_CANCEL:
                    await self._handle_task_cancel(role, env, consumer)
                else:
                    await consumer.commit()
        except asyncio.CancelledError:
            return

    async def _handle_task_start(self, role: str, env: Envelope, consumer: AIOKafkaConsumer) -> None:
        cmd = CmdTaskStart.model_validate(env.payload)
        async with self._busy_lock:
            if self._busy:
                await consumer.commit()
                return

            if self.active and not (self.active.task_id == env.task_id and self.active.node_id == env.node_id):
                self.active = None
                await self.state.write_active(None)

            lease_id = str(uuid.uuid4())
            lease_deadline_ms = self.clock.now_ms() + self.cfg.lease_ttl_ms

            self._busy = True
            self._cancel_flag.clear()
            self._cancel_meta["reason"] = None
            self._cancel_meta["deadline_ts_ms"] = None

            self.active = ActiveRun(
                task_id=env.task_id,
                node_id=env.node_id,
                step_type=role,
                attempt_epoch=env.attempt_epoch,
                lease_id=lease_id,
                cancel_token=cmd.cancel_token,
                started_at_ms=self.clock.now_ms(),
                state="running",
                checkpoint={},
            )
            await self.state.write_active(self.active)

            acc_env = Envelope(
                msg_type=MsgType.event,
                role=Role.worker,
                dedup_id=stable_hash({"acc": env.task_id, "n": env.node_id, "e": env.attempt_epoch, "lease": lease_id}),
                task_id=env.task_id,
                node_id=env.node_id,
                step_type=role,
                attempt_epoch=env.attempt_epoch,
                ts_ms=self.clock.now_ms(),
                payload={
                    "kind": EventKind.TASK_ACCEPTED,
                    "worker_id": self.worker_id,
                    "lease_id": lease_id,
                    "lease_deadline_ts_ms": lease_deadline_ms,
                },
            )
            await self._send_status(role, acc_env)

            await self._pause_all_cmd_consumers()
            self._spawn(self._heartbeat_loop(role))
            self._spawn(self._run_handler(role, env, cmd))

            await consumer.commit()

    async def _handle_task_cancel(self, role: str, env: Envelope, consumer: AIOKafkaConsumer) -> None:
        _ = CmdTaskCancel.model_validate(env.payload)
        if (
            self.active
            and self.active.task_id == env.task_id
            and self.active.node_id == env.node_id
            and self.active.attempt_epoch == env.attempt_epoch
        ):
            self._cancel_meta["reason"] = self._cancel_meta.get("reason") or "coordinator_cmd"
            self._cancel_flag.set()
            self.active.state = "cancelling"
            await self.state.write_active(self.active)
        await consumer.commit()

    # ---------- Handler execution ----------
    async def _run_handler(self, role: str, start_env: Envelope, cmd: CmdTaskStart) -> None:
        assert self.active is not None
        handler = self.handlers.get(role)
        if not handler:
            await self._emit_task_failed(role, start_env, "no_handler", True, "handler not registered")
            await self._cleanup_after_run()
            return

        ctx: RunContext | None = None
        try:
            await handler.init(
                {
                    "task_id": self.active.task_id,
                    "node_id": self.active.node_id,
                    "attempt_epoch": self.active.attempt_epoch,
                    "worker_id": self.worker_id,
                    "role": role,
                }
            )
            artifacts = ArtifactsWriter(
                db=self.db,
                clock=self.clock,
                task_id=self.active.task_id,
                node_id=self.active.node_id,
                attempt_epoch=self.active.attempt_epoch,
                worker_id=self.worker_id,
            )
            ctx = RunContext(
                cancel_flag=self._cancel_flag,
                cancel_meta=self._cancel_meta,
                artifacts_writer=artifacts,
                clock=self.clock,
                task_id=self.active.task_id,
                node_id=self.active.node_id,
                attempt_epoch=self.active.attempt_epoch,
                worker_id=self.worker_id,
            )

            loaded = await handler.load_input(cmd.input_ref, cmd.input_inline)

            # Choose input adapter if requested
            adapter_name = None
            adapter_args: dict[str, Any] = {}
            if isinstance(loaded, dict):
                inline = loaded.get("input_inline") or {}
                adapter_name = inline.get("input_adapter")
                adapter_args = inline.get("input_args", {}) or {}

            if adapter_name and adapter_name in self.input_adapters:
                from_nodes = (
                    adapter_args.get("from_nodes")
                    or (adapter_args.get("from_node") and [adapter_args["from_node"]])
                    or []
                )
                adapter_kwargs = dict(adapter_args)
                adapter_kwargs.pop("from_nodes", None)
                adapter_kwargs.pop("from_node", None)
                adapter_kwargs.setdefault("poll_ms", self.cfg.pull_poll_ms_default)
                adapter_kwargs.setdefault("eof_on_task_done", True)
                adapter_kwargs.setdefault("backoff_max_ms", self.cfg.pull_empty_backoff_ms_max)

                _json_log(
                    self.clock,
                    event="adapter_selected",
                    node=self.active.node_id,
                    adapter=adapter_name,
                    from_nodes=from_nodes,
                    kwargs=adapter_kwargs,
                )

                async def _iter_batches_adapter():
                    it = self.input_adapters[adapter_name](
                        task_id=self.active.task_id,
                        consumer_node=self.active.node_id,
                        from_nodes=from_nodes,
                        cancel_flag=self._cancel_flag,
                        **adapter_kwargs,
                    )
                    async for b in it:
                        if isinstance(b.payload, dict):
                            _cnt = len(b.payload.get("items") or b.payload.get("skus") or [])
                            _json_log(
                                self.clock,
                                event="adapter_batch",
                                node=self.active.node_id,
                                batch_uid=b.batch_uid,
                                items=_cnt,
                            )
                        yield b

                batch_iter = _iter_batches_adapter()
            else:
                _json_log(self.clock, event="handler_iter_batches_fallback", node=self.active.node_id)
                batch_iter = handler.iter_batches(loaded)

            self._spawn(self._db_cancel_watch_loop())
            async for batch in batch_iter:
                if self._cancel_flag.is_set():
                    raise asyncio.CancelledError()

                uid = batch.batch_uid or stable_hash({"payload": batch.payload})
                try:
                    res = await handler.process_batch(batch, ctx)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    reason, permanent = handler.classify_error(e)
                    res = BatchResult(success=False, reason_code=reason, permanent=permanent, error=str(e), metrics={})

                if res.success:
                    artifacts_ref = res.artifacts_ref
                    try:
                        artifacts_ref = await artifacts.upsert_partial(uid, res.metrics or {})
                    except Exception as e:
                        _json_log(self.clock, level="ERROR", msg="upsert_partial failed", batch_uid=uid, error=str(e))
                        artifacts_ref = artifacts_ref or {"batch_uid": uid}
                    await self._emit_batch_ok(role, start_env, batch, res, uid, override_artifacts_ref=artifacts_ref)
                    continue

                await self._emit_batch_failed(
                    role, start_env, batch, res, uid, override_artifacts_ref={"batch_uid": uid}
                )
                await self._emit_task_failed(
                    role, start_env, res.reason_code or "error", bool(res.permanent), res.error
                )
                return

            fin = await handler.finalize(ctx)
            metrics = (fin.metrics if fin else {}) if fin else {}
            ref = fin.artifacts_ref if fin else None
            ref = await artifacts.mark_complete(metrics, ref)
            await self._emit_task_done(role, start_env, metrics, ref)

        except asyncio.CancelledError:
            if self._stopping:
                _json_log(
                    self.clock, event="shutdown_preserve_state", node=self.active.node_id if self.active else None
                )
            else:
                await self._emit_cancelled(role, start_env, "cancelled")
            return
        except Exception as e:
            reason, permanent = handler.classify_error(e) if handler else ("unexpected_error", False)
            await self._emit_task_failed(role, start_env, reason, permanent, str(e))
            return
        finally:
            try:
                if ctx is not None:
                    await ctx._cleanup_resources()
            except Exception:
                pass
            if not self._stopping:
                await self._cleanup_after_run()

    async def _cleanup_after_run(self) -> None:
        self._cancel_flag.clear()
        self.active = None
        await self.state.write_active(None)
        self._busy = False
        self._cancel_meta["reason"] = None
        self._cancel_meta["deadline_ts_ms"] = None
        await self._resume_all_cmd_consumers()

    # ---------- Heartbeat ----------
    async def _heartbeat_loop(self, role: str) -> None:
        assert self.active is not None
        try:
            while self._busy and not self._stopping and self.active is not None:
                lease_deadline_ms = self.clock.now_ms() + self.cfg.lease_ttl_ms
                hb_env = Envelope(
                    msg_type=MsgType.event,
                    role=Role.worker,
                    dedup_id=stable_hash(
                        {
                            "hb": self.active.task_id,
                            "n": self.active.node_id,
                            "e": self.active.attempt_epoch,
                            "t": int(self.clock.now_ms() / max(1, self.cfg.hb_interval_ms)),
                        }
                    ),
                    task_id=self.active.task_id,
                    node_id=self.active.node_id,
                    step_type=role,
                    attempt_epoch=self.active.attempt_epoch,
                    ts_ms=self.clock.now_ms(),
                    payload={
                        "kind": EventKind.TASK_HEARTBEAT,
                        "worker_id": self.worker_id,
                        "lease_id": self.active.lease_id,
                        "lease_deadline_ts_ms": lease_deadline_ms,
                    },
                )
                await self._send_status(role, hb_env)
                await self.clock.sleep_ms(self.cfg.hb_interval_ms)
        except asyncio.CancelledError:
            return

    # ---------- Status emitters ----------
    async def _emit_batch_ok(
        self,
        role: str,
        base: Envelope,
        batch: Batch,
        res: BatchResult,
        batch_uid: str,
        override_artifacts_ref: dict[str, Any] | None = None,
    ) -> None:
        artifacts_ref = override_artifacts_ref if override_artifacts_ref is not None else res.artifacts_ref
        env = Envelope(
            msg_type=MsgType.event,
            role=Role.worker,
            dedup_id=stable_hash({"bok": base.task_id, "n": base.node_id, "e": base.attempt_epoch, "uid": batch_uid}),
            task_id=base.task_id,
            node_id=base.node_id,
            step_type=role,
            attempt_epoch=base.attempt_epoch,
            ts_ms=self.clock.now_ms(),
            payload={
                "kind": EventKind.BATCH_OK,
                "worker_id": self.worker_id,
                "batch_uid": batch_uid,
                "metrics": res.metrics or {},
                "artifacts_ref": artifacts_ref,
            },
        )
        await self._send_status(role, env)

    async def _emit_batch_failed(
        self,
        role: str,
        base: Envelope,
        batch: Batch,
        res: BatchResult,
        batch_uid: str,
        override_artifacts_ref: dict[str, Any] | None = None,
    ) -> None:
        env = Envelope(
            msg_type=MsgType.event,
            role=Role.worker,
            dedup_id=stable_hash({"bf": base.task_id, "n": base.node_id, "e": base.attempt_epoch, "uid": batch_uid}),
            task_id=base.task_id,
            node_id=base.node_id,
            step_type=role,
            attempt_epoch=base.attempt_epoch,
            ts_ms=self.clock.now_ms(),
            payload={
                "kind": EventKind.BATCH_FAILED,
                "worker_id": self.worker_id,
                "batch_uid": batch_uid,
                "reason_code": res.reason_code or "error",
                "permanent": bool(res.permanent),
                "error": res.error,
                "artifacts_ref": override_artifacts_ref,
            },
        )
        await self._send_status(role, env)

    async def _emit_task_done(
        self, role: str, base: Envelope, metrics: dict[str, int], artifacts_ref: dict[str, Any] | None
    ) -> None:
        env = Envelope(
            msg_type=MsgType.event,
            role=Role.worker,
            dedup_id=stable_hash({"td": base.task_id, "n": base.node_id, "e": base.attempt_epoch}),
            task_id=base.task_id,
            node_id=base.node_id,
            step_type=role,
            attempt_epoch=base.attempt_epoch,
            ts_ms=self.clock.now_ms(),
            payload={
                "kind": EventKind.TASK_DONE,
                "worker_id": self.worker_id,
                "metrics": metrics or {},
                "artifacts_ref": artifacts_ref,
                "final_uid": "__final__",
            },
        )
        await self._send_status(role, env)

    async def _emit_task_failed(
        self, role: str, base: Envelope, reason: str, permanent: bool, error: str | None
    ) -> None:
        env = Envelope(
            msg_type=MsgType.event,
            role=Role.worker,
            dedup_id=stable_hash(
                {"tf": base.task_id, "n": base.node_id, "e": base.attempt_epoch, "r": reason, "p": permanent}
            ),
            task_id=base.task_id,
            node_id=base.node_id,
            step_type=role,
            attempt_epoch=base.attempt_epoch,
            ts_ms=self.clock.now_ms(),
            payload={
                "kind": EventKind.TASK_FAILED,
                "worker_id": self.worker_id,
                "reason_code": reason,
                "permanent": bool(permanent),
                "error": error,
            },
        )
        await self._send_status(role, env)

    async def _emit_cancelled(self, role: str, base: Envelope, reason: str) -> None:
        env = Envelope(
            msg_type=MsgType.event,
            role=Role.worker,
            dedup_id=stable_hash({"c": base.task_id, "n": base.node_id, "e": base.attempt_epoch}),
            task_id=base.task_id,
            node_id=base.node_id,
            step_type=role,
            attempt_epoch=base.attempt_epoch,
            ts_ms=self.clock.now_ms(),
            payload={"kind": EventKind.CANCELLED, "worker_id": self.worker_id, "reason": reason},
        )
        await self._send_status(role, env)

    # ---------- Discovery (TASK_DISCOVER → TASK_SNAPSHOT) ----------
    async def _query_loop(self, consumer: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await consumer.getone()
                env = Envelope.model_validate(msg.value)
                if env.msg_type != MsgType.query or env.role != Role.coordinator:
                    await consumer.commit()
                    continue
                try:
                    qkind = env.payload.get("query")
                    qkind = qkind if isinstance(qkind, QueryKind) else QueryKind(qkind)
                except Exception:
                    qkind = None
                if qkind != QueryKind.TASK_DISCOVER:
                    await consumer.commit()
                    continue
                if env.step_type not in self.cfg.roles:
                    await consumer.commit()
                    continue

                ar = self.active
                if ar and ar.task_id == env.task_id and ar.node_id == env.node_id:
                    run_state = "cancelling" if self._cancel_flag.is_set() else "running"
                    payload = {
                        "reply": ReplyKind.TASK_SNAPSHOT,
                        "worker_id": self.worker_id,
                        "run_state": run_state,
                        "attempt_epoch": ar.attempt_epoch,
                        "lease": {
                            "worker_id": self.worker_id,
                            "lease_id": ar.lease_id,
                            "deadline_ts_ms": self.clock.now_ms() + self.cfg.lease_ttl_ms,
                        },
                        "progress": {},
                        "artifacts": None,
                    }
                else:
                    complete = False
                    try:
                        cnt = await self.db.artifacts.count_documents(
                            {"task_id": env.task_id, "node_id": env.node_id, "status": "complete"}
                        )
                        complete = cnt > 0
                    except Exception:
                        pass
                    payload = {
                        "reply": ReplyKind.TASK_SNAPSHOT,
                        "worker_id": None,
                        "run_state": "idle",
                        "attempt_epoch": 0,
                        "lease": None,
                        "progress": None,
                        "artifacts": {"complete": complete} if complete else None,
                    }

                reply = Envelope(
                    msg_type=MsgType.reply,
                    role=Role.worker,
                    dedup_id=stable_hash(
                        {"snap": env.task_id, "n": env.node_id, "e": env.attempt_epoch, "w": self.worker_id}
                    ),
                    task_id=env.task_id,
                    node_id=env.node_id,
                    step_type=env.step_type,
                    attempt_epoch=env.attempt_epoch,
                    ts_ms=self.clock.now_ms(),
                    payload=payload,
                    corr_id=env.corr_id,
                )
                await self._send_reply(reply)
                await consumer.commit()
        except asyncio.CancelledError:
            return

    async def _periodic_announce(self) -> None:
        try:
            while not self._stopping:
                await self.clock.sleep_ms(self.cfg.announce_interval_ms)
                await self._send_announce(
                    EventKind.WORKER_ONLINE,
                    extra={
                        "worker_id": self.worker_id,
                        "type": ",".join(self.cfg.roles),
                        "version": self.worker_version,
                        "capacity": {"tasks": 1},
                    },
                )
        except asyncio.CancelledError:
            return

    # ---------- Signals loop (CANCEL) ----------
    async def _signals_loop(self, consumer: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await consumer.getone()
                env = Envelope.model_validate(msg.value)

                if env.target_worker_id and env.target_worker_id != self.worker_id:
                    await consumer.commit()
                    continue

                pay = env.payload or {}
                if (pay.get("sig") or "").upper() != "CANCEL":
                    await consumer.commit()
                    continue
                try:
                    sc = SigCancel.model_validate(pay)
                except Exception:
                    await consumer.commit()
                    continue

                ar = self.active
                if not ar:
                    await consumer.commit()
                    continue

                if ar.task_id == env.task_id and ar.node_id == env.node_id and ar.attempt_epoch == env.attempt_epoch:
                    self._cancel_meta["reason"] = sc.reason
                    self._cancel_meta["deadline_ts_ms"] = sc.deadline_ts_ms
                    self._cancel_flag.set()
                    ar.state = "cancelling"
                    await self.state.write_active(ar)
                await consumer.commit()
        except asyncio.CancelledError:
            return

    # ---------- DB cancel watcher (failsafe) ----------
    async def _db_cancel_watch_loop(self) -> None:
        try:
            while self._busy and not self._stopping and self.active is not None and not self._cancel_flag.is_set():
                ar = self.active
                try:
                    doc = await self.db.tasks.find_one({"id": ar.task_id}, {"id": 1, "coordinator": 1, "graph": 1})
                    if doc:
                        if (doc.get("coordinator") or {}).get("cancelled") is True:
                            if not self._cancel_meta["reason"]:
                                self._cancel_meta["reason"] = "db_flag"
                            self._cancel_flag.set()
                            break
                        for n in (doc.get("graph", {}) or {}).get("nodes") or []:
                            if n.get("node_id") == ar.node_id:
                                if (n.get("coordinator") or {}).get("cancelled") is True or n.get(
                                    "status"
                                ) == "cancelling":
                                    if not self._cancel_meta["reason"]:
                                        self._cancel_meta["reason"] = "db_flag"
                                    self._cancel_flag.set()
                                break
                except Exception:
                    pass
                await self.clock.sleep_ms(self.cfg.db_cancel_poll_ms)
        except asyncio.CancelledError:
            return

    # ---------- DB indexes ----------
    async def _ensure_indexes(self) -> None:
        try:
            await self.db.artifacts.create_index([("task_id", 1), ("node_id", 1)], name="ix_artifacts_task_node")
            await self.db.artifacts.create_index(
                [("task_id", 1), ("node_id", 1), ("batch_uid", 1)],
                unique=True,
                sparse=True,
                name="uniq_artifact_batch_uid",
            )
            await self.db.stream_progress.create_index(
                [("task_id", 1), ("consumer_node", 1), ("from_node", 1), ("batch_uid", 1)],
                unique=True,
                name="uniq_stream_claim_batch",
            )
        except Exception:
            pass
