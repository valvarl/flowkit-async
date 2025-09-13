from __future__ import annotations

import asyncio
import copy
import logging
import uuid
from collections import OrderedDict
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ..core.config import WorkerConfig
from ..core.log import bind_context, get_logger, log_context, swallow, warn_once
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
from .state import ActiveRun, LocalStateManager


class Worker:
    """
    Stream-aware worker with cooperative cancellation and resilient batching.
    - Kafka I/O (producers/consumers per role)
    - Discovery (TASK_DISCOVER → TASK_SNAPSHOT)
    - Control-plane CANCEL via signals topic
    - DB-backed state for resume/takeover
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

        self.state = LocalStateManager(db=self.db, clock=self.clock, worker_id=self.worker_id)
        self.active: ActiveRun | None = None

        # dedup of command envelopes
        self._dedup: OrderedDict[str, int] = OrderedDict()
        self._dedup_lock = asyncio.Lock()

        self._main_tasks: set[asyncio.Task] = set()

        # logging
        self.log = get_logger("worker")
        bind_context(role="worker", worker_id=self.worker_id, version=self.worker_version)
        self.log.debug("worker.init", event="worker.init", roles=self.cfg.roles, version=self.worker_version)

    # ---------- lifecycle ----------
    async def start(self) -> None:
        # Helpful config print (silent by default unless tests enable stdout)
        cfg_dump: Any
        if hasattr(self.cfg, "model_dump"):  # pydantic v2
            cfg_dump = self.cfg.model_dump()
        elif hasattr(self.cfg, "dict"):  # pydantic v1
            cfg_dump = self.cfg.dict()
        else:
            cfg_dump = getattr(self.cfg, "__dict__", str(self.cfg))
        self.log.debug("worker.start", event="worker.start", cfg=cfg_dump)

        await self._ensure_indexes()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.cfg.kafka_bootstrap, value_serializer=dumps, enable_idempotence=True
        )
        await self._producer.start()

        await self.state.refresh()
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
            self.log.debug(
                "worker.recovery_present",
                event="worker.recovery_present",
                task_id=self.active.task_id,
                node_id=self.active.node_id,
            )

        self.log.debug("worker.started", event="worker.started", worker_id=self.worker_id)

    async def stop(self) -> None:
        self._stopping = True
        for t in list(self._main_tasks):
            t.cancel()
        self._main_tasks.clear()

        if self._query_consumer:
            with swallow(
                logger=self.log,
                code="worker.query_consumer.stop",
                msg="query consumer stop failed",
                level=logging.WARNING,
            ):
                await self._query_consumer.stop()
        if self._signals_consumer:
            with swallow(
                logger=self.log,
                code="worker.signals_consumer.stop",
                msg="signals consumer stop failed",
                level=logging.WARNING,
            ):
                await self._signals_consumer.stop()
        for c in self._cmd_consumers.values():
            with swallow(
                logger=self.log, code="worker.cmd_consumer.stop", msg="cmd consumer stop failed", level=logging.WARNING
            ):
                await c.stop()
        self._cmd_consumers.clear()

        if self._producer:
            with swallow(
                logger=self.log, code="worker.announce.offline", msg="announce offline failed", level=logging.WARNING
            ):
                await self._send_announce(EventKind.WORKER_OFFLINE, extra={"worker_id": self.worker_id})
            with swallow(
                logger=self.log, code="worker.producer.stop", msg="producer stop failed", level=logging.WARNING
            ):
                await self._producer.stop()
        self._producer = None

    def _spawn(self, coro, *, name: str | None = None) -> None:
        t = asyncio.create_task(coro, name=name or getattr(coro, "__name__", "task"))
        self._main_tasks.add(t)

        def _done(task: asyncio.Task) -> None:
            self._main_tasks.discard(task)
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                self.log.error("worker.task.crashed", event="worker.task.crashed", task=task.get_name(), exc_info=True)

        t.add_done_callback(_done)

    async def _commit_with_warn(self, consumer: AIOKafkaConsumer, *, code: str, msg: str) -> None:
        try:
            await consumer.commit()
        except Exception:
            warn_once(self.log, code=code, msg=msg, level=logging.WARNING)

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
                    await self._commit_with_warn(
                        consumer,
                        code=f"worker.commit.cmd.{role}",
                        msg=f"Kafka commit failed in cmd consumer for {role} (suppressed next occurrences)",
                    )
                    continue
                if env.msg_type != MsgType.cmd or env.role != Role.coordinator:
                    await self._commit_with_warn(
                        consumer,
                        code=f"worker.commit.cmd.{role}",
                        msg=f"Kafka commit failed in cmd consumer for {role} (suppressed next occurrences)",
                    )
                    continue
                if env.step_type != role:
                    await self._commit_with_warn(
                        consumer,
                        code=f"worker.commit.cmd.{role}",
                        msg=f"Kafka commit failed in cmd consumer for {role} (suppressed next occurrences)",
                    )
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
                    await self._commit_with_warn(
                        consumer,
                        code=f"worker.commit.cmd.{role}",
                        msg=f"Kafka commit failed in cmd consumer for {role} (suppressed next occurrences)",
                    )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("worker.cmd.loop.crashed", event="worker.cmd.loop.crashed", role=role, exc_info=True)

    async def _handle_task_start(self, role: str, env: Envelope, consumer: AIOKafkaConsumer) -> None:
        cmd = CmdTaskStart.model_validate(env.payload)
        async with self._busy_lock:
            if self._busy:
                await self._commit_with_warn(
                    consumer,
                    code=f"worker.commit.cmd.{role}",
                    msg=f"Kafka commit failed in cmd consumer for {role} (suppressed next occurrences)",
                )
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
            self.log.debug(
                "worker.send.accepted",
                event="worker.send.accepted",
                task_id=env.task_id,
                node_id=env.node_id,
                epoch=env.attempt_epoch,
                worker_id=self.worker_id,
                lease_id=lease_id,
                deadline=lease_deadline_ms,
            )
            await self._send_status(role, acc_env)

            with log_context(
                task_id=env.task_id,
                node_id=env.node_id,
                attempt_epoch=env.attempt_epoch,
                step_type=role,
            ):
                await self._pause_all_cmd_consumers()
                self._spawn(self._heartbeat_loop(role))
                self._spawn(self._run_handler(role, env, cmd))

            await self._commit_with_warn(
                consumer,
                code=f"worker.commit.cmd.{role}",
                msg=f"Kafka commit failed in cmd consumer for {role} (suppressed next occurrences)",
            )

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
        await self._commit_with_warn(
            consumer,
            code=f"worker.commit.cmd.{role}",
            msg=f"Kafka commit failed in cmd consumer for {role} (suppressed next occurrences)",
        )

    # ---------- Handler execution ----------
    async def _run_handler(self, role: str, start_env: Envelope, cmd: CmdTaskStart) -> None:
        """
        Execute the role handler with strict adapter precedence:
        1) If the Coordinator provided `input_inline.input_adapter`, the worker MUST use it.
           If the adapter is unknown, fail the task (permanent) to avoid silently misrouting data.
        2) Otherwise, if the handler provided a known adapter, use it.
        3) Otherwise, fallback to `handler.iter_batches(loaded)`.
        """
        if self.active is None:
            raise RuntimeError("No active run")

        handler = self.handlers.get(role)
        if not handler:
            await self._emit_task_failed(role, start_env, "no_handler", True, "handler not registered")
            await self._cleanup_after_run()
            return

        ctx: RunContext | None = None
        try:
            # --- init handler & context ---
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

            # --- adapter selection (strict cmd precedence) ---
            cmd_inline = cmd.input_inline or {}
            handler_inline = loaded.get("input_inline") if isinstance(loaded, dict) else None

            cmd_adapter = (cmd_inline or {}).get("input_adapter")
            handler_adapter = (handler_inline or {}).get("input_adapter") if isinstance(handler_inline, dict) else None

            adapter_name: str | None = None
            adapter_args: dict[str, Any] = {}
            io_source = "none"

            # 1) Coordinator-specified adapter (mandatory if present).
            if cmd_adapter:
                if cmd_adapter not in self.input_adapters:
                    self.log.error(
                        "worker.adapter.unknown_from_cmd",
                        event="worker.adapter.unknown_from_cmd",
                        node_id=self.active.node_id,
                        adapter=cmd_adapter,
                    )
                    await self._emit_task_failed(
                        role,
                        start_env,
                        "bad_input_adapter",
                        True,
                        f"unknown input_adapter '{cmd_adapter}'",
                    )
                    return
                adapter_name = cmd_adapter
                adapter_args = cmd_inline.get("input_args") or {}
                io_source = "cmd"

                # Handler tried to propose a different route: ignore it, keep a debug trace.
                if handler_adapter and handler_adapter != cmd_adapter:
                    self.log.debug(
                        "worker.adapter.handler_conflict_ignored",
                        event="worker.adapter.handler_conflict_ignored",
                        node_id=self.active.node_id,
                        handler_adapter=handler_adapter,
                        cmd_adapter=cmd_adapter,
                    )

            # 2) Otherwise use handler-provided adapter if known.
            elif handler_adapter and handler_adapter in self.input_adapters:
                adapter_name = handler_adapter
                adapter_args = (handler_inline.get("input_args") or {}) if isinstance(handler_inline, dict) else {}
                io_source = "handler"

            # 3) Otherwise no adapter → fallback to iter_batches(loaded).

            # --- build batch iterator ---
            if adapter_name:
                # ---- validate and normalize adapter args BEFORE starting streaming ----
                def _normalize_aliases(args: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
                    from_nodes = args.get("from_nodes") or ([args["from_node"]] if args.get("from_node") else [])
                    kwargs = dict(args)
                    kwargs.pop("from_nodes", None)
                    kwargs.pop("from_node", None)
                    kwargs.setdefault("poll_ms", self.cfg.pull_poll_ms_default)
                    kwargs.setdefault("eof_on_task_done", True)
                    kwargs.setdefault("backoff_max_ms", self.cfg.pull_empty_backoff_ms_max)
                    return from_nodes, kwargs

                def _validate_adapter_args(name: str, args: dict[str, Any]) -> tuple[bool, str | None]:
                    if name == "pull.from_artifacts":
                        return True, None
                    if name == "pull.from_artifacts.rechunk:size":
                        if not isinstance(args.get("size"), int) or args["size"] <= 0:
                            return False, "missing or invalid 'size' (positive int required)"
                        mlk = args.get("meta_list_key", None)
                        if mlk is not None and not isinstance(mlk, str):
                            return False, "'meta_list_key' must be a string if provided"
                        # strict mode: require meta_list_key
                        if self.cfg.strict_input_adapters and not isinstance(mlk, str):
                            return False, "strict mode: 'meta_list_key' is required and must be a string"
                        return True, None
                    return False, f"unknown adapter '{name}'"

                # normalize first
                from_nodes, adapter_kwargs = _normalize_aliases(adapter_args)
                ok, why = _validate_adapter_args(adapter_name, {**adapter_kwargs, "from_nodes": from_nodes})
                if not ok:
                    self.log.error(
                        "worker.adapter.validation_failed",
                        event="worker.adapter.validation_failed",
                        node_id=self.active.node_id,
                        adapter=adapter_name,
                        io_source=io_source,
                        reason=why,
                    )
                    await self._emit_task_failed(
                        role,
                        start_env,
                        "bad_input_args",
                        True,
                        why,
                    )
                    return

                # pull routing fields out of args
                self.log.debug(
                    "worker.adapter.selected",
                    event="worker.adapter.selected",
                    node_id=self.active.node_id,
                    adapter=adapter_name,
                    from_nodes=from_nodes,
                    adapter_kwargs=adapter_kwargs,
                    io_source=io_source,
                )
                self.log.debug(
                    "worker.adapter.validation",
                    event="worker.adapter.validation",
                    node_id=self.active.node_id,
                    adapter=adapter_name,
                    validation_status="ok",
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
                        # Avoid product-specific assumptions; log only generic info.
                        self.log.debug(
                            "worker.adapter.batch",
                            event="worker.adapter.batch",
                            node_id=self.active.node_id,
                            batch_uid=b.batch_uid,
                        )
                        yield b

                batch_iter = _iter_batches_adapter()
            else:
                self.log.debug(
                    "worker.handler.iter_batches_fallback",
                    event="worker.handler.iter_batches_fallback",
                    node_id=self.active.node_id,
                    io_source="handler",
                )
                batch_iter = handler.iter_batches(loaded)

            # --- streaming loop ---
            self._spawn(self._db_cancel_watch_loop())
            had_batches = False
            async for batch in batch_iter:
                had_batches = True
                if self._cancel_flag.is_set():
                    raise asyncio.CancelledError()

                uid = batch.batch_uid or stable_hash({"payload": batch.payload})
                try:
                    res = await handler.process_batch(batch, ctx)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    reason, permanent = handler.classify_error(e)
                    res = BatchResult(
                        success=False,
                        reason_code=reason,
                        permanent=permanent,
                        error=str(e),
                        metrics={},
                    )

                if res.success:
                    artifacts_ref = res.artifacts_ref
                    try:
                        artifacts_ref = await artifacts.upsert_partial(uid, res.metrics or {})
                    except Exception as e:
                        self.log.error(
                            "worker.artifacts.upsert_partial.failed",
                            event="worker.artifacts.upsert_partial.failed",
                            batch_uid=uid,
                            error=str(e),
                            exc_info=True,
                        )
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

            self.log.debug(
                "worker.upstream.status",
                event="worker.upstream.status",
                node_id=self.active.node_id,
                upstream_empty=(not had_batches),
            )

            fin = await handler.finalize(ctx)
            metrics = (fin.metrics if fin else {}) if fin else {}
            ref = fin.artifacts_ref if fin else None
            ref = await artifacts.mark_complete(metrics, ref)
            await self._emit_task_done(role, start_env, metrics, ref)

        except asyncio.CancelledError:
            if self._stopping:
                self.log.debug(
                    "worker.shutdown.preserve_state",
                    event="worker.shutdown.preserve_state",
                    node_id=self.active.node_id if self.active else None,
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
        if self.active is None:
            raise RuntimeError("No active run")
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
                self.log.debug(
                    "worker.send.hb",
                    event="worker.send.hb",
                    task_id=self.active.task_id,
                    node_id=self.active.node_id,
                    epoch=self.active.attempt_epoch,
                    worker_id=self.worker_id,
                    lease_id=self.active.lease_id,
                    deadline=lease_deadline_ms,
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
                    await self._commit_with_warn(
                        consumer,
                        code="worker.commit.query",
                        msg="Kafka commit failed in query consumer (suppressed next occurrences)",
                    )
                    continue
                try:
                    qkind = env.payload.get("query")
                    qkind = qkind if isinstance(qkind, QueryKind) else QueryKind(qkind)
                except Exception:
                    qkind = None
                if qkind != QueryKind.TASK_DISCOVER:
                    await self._commit_with_warn(
                        consumer,
                        code="worker.commit.query",
                        msg="Kafka commit failed in query consumer (suppressed next occurrences)",
                    )
                    continue
                if env.step_type not in self.cfg.roles:
                    await self._commit_with_warn(
                        consumer,
                        code="worker.commit.query",
                        msg="Kafka commit failed in query consumer (suppressed next occurrences)",
                    )
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
                await self._commit_with_warn(
                    consumer,
                    code="worker.commit.query",
                    msg="Kafka commit failed in query consumer (suppressed next occurrences)",
                )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("worker.query.loop.crashed", event="worker.query.loop.crashed", exc_info=True)

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
                    await self._commit_with_warn(
                        consumer,
                        code="worker.commit.signals",
                        msg="Kafka commit failed in signals consumer (suppressed next occurrences)",
                    )
                    continue

                pay = env.payload or {}
                if (pay.get("sig") or "").upper() != "CANCEL":
                    await self._commit_with_warn(
                        consumer,
                        code="worker.commit.signals",
                        msg="Kafka commit failed in signals consumer (suppressed next occurrences)",
                    )
                    continue
                try:
                    sc = SigCancel.model_validate(pay)
                except Exception:
                    await self._commit_with_warn(
                        consumer,
                        code="worker.commit.signals",
                        msg="Kafka commit failed in signals consumer (suppressed next occurrences)",
                    )
                    continue

                ar = self.active
                if not ar:
                    await self._commit_with_warn(
                        consumer,
                        code="worker.commit.signals",
                        msg="Kafka commit failed in signals consumer (suppressed next occurrences)",
                    )
                    continue

                if ar.task_id == env.task_id and ar.node_id == env.node_id and ar.attempt_epoch == env.attempt_epoch:
                    self._cancel_meta["reason"] = sc.reason
                    self._cancel_meta["deadline_ts_ms"] = sc.deadline_ts_ms
                    self._cancel_flag.set()
                    ar.state = "cancelling"
                    await self.state.write_active(ar)
                await self._commit_with_warn(
                    consumer,
                    code="worker.commit.signals",
                    msg="Kafka commit failed in signals consumer (suppressed next occurrences)",
                )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("worker.signals.loop.crashed", event="worker.signals.loop.crashed", exc_info=True)

    # ---------- DB cancel watcher (failsafe) ----------
    async def _db_cancel_watch_loop(self) -> None:
        try:
            while self._busy and not self._stopping and self.active is not None and not self._cancel_flag.is_set():
                ar = self.active
                with swallow(
                    logger=self.log,
                    code="worker.db_cancel_watch",
                    msg="db cancel watch iteration failed",
                    level=logging.WARNING,
                    expected=True,
                ):
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
                await self.clock.sleep_ms(self.cfg.db_cancel_poll_ms)
        except asyncio.CancelledError:
            return

    # ---------- DB indexes ----------
    async def _ensure_indexes(self) -> None:
        with swallow(
            logger=self.log,
            code="worker.idx.artifacts1",
            msg="create index artifacts (task,node) failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.artifacts.create_index([("task_id", 1), ("node_id", 1)], name="ix_artifacts_task_node")
        with swallow(
            logger=self.log,
            code="worker.idx.artifacts2",
            msg="create index artifacts batch failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.artifacts.create_index(
                [("task_id", 1), ("node_id", 1), ("batch_uid", 1)],
                unique=True,
                sparse=True,
                name="uniq_artifact_batch_uid",
            )
        with swallow(
            logger=self.log,
            code="worker.idx.stream_progress",
            msg="create index stream_progress failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.stream_progress.create_index(
                [("task_id", 1), ("consumer_node", 1), ("from_node", 1), ("batch_uid", 1)],
                unique=True,
                name="uniq_stream_claim_batch",
            )
        with swallow(
            logger=self.log,
            code="worker.idx.worker_state",
            msg="create index worker_state ttl failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.worker_state.create_index(
                [("updated_at", 1)],
                expireAfterSeconds=7 * 24 * 3600,
                name="ttl_worker_state_updated_at",
            )
