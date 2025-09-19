# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import uuid
from collections import OrderedDict
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer

from ..transport.kafka_bus import KafkaBus
from ..core.config import WorkerConfig
from ..core.log import bind_context, get_logger, log_context, swallow, warn_once
from ..core.time import Clock, SystemClock
from ..core.utils import stable_hash
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
)
from .state import ActiveRun


class Worker:
    """
    Stream-aware worker skeleton wired to the control plane (Kafka).

    Responsibilities implemented here:
      - Kafka I/O (command/status/reply/signals/announce topics)
      - Discovery flow (TASK_DISCOVER → TASK_SNAPSHOT)
      - Task lifecycle: ACCEPTED → (HEARTBEATS) → DONE/FAILED/CANCELLED
      - Cooperative cancellation via signals (soft deadline) and local flags
      - Deduplication for incoming command envelopes

    Execution engine integration:
      - This version ships a no-op executor (immediate success).
      - The `_run_task_engine()` method is the single hook to attach your
        StreamEngine (sources → ops → sinks) later without breaking the API.

    Notes:
      - The worker is single-concurrency per process by design; if you need
        to run many tasks per worker, place a fair scheduler in front of
        `_run_task_engine()` (e.g. `runtime/worker/concurrency.WorkerPool`).
      - Durable per-worker state is intentionally avoided here to keep
        the adoption/resume policy under the Coordinator's control.
    """

    def __init__(
        self,
        *,
        cfg: Optional[WorkerConfig] = None,
        clock: Optional[Clock] = None,
        roles: Optional[list[str]] = None,
    ) -> None:
        self.cfg = WorkerConfig.load() if cfg is None else cfg
        if roles:
            self.cfg.worker_types = list(roles)

        self.clock: Clock = clock or SystemClock()

        # identity
        self.worker_id = self.cfg.worker_id or f"w-{uuid.uuid4().hex[:8]}"
        self.worker_version = self.cfg.worker_version

        # transport
        self.bus = KafkaBus(self.cfg.kafka_bootstrap)

        # consumers
        self._cmd_consumers: dict[str, AIOKafkaConsumer] = {}
        self._query_consumer: Optional[AIOKafkaConsumer] = None
        self._signals_consumer: Optional[AIOKafkaConsumer] = None

        # run-state
        self.active: Optional[ActiveRun] = None
        self._busy = False
        self._busy_lock = asyncio.Lock()
        self._stopping = False

        # cancel state (cooperative)
        self._cancel_flag = asyncio.Event()
        self._cancel_meta: dict[str, Any] = {"reason": None, "deadline_ts_ms": None}

        # dedup for incoming commands
        self._dedup: OrderedDict[str, int] = OrderedDict()
        self._dedup_lock = asyncio.Lock()

        # background tasks
        self._bg: set[asyncio.Task] = set()

        # logging
        self.log = get_logger("worker")
        bind_context(role="worker", worker_id=self.worker_id, version=self.worker_version)
        self.log.debug("worker.init", event="worker.init", roles=self.cfg.worker_types, version=self.worker_version)

    # --------------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        """Open bus, start consumers and periodic announce."""
        # Helpful config dump (doesn't spam unless log level allows)
        cfg_dump = self.cfg.model_dump() if hasattr(self.cfg, "model_dump") else str(self.cfg)
        self.log.debug("worker.start", event="worker.start", cfg=cfg_dump)

        await self.bus.start()

        # Initial announce (ONLINE)
        await self._send_announce(
            EventKind.WORKER_ONLINE,
            extra={
                "worker_id": self.worker_id,
                "type": ",".join(self.cfg.worker_types),
                "capabilities": {"roles": self.cfg.worker_types},
                "version": self.worker_version,
                "capacity": {"tasks": 1},
            },
        )

        # command consumers per role
        for role in self.cfg.worker_types:
            topic = self.cfg.topic_cmd(role)
            c = await self.bus.new_consumer([topic], group_id=f"workers.{role}.v2", manual_commit=True)
            self._cmd_consumers[role] = c
            self._spawn(self._cmd_loop(role, c))

        # query consumer (discovery)
        self._query_consumer = await self.bus.new_consumer(
            [self.cfg.topic_reply_query_in], group_id="workers.query.v2", manual_commit=True
        )
        self._spawn(self._query_loop(self._query_consumer))

        # signals consumer (control plane; unique per worker to target CANCEL precisely)
        self._signals_consumer = await self.bus.new_consumer(
            [self.cfg.topic_signals], group_id=f"workers.signals.{self.worker_id}", manual_commit=True
        )
        self._spawn(self._signals_loop(self._signals_consumer))

        # periodic announce (ONLINE heartbeat)
        self._spawn(self._periodic_announce())

        self.log.debug("worker.started", event="worker.started", worker_id=self.worker_id)

    async def stop(self) -> None:
        """Graceful shutdown: stop loops, announce OFFLINE, close bus."""
        self._stopping = True
        for t in list(self._bg):
            t.cancel()
        self._bg.clear()

        # best-effort offline announce
        with swallow(self.log, "worker.announce.offline", "announce offline failed", logging.WARNING, expected=True):
            await self._send_announce(EventKind.WORKER_OFFLINE, extra={"worker_id": self.worker_id})

        await self.bus.stop()
        self._cmd_consumers.clear()
        self._query_consumer = None
        self._signals_consumer = None
        self.log.debug("worker.stopped", event="worker.stopped")

    # --------------------------------------------------------------------- internal helpers

    def _spawn(self, coro, *, name: Optional[str] = None) -> None:
        t = asyncio.create_task(coro, name=name or getattr(coro, "__name__", "task"))
        self._bg.add(t)

        def _done(task: asyncio.Task) -> None:
            self._bg.discard(task)
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

    # --------------------------------------------------------------------- bus senders

    async def _send_status(self, role: str, env: Envelope) -> None:
        topic = self.cfg.topic_status(role)
        key = f"{env.task_id}:{env.node_id}".encode("utf-8")
        await self.bus.send(topic, key, env)

    async def _send_announce(self, kind: EventKind, extra: dict[str, Any]) -> None:
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
        await self.bus.send(self.cfg.topic_worker_announce, None, env)

    async def _send_reply(self, env: Envelope) -> None:
        key = (env.task_id or "*").encode("utf-8")
        await self.bus.send(self.cfg.topic_reply, key, env)

    # --------------------------------------------------------------------- dedup

    async def _seen_or_add(self, dedup_id: str) -> bool:
        """LRU dedup for incoming command envelopes."""
        async with self._dedup_lock:
            now = self.clock.now_ms()
            # drop expired
            for k in list(self._dedup.keys()):
                if now - self._dedup[k] > self.cfg.dedup_ttl_ms:
                    self._dedup.pop(k, None)
            if dedup_id in self._dedup:
                return True
            self._dedup[dedup_id] = now
            # bound size
            while len(self._dedup) > self.cfg.dedup_cache_size:
                self._dedup.popitem(last=False)
            return False

    # --------------------------------------------------------------------- command loop

    async def _cmd_loop(self, role: str, consumer: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await consumer.getone()
                try:
                    env = Envelope.model_validate(msg.value)
                except Exception:
                    await self._commit_with_warn(
                        consumer,
                        code=f"worker.commit.cmd.{role}",
                        msg=f"Kafka commit failed in cmd consumer for {role} (bad envelope; suppressed)",
                    )
                    continue

                # ensure we only process coordinator commands for our role
                if env.msg_type != MsgType.cmd or env.role != Role.coordinator or env.step_type != role:
                    await self._commit_with_warn(
                        consumer,
                        code=f"worker.commit.cmd.{role}",
                        msg=f"Kafka commit failed in cmd consumer for {role} (skipped non-cmd; suppressed)",
                    )
                    continue

                # deduplicate
                if await self._seen_or_add(env.dedup_id):
                    await self._commit_with_warn(
                        consumer,
                        code=f"worker.commit.cmd.{role}",
                        msg=f"Kafka commit failed in cmd consumer for {role} (dedup commit; suppressed)",
                    )
                    continue

                # dispatch by command kind
                try:
                    _k = env.payload.get("cmd")
                    kind = _k if isinstance(_k, CommandKind) else CommandKind(_k)
                except Exception:
                    kind = None

                if kind == CommandKind.TASK_START:
                    await self._on_task_start(role, env, consumer)
                elif kind == CommandKind.TASK_CANCEL:
                    await self._on_task_cancel(role, env, consumer)
                else:
                    await self._commit_with_warn(
                        consumer,
                        code=f"worker.commit.cmd.{role}",
                        msg=f"Kafka commit failed in cmd consumer for {role} (unknown cmd; suppressed)",
                    )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("worker.cmd.loop.crashed", event="worker.cmd.loop.crashed", role=role, exc_info=True)

    async def _on_task_start(self, role: str, env: Envelope, consumer: AIOKafkaConsumer) -> None:
        cmd = CmdTaskStart.model_validate(env.payload)
        async with self._busy_lock:
            # only one task at a time
            if self._busy:
                await self._commit_with_warn(
                    consumer,
                    code=f"worker.commit.cmd.{role}",
                    msg=f"Kafka commit failed in cmd consumer for {role} (busy; suppressed)",
                )
                return

            # reset local flags/state
            self._busy = True
            self._stopping = False
            self._cancel_flag.clear()
            self._cancel_meta.update({"reason": None, "deadline_ts_ms": None})

            # allocate a lease (coordinator treats it as soft assertion)
            lease_id = str(uuid.uuid4())
            lease_deadline_ms = self.clock.now_ms() + self.cfg.lease_ttl_ms

            # active run state (in-memory; durable resume is coordinator-driven)
            self.active = ActiveRun(
                task_id=env.task_id,
                node_id=env.node_id,
                attempt_epoch=env.attempt_epoch,
            )

            # ACK: TASK_ACCEPTED
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

            # run: heartbeat + executor
            with log_context(
                task_id=env.task_id,
                node_id=env.node_id,
                attempt_epoch=env.attempt_epoch,
                step_type=role,
            ):
                await self._pause_all_cmd_consumers()
                self._spawn(self._heartbeat_loop(role, lease_id))
                self._spawn(self._run_task(role, env, cmd))

            await self._commit_with_warn(
                consumer,
                code=f"worker.commit.cmd.{role}",
                msg=f"Kafka commit failed in cmd consumer for {role} (post-start; suppressed)",
            )

    async def _on_task_cancel(self, role: str, env: Envelope, consumer: AIOKafkaConsumer) -> None:
        _ = CmdTaskCancel.model_validate(env.payload)  # schema check; fields are in payload if needed
        ar = self.active
        if ar and ar.task_id == env.task_id and ar.node_id == env.node_id and ar.attempt_epoch == env.attempt_epoch:
            # cooperative cancel, no hard stop here
            self._cancel_meta["reason"] = self._cancel_meta.get("reason") or "coordinator_cmd"
            self._cancel_flag.set()
        await self._commit_with_warn(
            consumer,
            code=f"worker.commit.cmd.{role}",
            msg=f"Kafka commit failed in cmd consumer for {role} (cancel; suppressed)",
        )

    # --------------------------------------------------------------------- handler/execution

    async def _run_task(self, role: str, start_env: Envelope, cmd: CmdTaskStart) -> None:
        """
        Outer task runner: executes the pipeline and emits terminal status.

        This stub version finishes immediately with TASK_DONE, but preserves the
        full control-plane protocol, so that upgrading to a real engine later
        only requires implementing `_run_task_engine()`.
        """
        try:
            # --- TODO(engine): plug your StreamEngine here -------------------
            # If you already have a PluginRegistry/StreamEngine/Stores ready,
            # call `await self._run_task_engine(role, start_env, cmd)`.
            await self._run_task_noop(role, start_env, cmd)
            # -----------------------------------------------------------------
        except asyncio.CancelledError:
            # If the process is shutting down, keep silence; otherwise report CANCELLED.
            if not self._stopping:
                await self._emit_cancelled(role, start_env, "cancelled")
            return
        except Exception as e:
            await self._emit_task_failed(role, start_env, reason="unexpected_error", permanent=False, error=str(e))
            return
        finally:
            # cleanup local state and resume command consumption
            self._cancel_flag.clear()
            self.active = None
            self._busy = False
            await self._resume_all_cmd_consumers()

    async def _run_task_noop(self, role: str, base: Envelope, cmd: CmdTaskStart) -> None:
        """
        Minimal placeholder executor:
          - respects soft cancellation (if set while "working")
          - emits TASK_DONE with empty metrics
        """
        # Simulate some cooperative work window (optional tiny wait)
        await asyncio.sleep(0)
        if self._cancel_flag.is_set():
            raise asyncio.CancelledError()
        await self._emit_task_done(role, base, metrics={}, artifacts_ref=None)

    # Placeholder for future StreamEngine integration
    async def _run_task_engine(self, role: str, base: Envelope, cmd: CmdTaskStart) -> None:  # pragma: no cover
        """
        Plug your StreamEngine here. Suggested outline:

            ctx = AdapterContext(
                task_id=base.task_id,
                node_id=base.node_id,
                attempt_epoch=base.attempt_epoch,
                worker_id=self.worker_id,
                cancel_flag=self._cancel_flag,
                # plus injections: checkpoints/artifacts/hooks/metrics/etc.
            )
            engine = StreamEngine(registry=self.registry, ctx=ctx, hooks=..., metrics=...)
            plan = build_plan_from_cmd(cmd)  # unify cmd.input_inline into engine plan
            await engine.build_from_plan(plan)
            await engine.start()
            await engine.drain()   # graceful finish
            await engine.stop()
            await self._emit_task_done(role, base, metrics=..., artifacts_ref=...)

        BATCH_OK / BATCH_FAILED can be emitted either from inside the engine
        via worker hooks, or synthesised here based on artifacts writes.
        """
        raise NotImplementedError

    # --------------------------------------------------------------------- heartbeats

    async def _heartbeat_loop(self, role: str, lease_id: str) -> None:
        """Periodic TASK_HEARTBEAT until the task is finished or worker is stopping."""
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
                        "lease_id": lease_id,
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
                    lease_id=lease_id,
                    deadline=lease_deadline_ms,
                )
                await self._send_status(role, hb_env)
                await self.clock.sleep_ms(self.cfg.hb_interval_ms)
        except asyncio.CancelledError:
            return

    # --------------------------------------------------------------------- discovery (query→reply)

    async def _query_loop(self, consumer: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await consumer.getone()
                try:
                    env = Envelope.model_validate(msg.value)
                except Exception:
                    await self._commit_with_warn(
                        consumer, code="worker.commit.query", msg="Kafka commit failed (bad envelope; suppressed)"
                    )
                    continue

                # only coordinator queries
                if env.msg_type != MsgType.query or env.role != Role.coordinator:
                    await self._commit_with_warn(
                        consumer, code="worker.commit.query", msg="Kafka commit failed (skipped; suppressed)"
                    )
                    continue

                try:
                    qkind = env.payload.get("query")
                    qkind = qkind if isinstance(qkind, QueryKind) else QueryKind(qkind)
                except Exception:
                    qkind = None

                if qkind != QueryKind.TASK_DISCOVER or env.step_type not in self.cfg.worker_types:
                    await self._commit_with_warn(
                        consumer, code="worker.commit.query", msg="Kafka commit failed (not DISCOVER; suppressed)"
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
                            "lease_id": "inproc",  # informational; coord treats deadlines from heartbeats
                            "deadline_ts_ms": self.clock.now_ms() + self.cfg.lease_ttl_ms,
                        },
                        "progress": {},
                        "artifacts": None,
                    }
                else:
                    # Minimal snapshot when idle (coordinator may also check artifacts directly)
                    payload = {
                        "reply": ReplyKind.TASK_SNAPSHOT,
                        "worker_id": None,
                        "run_state": "idle",
                        "attempt_epoch": 0,
                        "lease": None,
                        "progress": None,
                        "artifacts": None,
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
                    consumer, code="worker.commit.query", msg="Kafka commit failed (post-reply; suppressed)"
                )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("worker.query.loop.crashed", event="worker.query.loop.crashed", exc_info=True)

    # --------------------------------------------------------------------- signals (CANCEL)

    async def _signals_loop(self, consumer: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await consumer.getone()
                try:
                    env = Envelope.model_validate(msg.value)
                except Exception:
                    await self._commit_with_warn(
                        consumer, code="worker.commit.signals", msg="Kafka commit failed (bad envelope; suppressed)"
                    )
                    continue

                # ensure the signal targets this worker
                if env.target_worker_id and env.target_worker_id != self.worker_id:
                    await self._commit_with_warn(
                        consumer, code="worker.commit.signals", msg="Kafka commit failed (not my signal; suppressed)"
                    )
                    continue

                pay = env.payload or {}
                if (pay.get("sig") or "").upper() != "CANCEL":
                    await self._commit_with_warn(
                        consumer, code="worker.commit.signals", msg="Kafka commit failed (not CANCEL; suppressed)"
                    )
                    continue

                # cooperative cancel if the signal matches the current run
                ar = self.active
                if (
                    ar
                    and ar.task_id == env.task_id
                    and ar.node_id == env.node_id
                    and ar.attempt_epoch == env.attempt_epoch
                ):
                    self._cancel_meta["reason"] = pay.get("reason") or self._cancel_meta.get("reason") or "signal"
                    self._cancel_meta["deadline_ts_ms"] = pay.get("deadline_ts_ms")
                    self._cancel_flag.set()

                await self._commit_with_warn(
                    consumer, code="worker.commit.signals", msg="Kafka commit failed (post-signal; suppressed)"
                )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("worker.signals.loop.crashed", event="worker.signals.loop.crashed", exc_info=True)

    # --------------------------------------------------------------------- status emitters

    async def _emit_task_done(self, role: str, base: Envelope, *, metrics: dict[str, Any], artifacts_ref: Any | None):
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

    async def _emit_task_failed(self, role: str, base: Envelope, *, reason: str, permanent: bool, error: str | None):
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

    # --------------------------------------------------------------------- periodic announce

    async def _periodic_announce(self) -> None:
        try:
            while not self._stopping:
                await self.clock.sleep_ms(self.cfg.announce_interval_ms)
                await self._send_announce(
                    EventKind.WORKER_ONLINE,
                    extra={
                        "worker_id": self.worker_id,
                        "type": ",".join(self.cfg.worker_types),
                        "version": self.worker_version,
                        "capacity": {"tasks": 1},
                    },
                )
        except asyncio.CancelledError:
            return
