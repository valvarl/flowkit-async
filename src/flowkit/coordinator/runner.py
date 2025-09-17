from __future__ import annotations

import asyncio
import copy
import logging
import uuid
from typing import Any

from aiokafka import AIOKafkaConsumer

from ..bus.kafka import KafkaBus
from ..core.config import CoordinatorConfig
from ..core.log import (
    bind_context,
    get_logger,
    log_context,
    swallow,
    warn_once,
)
from ..core.time import Clock, SystemClock
from ..core.utils import stable_hash
from ..graph.compiler import prepare_for_task_create
from ..io.validation import normalize_adapter_args, validate_input_adapter
from ..outbox.dispatcher import OutboxDispatcher
from ..protocol.messages import (
    CmdTaskStart,
    CommandKind,
    Envelope,
    EvBatchFailed,
    EvBatchOk,
    EventKind,
    EvHeartbeat,
    EvTaskAccepted,
    EvTaskDone,
    EvTaskFailed,
    MsgType,
    QTaskDiscover,
    QueryKind,
    Role,
    RunState,
    TaskDoc,
)
from .adapters import AdapterError, default_adapters

# Common projection for places where execution plan is needed together with graph.
# Keep it in one place to avoid projection drift between call sites.
_TASK_PROJ_PLAN = {"id": 1, "graph": 1, "execution_plan": 1}


class Coordinator:
    """
    Orchestrates DAG execution across workers via Kafka topics.
    `db` is injected (e.g., Motor client). `clock` is injectable for tests.
    """

    def __init__(
        self,
        *,
        db,
        cfg: CoordinatorConfig | None = None,
        worker_types: list[str] | None = None,
        clock: Clock | None = None,
        adapters: dict[str, Any] | None = None,
        sensitive_detector: Any | None = None,
    ) -> None:
        self.db = db
        self.cfg = copy.deepcopy(cfg) if cfg is not None else CoordinatorConfig.load()
        if worker_types:
            self.cfg.worker_types = list(worker_types)

        self.clock: Clock = clock or SystemClock()
        self.bus = KafkaBus(self.cfg.kafka_bootstrap)
        self.outbox = OutboxDispatcher(db=db, bus=self.bus, cfg=self.cfg, clock=self.clock)
        self.adapters = adapters or dict(default_adapters(db=db, clock=self.clock, detector=sensitive_detector))

        self._tasks: set[asyncio.Task] = set()
        self._running = False

        self._announce_consumer: AIOKafkaConsumer | None = None
        self._status_consumers: dict[str, AIOKafkaConsumer] = {}
        self._query_reply_consumer: AIOKafkaConsumer | None = None

        self._gid = f"coord.{uuid.uuid4().hex[:6]}"

        # logging
        self.log = get_logger("coordinator")
        bind_context(role="coordinator", gid=self._gid)
        self.log.debug("coordinator.init", event="coord.init")

    # ---- lifecycle
    async def start(self) -> None:
        # Helpful config print (silent by default unless tests enable stdout)
        cfg_dump: Any
        if hasattr(self.cfg, "model_dump"):  # pydantic v2
            cfg_dump = self.cfg.model_dump()
        elif hasattr(self.cfg, "dict"):  # pydantic v1
            cfg_dump = self.cfg.dict()
        else:
            cfg_dump = getattr(self.cfg, "__dict__", str(self.cfg))
        self.log.debug("coordinator.start", event="coord.start", cfg=cfg_dump)

        await self._ensure_indexes()
        await self.bus.start()
        await self._start_consumers()
        await self.outbox.start()
        self._running = True
        self._spawn(self._scheduler_loop(), name="scheduler")
        self._spawn(self._heartbeat_monitor(), name="hb-monitor")
        self._spawn(self._finalizer_loop(), name="finalizer")
        self._spawn(self._resume_inflight(), name="resume-inflight")
        self.log.debug("coordinator.started", event="coord.started", gid=self._gid)

    async def stop(self) -> None:
        self._running = False
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()
        with swallow(
            logger=self.log, code="outbox.stop", msg="outbox stop failed", level=logging.ERROR, expected=False
        ):
            await self.outbox.stop()
        with swallow(logger=self.log, code="bus.stop", msg="bus stop failed", level=logging.ERROR, expected=False):
            await self.bus.stop()
        self.log.debug("coordinator.stopped", event="coord.stopped")

    async def restart(self) -> None:
        """
        Soft restart: stop coordinator loops and Kafka consumers, then bring them back up.
        DB/Bus/Outbox instances are preserved so DB-backed deduplication
        (worker_events / metrics_raw / outbox) remains intact.
        """
        # Fully stop current loops/consumers
        await self.stop()
        # Reset internal references for a clean start
        self._announce_consumer = None
        self._query_reply_consumer = None
        self._status_consumers = {}
        self._tasks = set()
        # Keep the same _gid — the consumer group will reconnect
        await self.start()

    def _spawn(self, coro, *, name: str | None = None):
        t = asyncio.create_task(coro, name=name or getattr(coro, "__name__", "task"))
        self._tasks.add(t)

        def _done(task: asyncio.Task) -> None:
            self._tasks.discard(task)
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                self.log.error("task.crashed", event="coord.task.crashed", task=task.get_name(), exc_info=True)

        t.add_done_callback(_done)

    async def _commit_with_warn(self, c: AIOKafkaConsumer, *, code: str, msg: str) -> None:
        """
        Best-effort commit with single-shot warning on failure.
        """
        try:
            await c.commit()
        except Exception:
            warn_once(self.log, code=code, msg=msg, level=logging.WARNING)

    # ---- one-shot debug for readiness skips ----
    def _log_node_not_ready_once(self, *, task_id: str, node: dict[str, Any], reason: str, **extra: Any) -> None:
        """
        Emit a single debug line per (task,node,reason) to explain why the node
        wasn't started. Prevents log spam in tight scheduler loops.
        """
        code = f"node.ready.skip:{task_id}:{node.get('node_id')}:{reason}"
        warn_once(
            self.log,
            code=code,
            msg="node not ready",
            level=logging.DEBUG,
            event="coord.node.ready.no",
            task_id=task_id,
            node_id=node.get("node_id"),
            reason=reason,
            **extra,
        )

    # ---- consumers
    async def _start_consumers(self) -> None:
        self._announce_consumer = await self.bus.new_consumer(
            [self.cfg.topic_worker_announce], group_id=f"{self._gid}.announce", manual_commit=True
        )
        self._spawn(self._run_announce_consumer(self._announce_consumer))

        for t in self.cfg.worker_types:
            topic = self.cfg.topic_status(t)
            c = await self.bus.new_consumer([topic], group_id=f"{self._gid}.status.{t}", manual_commit=True)
            self._status_consumers[t] = c
            self._spawn(self._run_status_consumer(t, c))

        self._query_reply_consumer = await self.bus.new_consumer(
            [self.cfg.topic_reply], group_id=f"{self._gid}.reply", manual_commit=True
        )
        self._spawn(self._run_reply_consumer(self._query_reply_consumer))

    async def _run_reply_consumer(self, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                try:
                    env = Envelope.model_validate(msg.value)
                except Exception:
                    warn_once(
                        self.log,
                        code="reply.envelope.invalid",
                        msg="Invalid reply envelope; skipping (suppressed next occurrences)",
                        level=logging.WARNING,
                    )
                    await self._commit_with_warn(
                        c,
                        code="kafka.commit.reply",
                        msg="Kafka commit failed in reply consumer (suppressed next occurrences)",
                    )
                    continue
                if env.msg_type == MsgType.reply and env.role == Role.worker:
                    self.bus.push_reply(env.corr_id, env)
                await self._commit_with_warn(
                    c,
                    code="kafka.commit.reply",
                    msg="Kafka commit failed in reply consumer (suppressed next occurrences)",
                )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("reply.consumer.crashed", event="coord.reply.crash", exc_info=True)

    async def _run_announce_consumer(self, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                try:
                    env = Envelope.model_validate(msg.value)
                except Exception:
                    warn_once(
                        self.log,
                        code="announce.envelope.invalid",
                        msg="Invalid announce envelope; skipping",
                        level=logging.WARNING,
                    )
                    await self._commit_with_warn(
                        c,
                        code="kafka.commit.announce",
                        msg="Kafka commit failed in announce consumer (suppressed next occurrences)",
                    )
                    continue
                if not (env.msg_type == MsgType.event and env.role == Role.worker):
                    # commit even when skipping non-worker or non-event messages;
                    # otherwise the consumer will re-read the same record forever.
                    await self._commit_with_warn(
                        c,
                        code="kafka.commit.announce",
                        msg="Kafka commit failed in announce consumer (suppressed next occurrences)",
                    )
                    continue
                payload = env.payload
                _k = payload.get("kind")
                try:
                    kind = _k if isinstance(_k, EventKind) else EventKind(_k)
                except Exception:
                    kind = None
                    warn_once(
                        self.log,
                        code="announce.kind.invalid",
                        msg="Invalid 'kind' in worker announce; skipping",
                        level=logging.WARNING,
                        payload=_k,
                    )
                try:
                    if kind == EventKind.WORKER_ONLINE:
                        await self.db.worker_registry.update_one(
                            {"worker_id": payload["worker_id"]},
                            {
                                "$set": {
                                    "worker_id": payload["worker_id"],
                                    "type": payload.get("type"),
                                    "capabilities": payload.get("capabilities"),
                                    "version": payload.get("version"),
                                    "status": "online",
                                    "last_seen": self.clock.now_dt(),
                                    "capacity": payload.get("capacity", {}),
                                }
                            },
                            upsert=True,
                        )
                        self.log.debug("worker.online", event="coord.worker.online", worker_id=payload.get("worker_id"))
                    elif kind == EventKind.WORKER_OFFLINE:
                        await self.db.worker_registry.update_one(
                            {"worker_id": payload["worker_id"]},
                            {"$set": {"status": "offline", "last_seen": self.clock.now_dt()}},
                        )
                        self.log.debug(
                            "worker.offline", event="coord.worker.offline", worker_id=payload.get("worker_id")
                        )
                    else:
                        await self.db.worker_registry.update_one(
                            {"worker_id": payload.get("worker_id")},
                            {"$set": {"last_seen": self.clock.now_dt()}},
                            upsert=True,
                        )
                finally:
                    await self._commit_with_warn(
                        c,
                        code="kafka.commit.announce",
                        msg="Kafka commit failed in announce consumer (suppressed next occurrences)",
                    )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("announce.consumer.crashed", event="coord.announce.crash", exc_info=True)

    async def _run_status_consumer(self, step_type: str, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                try:
                    env = Envelope.model_validate(msg.value)
                except Exception:
                    warn_once(
                        self.log,
                        code="status.envelope.invalid",
                        msg="Invalid status envelope; skipping (suppressed next occurrences)",
                        level=logging.WARNING,
                    )
                    await self._commit_with_warn(
                        c,
                        code=f"kafka.commit.status.{step_type}",
                        msg=f"Kafka commit failed in status consumer for {step_type} (suppressed next occurrences)",
                    )
                    continue

                # record raw worker event (best-effort)
                try:
                    await self._record_worker_event(env)
                except Exception:
                    self.log.warning(
                        "worker_event.record.failed",
                        event="coord.worker_event.upsert_failed",
                        exc_info=True,
                        task_id=env.task_id,
                        node_id=env.node_id,
                    )

                try:
                    tdoc = await self.db.tasks.find_one({"id": env.task_id}, {"graph": 1, "status": 1})
                    if not tdoc:
                        self.log.debug(
                            "status.task.missing",
                            event="coord.status.task_missing",
                            task_id=env.task_id,
                            node_id=env.node_id,
                        )
                        await self._commit_with_warn(
                            c,
                            code=f"kafka.commit.status.{step_type}",
                            msg=f"Kafka commit failed in status consumer for {step_type} (suppressed next occurrences)",
                        )
                        continue
                    node = self._get_node(tdoc, env.node_id)
                    if not node or env.attempt_epoch != int(node.get("attempt_epoch", 0)):
                        self.log.debug(
                            "status.node.epoch_mismatch",
                            event="coord.status.epoch_mismatch",
                            task_id=env.task_id,
                            node_id=env.node_id,
                            env_epoch=env.attempt_epoch,
                            node_epoch=int((node or {}).get("attempt_epoch", 0)),
                        )
                        await self._commit_with_warn(
                            c,
                            code=f"kafka.commit.status.{step_type}",
                            msg=f"Kafka commit failed in status consumer for {step_type} (suppressed next occurrences)",
                        )
                        continue
                except Exception:
                    self.log.warning("status.task.lookup_failed", event="coord.status.lookup_failed", exc_info=True)
                    await self._commit_with_warn(
                        c,
                        code=f"kafka.commit.status.{step_type}",
                        msg=f"Kafka commit failed in status consumer for {step_type} (suppressed next occurrences)",
                    )
                    continue

                # touch last_event_recv_ms (best-effort)
                with swallow(logger=self.log, code="task.touch", msg="task touch failed", level=logging.WARNING):
                    await self.db.tasks.update_one(
                        {"id": env.task_id},
                        {"$max": {"last_event_recv_ms": self.clock.now_ms()}, "$currentDate": {"updated_at": True}},
                    )

                node_status = self._to_runstate(node.get("status"))

                _k = env.payload.get("kind")
                try:
                    kind = _k if isinstance(_k, EventKind) else EventKind(_k)
                except Exception:
                    kind = None
                    warn_once(
                        self.log,
                        code="status.kind.invalid",
                        msg="Invalid 'kind' in status payload; skipping",
                        level=logging.WARNING,
                        payload=_k,
                    )

                needs_running = {
                    EventKind.TASK_ACCEPTED,
                    EventKind.TASK_HEARTBEAT,
                    EventKind.BATCH_OK,
                    EventKind.BATCH_FAILED,
                    EventKind.TASK_DONE,
                }

                allow_even_if_not_running = False
                if kind == EventKind.TASK_FAILED:
                    try:
                        pf = EvTaskFailed.model_validate(env.payload)
                        allow_even_if_not_running = bool(pf.permanent)
                    except Exception:
                        allow_even_if_not_running = False

                if (kind in needs_running) and (node_status != RunState.running) and not allow_even_if_not_running:
                    self.log.debug(
                        "status.skipped.not_running",
                        event="coord.status.not_running",
                        task_id=env.task_id,
                        node_id=env.node_id,
                        kind=str(kind),
                        node_status=str(node_status),
                    )
                    await self._commit_with_warn(
                        c,
                        code=f"kafka.commit.status.{step_type}",
                        msg=f"Kafka commit failed in status consumer for {step_type} (suppressed next occurrences)",
                    )
                    continue

                try:
                    with log_context(
                        task_id=env.task_id,
                        node_id=env.node_id,
                        attempt_epoch=env.attempt_epoch,
                        step_type=env.step_type,
                    ):
                        if kind == EventKind.TASK_ACCEPTED:
                            await self._on_task_accepted(env)
                        elif kind == EventKind.TASK_HEARTBEAT:
                            await self._on_task_heartbeat(env)
                        elif kind == EventKind.TASK_RESUMED:
                            await self._on_task_heartbeat(env)
                        elif kind == EventKind.BATCH_OK:
                            await self._on_batch_ok(env)
                        elif kind == EventKind.BATCH_FAILED:
                            await self._on_batch_failed(env)
                        elif kind == EventKind.TASK_DONE:
                            await self._on_task_done(env)
                        elif kind == EventKind.TASK_FAILED:
                            await self._on_task_failed(env)
                        elif kind == EventKind.CANCELLED:
                            await self._on_cancelled(env)
                finally:
                    await self._commit_with_warn(
                        c,
                        code=f"kafka.commit.status.{step_type}",
                        msg=f"Kafka commit failed in status consumer for {step_type} (suppressed next occurrences)",
                    )
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("status.consumer.crashed", event="coord.status.crash", step_type=step_type, exc_info=True)

    # ---- DAG utils
    def _get_node(self, task_doc: dict[str, Any], node_id: str) -> dict[str, Any] | None:
        nodes: list[dict[str, Any]] = (task_doc.get("graph") or {}).get("nodes") or []
        for n in nodes:
            if n.get("node_id") == node_id:
                return n
        return None

    # ---- Public API
    async def create_task(self, *, params: dict[str, Any], graph: dict[str, Any]) -> str:
        task_id = str(uuid.uuid4())

        try:
            spec, plan, graph_runtime = prepare_for_task_create(
                graph,
                allowed_roles=self.cfg.worker_types,
            )
        except Exception:
            self.log.error("coord.graph.validation_failed", event="coord.graph.validation_failed", exc_info=True)
            raise

        doc = TaskDoc(
            id=task_id,
            pipeline_id=task_id,
            status=RunState.queued,
            params=params,
            graph=graph_runtime,  # normalized runtime view (nodes with runtime fields), no edges/edges_ex
            execution_plan=plan,  # explicit plan for coordinator runtime
            status_history=[{"from": None, "to": RunState.queued, "at": self.clock.now_dt()}],
            started_at=self.clock.now_dt().isoformat(),
            last_event_recv_ms=self.clock.now_ms(),
        ).model_dump(mode="json")

        await self.db.tasks.insert_one(doc)
        self.log.debug("task.created", event="coord.task.created", task_id=task_id)
        return task_id

    def _plan_children(self, task_doc: dict[str, Any], parent_id: str) -> list[str]:
        plan = task_doc.get("execution_plan") or {}
        return list(plan.get("children_by_parent", {}).get(parent_id, []) or [])

    def _plan_trigger_kind(self, task_doc: dict[str, Any], child_id: str, parent_id: str) -> str | None:
        plan = task_doc.get("execution_plan") or {}
        return ((plan.get("triggers", {}) or {}).get(child_id, {}) or {}).get(parent_id)

    def _plan_parents(self, task_doc: dict[str, Any], child_id: str) -> list[str]:
        plan = task_doc.get("execution_plan") or {}
        return list(plan.get("parents_by_child", {}).get(child_id, []) or [])

    def _build_io_plan(self, task_doc: dict[str, Any], node: dict[str, Any]) -> dict[str, Any] | None:
        """Build explicit `input_inline` (adapter + args) for the node.
        Rules:
          - If node already defines an adapter → validate and return as-is.
          - Else if it has parents → default to pull.from_artifacts(from_nodes=parents).
          - Else (no parents) → return None (source handler is expected to generate input).
        """
        io = node.get("io") or {}
        ii_raw = io.get("input_inline")
        ii = (ii_raw or {}) if isinstance(ii_raw, dict) else {}
        adapter = ii.get("input_adapter")
        args = (ii.get("input_args") or {}) if isinstance(ii.get("input_args"), dict) else {}
        start_when = str(io.get("start_when", "ready")).lower()

        parents = self._plan_parents(task_doc, node["node_id"])
        # Fallback: if plan is absent, use depends_on from the runtime graph
        if not parents:
            parents = list(node.get("depends_on") or [])

        # Explicit adapter present → validate and return
        if adapter:
            from_nodes, kwargs_norm = normalize_adapter_args(args or {})
            # Sanity check: from_nodes ⊆ parents (if parents exist)
            if from_nodes and parents:
                invalid = [x for x in from_nodes if x not in parents]
                if invalid:
                    raise ValueError(f"node {node['node_id']}: input_args.from_nodes not subset of parents: {invalid}")
            ok, why = validate_input_adapter(
                adapter, {**kwargs_norm, "from_nodes": from_nodes}, strict=self.cfg.strict_input_adapters
            )
            if not ok:
                raise ValueError(f"node {node['node_id']}: {why}")
            return {"input_adapter": adapter, "input_args": {**kwargs_norm, "from_nodes": from_nodes}}

        if isinstance(ii_raw, dict) and ii_raw and "input_adapter" not in ii_raw:
            return ii_raw

        # No explicit adapter → auto-build if parents exist
        if parents and start_when != "first_batch":
            adapter = "pull.from_artifacts"
            from_nodes, kwargs_norm = normalize_adapter_args({"from_nodes": parents})
            ok, why = validate_input_adapter(
                adapter, {**kwargs_norm, "from_nodes": from_nodes}, strict=self.cfg.strict_input_adapters
            )
            if not ok:
                raise ValueError(f"node {node['node_id']}: {why}")
            return {"input_adapter": adapter, "input_args": {**kwargs_norm, "from_nodes": parents}}

        # Source node: no adapter
        return None

    # ---- Scheduler loop
    async def _scheduler_loop(self) -> None:
        try:
            while True:
                await self._schedule_ready_nodes()
                await self.clock.sleep_ms(self.cfg.scheduler_tick_ms)
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("scheduler.loop.crashed", event="coord.scheduler.crash", exc_info=True)

    def _to_runstate(self, v):
        if isinstance(v, RunState):
            return v
        if isinstance(v, str):
            try:
                return RunState(v)
            except Exception:
                return None
        return None

    async def _first_batch_available(self, task_doc, node) -> bool:
        io = node.get("io") or {}
        inp = io.get("input_inline") or {}
        args = inp.get("input_args") or {}
        from_nodes = args.get("from_nodes") or (node.get("depends_on") or [])
        if not from_nodes:
            return False
        q = {"task_id": task_doc["id"], "node_id": {"$in": from_nodes}, "status": {"$in": ["partial", "complete"]}}
        return (await self.db.artifacts.find_one(q)) is not None

    async def _node_ready(self, task_doc, node) -> bool:
        task_id = task_doc.get("id")
        status = self._to_runstate(node.get("status"))
        if status not in (RunState.queued, RunState.deferred):
            self._log_node_not_ready_once(task_id=task_id, node=node, reason="bad_status", node_status=str(status))
            return False
        if status == RunState.deferred:
            nra = int(node.get("next_retry_at_ms") or 0)
            now = self.clock.now_ms()
            if nra and nra > now:
                self._log_node_not_ready_once(
                    task_id=task_id,
                    node=node,
                    reason="deferred_backoff",
                    next_retry_at_ms=nra,
                    now_ms=now,
                    wait_ms=max(0, nra - now),
                )
                return False

        io = node.get("io") or {}
        start_when = (io.get("start_when") or "").lower()
        if start_when == "first_batch":
            fb = await self._first_batch_available(task_doc, node)
            if not fb:
                args = (io.get("input_inline") or {}).get("input_args") or {}
                fnodes = args.get("from_nodes") or (node.get("depends_on") or [])
                self._log_node_not_ready_once(
                    task_id=task_id, node=node, reason="awaiting_first_batch", from_nodes=fnodes
                )
            return fb

        deps = node.get("depends_on") or []
        dep_states = [self._to_runstate((self._get_node(task_doc, d) or {}).get("status")) for d in deps]
        fan_in = (node.get("fan_in") or "all").lower()
        if fan_in == "all":
            ok = all(s == RunState.finished for s in dep_states)
            if not ok:
                self._log_node_not_ready_once(
                    task_id=task_id,
                    node=node,
                    reason="fanin_all_pending",
                    deps=deps,
                    finished=sum(1 for s in dep_states if s == RunState.finished),
                    total=len(dep_states),
                )
            return ok
        if fan_in == "any":
            ok = any(s == RunState.finished for s in dep_states)
            if not ok:
                self._log_node_not_ready_once(
                    task_id=task_id, node=node, reason="fanin_any_pending", deps=deps, finished=0, total=len(dep_states)
                )
            return ok
        if fan_in.startswith("count:"):
            try:
                k = int(fan_in.split(":", 1)[1])
            except Exception:
                k = len(deps)
            have = sum(1 for s in dep_states if s == RunState.finished)
            ok = have >= k
            if not ok:
                self._log_node_not_ready_once(
                    task_id=task_id,
                    node=node,
                    reason="fanin_count_pending",
                    required=k,
                    finished=have,
                    total=len(dep_states),
                )
            return ok
        return False

    async def _schedule_ready_nodes(self) -> None:
        cur = self.db.tasks.find(
            {"status": {"$in": [RunState.running, RunState.queued, RunState.deferred]}},
            {"id": 1, "graph": 1, "status": 1},
        )
        async for t in cur:
            if (t.get("coordinator") or {}).get("cancelled") is True:
                continue
            if self._to_runstate(t.get("status")) == RunState.queued:
                await self.db.tasks.update_one({"id": t["id"]}, {"$set": {"status": RunState.running}})
                self.log.debug("task.bump.running", event="coord.task.bump_running", task_id=t["id"])
            for n in t.get("graph", {}).get("nodes") or []:
                if n.get("type") == "coordinator_fn":
                    if await self._node_ready(t, n):
                        await self._run_coordinator_fn(t, n)
                    continue
                if await self._node_ready(t, n):
                    await self._preflight_and_maybe_start(t, n)

    async def _run_coordinator_fn(self, task_doc: dict[str, Any], node: dict[str, Any]) -> None:
        try:
            await self.db.tasks.update_one(
                {"id": task_doc["id"], "graph.nodes.node_id": node["node_id"]},
                {
                    "$set": {
                        "graph.nodes.$.status": RunState.running,
                        "graph.nodes.$.started_at": self.clock.now_dt(),
                        "graph.nodes.$.attempt_epoch": int(node.get("attempt_epoch", 0)) + 1,
                    }
                },
            )
            io = node.get("io", {}) or {}
            fn_name = io.get("fn", "noop")
            fn_args = io.get("fn_args", {}) or {}
            fn = self.adapters.get(fn_name)
            if not fn:
                raise AdapterError(f"adapter '{fn_name}' not registered")
            if isinstance(fn_args, dict) and "target" in fn_args and isinstance(fn_args["target"], dict):
                fn_args["target"].setdefault("node_id", node["node_id"])
            await fn(task_doc["id"], **fn_args)
            await self.db.tasks.update_one(
                {"id": task_doc["id"], "graph.nodes.node_id": node["node_id"]},
                {
                    "$set": {
                        "graph.nodes.$.status": RunState.finished,
                        "graph.nodes.$.finished_at": self.clock.now_dt(),
                        "graph.nodes.$.last_event_recv_ms": self.clock.now_ms(),
                    }
                },
            )
            self.log.debug("coord.fn.done", event="coord.fn.done", node_id=node["node_id"])
        except Exception as e:
            backoff = int((node.get("retry_policy") or {}).get("backoff_sec") or 300)
            self.log.error(
                "coord.fn.failed",
                event="coord.fn.failed",
                node_id=node.get("node_id"),
                backoff_sec=backoff,
                exc_info=True,
            )
            await self.db.tasks.update_one(
                {"id": task_doc["id"], "graph.nodes.node_id": node["node_id"]},
                {
                    "$set": {
                        "graph.nodes.$.status": RunState.deferred,
                        "graph.nodes.$.next_retry_at_ms": self.clock.now_ms() + backoff * 1000,
                        "graph.nodes.$.last_error": str(e),
                    }
                },
            )

    # ---- enqueue helpers
    async def _enqueue_cmd(self, env: Envelope) -> None:
        topic = self.cfg.topic_cmd(env.step_type)
        key = f"{env.task_id}:{env.node_id}"
        await self.outbox.enqueue(topic=topic, key=key, env=env)

    async def _enqueue_query(self, env: Envelope) -> None:
        key = env.task_id
        await self.outbox.enqueue(topic=self.cfg.topic_query, key=key, env=env)

    async def _enqueue_signal(self, *, key_worker_id: str, env: Envelope) -> None:
        self.log.debug(
            "signal.cancel.enqueued",
            event="coord.signal.cancel",
            target_worker_id=key_worker_id,
            task_id=env.task_id,
            node_id=env.node_id,
            deadline_ts_ms=(env.payload or {}).get("deadline_ts_ms"),
        )
        await self.outbox.enqueue(topic=self.cfg.topic_signals, key=key_worker_id, env=env)

    async def _preflight_and_maybe_start(self, task_doc: dict[str, Any], node: dict[str, Any]) -> None:
        task_id = task_doc["id"]
        node_id = node["node_id"]
        old_epoch = int(node.get("attempt_epoch", 0))
        new_epoch = old_epoch + 1

        self.log.debug(
            "preflight",
            event="coord.preflight",
            task_id=task_id,
            node_id=node_id,
            old_epoch=old_epoch,
            new_epoch=new_epoch,
        )

        # already complete?
        try:
            cnt = await self.db.artifacts.count_documents(
                {"task_id": task_id, "node_id": node_id, "status": "complete"}
            )
            if cnt > 0:
                await self.db.tasks.update_one(
                    {"id": task_id, "graph.nodes.node_id": node_id},
                    {
                        "$set": {
                            "graph.nodes.$.status": RunState.finished,
                            "graph.nodes.$.finished_at": self.clock.now_dt(),
                            "graph.nodes.$.attempt_epoch": new_epoch,
                        }
                    },
                )
                self.log.debug("preflight.complete.cached", event="coord.preflight.complete", node_id=node_id)
                return
        except Exception:
            self.log.warning(
                "preflight.complete.check_failed", event="coord.preflight.complete_check_failed", exc_info=True
            )

        discover_env = Envelope(
            msg_type=MsgType.query,
            role=Role.coordinator,
            dedup_id=stable_hash(
                {"query": str(QueryKind.TASK_DISCOVER), "task_id": task_id, "node_id": node_id, "epoch": new_epoch}
            ),
            task_id=task_id,
            node_id=node_id,
            step_type=node["type"],
            attempt_epoch=new_epoch,
            ts_ms=self.clock.now_ms(),
            payload=QTaskDiscover(query=QueryKind.TASK_DISCOVER, want_epoch=new_epoch).model_dump(),
        )
        ev = self.bus.register_reply(discover_env.corr_id)
        await self._enqueue_query(discover_env)

        try:
            await asyncio.wait_for(ev.wait(), timeout=self.cfg.discovery_window_sec)
        except TimeoutError:
            self.log.debug("preflight.discover.timeout", event="coord.preflight.discover_timeout")

        replies = self.bus.collect_replies(discover_env.corr_id)
        self.log.debug("preflight.discover.replies", event="coord.preflight.replies", count=len(replies))

        prev_lease_worker = ((node.get("lease") or {}).get("worker_id")) or None

        active = [
            r
            for r in replies
            if (
                r.payload.get("run_state") in ("running", "finishing")
                # The snapshot carries the worker's actual run epoch (ar.attempt_epoch),
                # while the node document stores the node's current epoch.
                and r.payload.get("attempt_epoch") == node.get("attempt_epoch")
                # Adopt is allowed only if the lease owner matches the previous lease worker.
                and prev_lease_worker
                and r.payload.get("worker_id") == prev_lease_worker
                and ((r.payload.get("lease") or {}).get("worker_id") == prev_lease_worker)
            )
        ]

        if active:
            snap = active[0].payload
            lease = snap.get("lease") or {}
            await self.db.tasks.update_one(
                {"id": task_id, "graph.nodes.node_id": node_id},
                {
                    "$set": {
                        "graph.nodes.$.status": RunState.running,
                        "graph.nodes.$.lease.worker_id": lease.get("worker_id"),
                        "graph.nodes.$.lease.lease_id": lease.get("lease_id"),
                        "graph.nodes.$.lease.deadline_ts_ms": lease.get("deadline_ts_ms"),
                    }
                },
            )
            self.log.debug(
                "preflight.adopt",
                event="coord.preflight.adopt",
                task_id=task_id,
                node_id=node_id,
                epoch=int(node.get("attempt_epoch", 0)),
                worker_id=prev_lease_worker,
            )
            await self.db.tasks.update_one({"id": task_id}, {"$max": {"last_event_recv_ms": self.clock.now_ms()}})
            return

        complete = any((r.payload.get("artifacts") or {}).get("complete") for r in replies)
        if complete:
            self.log.debug("preflight.complete.snapshot", event="coord.preflight.complete_snapshot", node_id=node_id)
            await self.db.tasks.update_one(
                {"id": task_id, "graph.nodes.node_id": node_id},
                {
                    "$set": {
                        "graph.nodes.$.status": RunState.finished,
                        "graph.nodes.$.finished_at": self.clock.now_dt(),
                        "graph.nodes.$.attempt_epoch": new_epoch,
                    }
                },
            )
            return

        # soft gating after discovery window
        try:
            fresh = await self.db.tasks.find_one({"id": task_id}, {"graph": 1})
            fresh_node = self._get_node(fresh, node_id) if fresh else None
            last = int((fresh_node or {}).get("last_event_recv_ms") or 0)
            status = self._to_runstate((fresh_node or {}).get("status"))
            if status not in (RunState.deferred,):
                if max(0, self.clock.now_ms() - last) < self.cfg.discovery_window_ms:
                    self.log.debug(
                        "preflight.gate.skip",
                        event="coord.preflight.gate_skip",
                        node_id=node_id,
                        last_event_ms=last,
                        now=self.clock.now_ms(),
                        window_ms=self.cfg.discovery_window_ms,
                    )
                    return
        except Exception:
            self.log.warning("preflight.gate.check_failed", event="coord.preflight.gate_check_failed", exc_info=True)

        # CAS transition
        q = {
            "id": task_id,
            "graph.nodes": {
                "$elemMatch": {
                    "node_id": node_id,
                    "status": {"$in": [RunState.queued, RunState.deferred]},
                    "attempt_epoch": int(node.get("attempt_epoch", 0)),
                }
            },
        }
        upd = {
            "$set": {
                "graph.nodes.$.status": RunState.running,
                "graph.nodes.$.started_at": self.clock.now_dt(),
                "graph.nodes.$.attempt_epoch": new_epoch,
            }
        }
        cas_res = await self.db.tasks.find_one_and_update(q, upd)
        if not cas_res:
            self.log.debug(
                "preflight.cas.skip", event="coord.preflight.cas_skip", node_id=node_id, reason="predicate_failed"
            )
            return

        self.log.debug(
            "preflight.cas.ok", event="coord.preflight.cas_ok", task_id=task_id, node_id=node_id, epoch=new_epoch
        )

        cancel_token = str(uuid.uuid4())

        # Build explicit I/O plan (may be None for source nodes)
        try:
            io_inline = self._build_io_plan(task_doc, node)
        except Exception as e:
            # FAST-FAIL POLICY:
            # Невалидная конфигурация I/O-плана — детерминированная ошибка графа.
            # Сразу фейлим узел и всю задачу, дополнительно инициируем каскадную отмену.
            err = f"io_plan:{e}"
            with swallow(
                logger=self.log,
                code="node.fail.io_plan",
                msg="failed to mark node as failed on io_plan error",
                level=logging.ERROR,
            ):
                await self.db.tasks.update_one(
                    {"id": task_id, "graph.nodes.node_id": node_id},
                    {
                        "$set": {
                            "graph.nodes.$.status": RunState.failed,
                            "graph.nodes.$.finished_at": self.clock.now_dt(),
                            "graph.nodes.$.last_error": err,
                        }
                    },
                )
            # Отменяем остальные узлы (если какие-то уже успели стартовать) и помечаем задачу как failed.
            with swallow(
                logger=self.log,
                code="task.fail.io_plan",
                msg="failed to fail task on io_plan error",
                level=logging.ERROR,
            ):
                await self._cascade_cancel(task_id, reason=err)
                await self.db.tasks.update_one(
                    {"id": task_id},
                    {"$set": {"status": RunState.failed, "finished_at": self.clock.now_dt()}},
                )
            self.log.error(
                "preflight.io_plan.invalid", event="coord.preflight.io_plan.invalid", node_id=node_id, error=str(e)
            )
            return

        cmd = CmdTaskStart(
            cmd=CommandKind.TASK_START,
            input_ref=node.get("io", {}).get("input_ref"),
            input_inline=io_inline,
            batching=node.get("io", {}).get("batching"),
            cancel_token=cancel_token,
        )
        env = Envelope(
            msg_type=MsgType.cmd,
            role=Role.coordinator,
            dedup_id=stable_hash(
                {"cmd": str(CommandKind.TASK_START), "task_id": task_id, "node_id": node_id, "epoch": new_epoch}
            ),
            task_id=task_id,
            node_id=node_id,
            step_type=node["type"],
            attempt_epoch=new_epoch,
            ts_ms=self.clock.now_ms(),
            payload=cmd.model_dump(),
        )
        self.log.debug(
            "cmd.start", event="coord.cmd.start", node_id=node_id, epoch=new_epoch, cancel_token=cancel_token
        )
        await self._enqueue_cmd(env)

    # ---- event handlers
    async def _record_worker_event(self, env: Envelope) -> None:
        evh = env.dedup_id
        await self.db.worker_events.update_one(
            {"task_id": env.task_id, "node_id": env.node_id, "event_hash": evh},
            {
                "$setOnInsert": {
                    "task_id": env.task_id,
                    "node_id": env.node_id,
                    "step_type": env.step_type,
                    "event_hash": evh,
                    "payload": env.payload,
                    "ts_ms": env.ts_ms,
                    "recv_ts_ms": self.clock.now_ms(),
                    "attempt_epoch": env.attempt_epoch,
                    "created_at": self.clock.now_dt(),
                    "metrics_applied": False,
                }
            },
            upsert=True,
        )
        # Touch last_event (best-effort)
        with swallow(logger=self.log, code="task.touch", msg="task touch failed", level=logging.WARNING):
            await self.db.tasks.update_one(
                {"id": env.task_id},
                {"$max": {"last_event_recv_ms": self.clock.now_ms()}, "$currentDate": {"updated_at": True}},
            )

    async def _on_task_accepted(self, env: Envelope) -> None:
        p = EvTaskAccepted.model_validate(env.payload)
        self.log.debug(
            "on.accept",
            event="coord.on.accept",
            node_id=env.node_id,
            epoch=env.attempt_epoch,
            worker_id=p.worker_id,
            lease_id=p.lease_id,
            deadline=p.lease_deadline_ts_ms,
        )
        await self.db.tasks.update_one(
            {"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {
                "$set": {
                    "graph.nodes.$.lease.worker_id": p.worker_id,
                    "graph.nodes.$.lease.lease_id": p.lease_id,
                    "graph.nodes.$.lease.deadline_ts_ms": p.lease_deadline_ts_ms,
                }
            },
        )

    async def _on_task_heartbeat(self, env: Envelope) -> None:
        p = EvHeartbeat.model_validate(env.payload)
        self.log.debug(
            "on.heartbeat",
            event="coord.on.heartbeat",
            node_id=env.node_id,
            epoch=env.attempt_epoch,
            worker_id=p.worker_id,
            lease_id=p.lease_id,
            deadline=p.lease_deadline_ts_ms,
        )
        await self.db.tasks.update_one(
            {"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {
                "$set": {
                    "graph.nodes.$.lease.worker_id": p.worker_id,
                    "graph.nodes.$.lease.lease_id": p.lease_id,
                },
                "$max": {
                    "graph.nodes.$.lease.deadline_ts_ms": int(p.lease_deadline_ts_ms or 0),
                    "graph.nodes.$.last_event_recv_ms": self.clock.now_ms(),
                },
            },
        )

    async def _maybe_start_children(
        self, parent_task: dict[str, Any], parent_node_id: str, *, trigger_kind: str
    ) -> None:
        """
        Start child nodes that are triggered by a specific parent event.

        Semantics:
        - trigger_kind == "on_batch": fire ASAP to minimize latency.
          * If the plan says "on_batch" for (parent→child) → start immediately (no readiness gate).
          * Otherwise, if child's io.start_when == "first_batch" → only start if _node_ready()
            (preserves historical behavior).
        - trigger_kind == "on_done": start as early as possible but *respect readiness*
          (fan-in, deferred backoff, etc.) via _node_ready().
        """
        task_id = parent_task["id"]
        if (parent_task.get("coordinator") or {}).get("cancelled") is True:
            return

        children = self._plan_children(parent_task, parent_node_id)
        if not children:
            return

        for child_id in children:
            child = self._get_node(parent_task, child_id)
            if not child:
                continue

            trig = self._plan_trigger_kind(parent_task, child_id, parent_node_id)  # may be None
            wants_first_batch = str((child.get("io", {}) or {}).get("start_when", "ready")).lower() == "first_batch"

            if trigger_kind == "on_batch":
                rule_async = trig == "on_batch"
                if not (rule_async or wants_first_batch):
                    continue

                # One-time flag to avoid multiple starts on many batches
                q = {
                    "id": task_id,
                    "graph.nodes": {
                        "$elemMatch": {
                            "node_id": child_id,
                            "status": {"$in": [RunState.queued, RunState.deferred]},
                            "streaming.started_on_first_batch": {"$ne": True},
                        }
                    },
                }
                res = await self.db.tasks.find_one_and_update(
                    q,
                    {
                        "$set": {
                            "graph.nodes.$.streaming.started_on_first_batch": True,
                            "graph.nodes.$.next_retry_at_ms": 0,
                        }
                    },
                )
                if not res:
                    continue

                fresh = await self.db.tasks.find_one({"id": task_id}, _TASK_PROJ_PLAN)
                ch_node = self._get_node(fresh, child_id) if fresh else None
                if not (fresh and ch_node):
                    continue

                # Preserve historical behavior:
                # - plan says on_batch → skip readiness check (low latency)
                # - only io.start_when=first_batch → still check readiness
                if rule_async:
                    await self._preflight_and_maybe_start(fresh, ch_node)
                else:
                    if await self._node_ready(fresh, ch_node):
                        await self._preflight_and_maybe_start(fresh, ch_node)

            elif trigger_kind == "on_done":
                if trig != "on_done":
                    continue
                # Fresh snapshot before checking readiness to reduce TOCTOU races
                fresh = await self.db.tasks.find_one({"id": task_id}, _TASK_PROJ_PLAN)
                ch_node = self._get_node(fresh, child_id) if fresh else None
                if not (fresh and ch_node):
                    continue
                if await self._node_ready(fresh, ch_node):
                    await self._preflight_and_maybe_start(fresh, ch_node)
            else:
                # unknown trigger kind — ignore gracefully
                self.log.debug(
                    "children.trigger.unknown",
                    event="coord.children.trigger_unknown",
                    parent_id=parent_node_id,
                    child_id=child_id,
                    trigger_kind=trigger_kind,
                    plan_trig=trig,
                )
                continue

    async def _on_batch_ok(self, env: Envelope) -> None:
        p = EvBatchOk.model_validate(env.payload)
        batch_uid = p.batch_uid

        if p.artifacts_ref:
            meta = dict(p.metrics or {})
            await self.db.artifacts.update_one(
                {"task_id": env.task_id, "node_id": env.node_id, "batch_uid": batch_uid},
                {
                    "$setOnInsert": {
                        "task_id": env.task_id,
                        "node_id": env.node_id,
                        "attempt_epoch": env.attempt_epoch,
                        "status": "partial",
                        "created_at": self.clock.now_dt(),
                    },
                    "$set": {
                        "status": "partial",
                        "meta": meta,
                        "artifacts_ref": p.artifacts_ref,
                        "updated_at": self.clock.now_dt(),
                    },
                },
                upsert=True,
            )

        if p.metrics:
            doc = {
                "task_id": env.task_id,
                "node_id": env.node_id,
                "batch_uid": batch_uid,
                "metrics": p.metrics,
                "attempt_epoch": env.attempt_epoch,
                "ts_ms": env.ts_ms,
                "created_at": self.clock.now_dt(),
            }
            with swallow(
                logger=self.log,
                code="metrics.insert",
                msg="metrics insert failed",
                level=logging.WARNING,
                expected=True,
            ):
                await self.db.metrics_raw.insert_one(doc)

        with swallow(
            logger=self.log, code="children.first_batch", msg="children start check failed", level=logging.WARNING
        ):
            tdoc = await self.db.tasks.find_one({"id": env.task_id}, _TASK_PROJ_PLAN)
            if tdoc:
                await self._maybe_start_children(tdoc, env.node_id, trigger_kind="on_batch")

    async def _on_batch_failed(self, env: Envelope) -> None:
        p = EvBatchFailed.model_validate(env.payload)
        with swallow(
            logger=self.log,
            code="metrics.insert_failed",
            msg="metrics insert for failed batch failed",
            level=logging.WARNING,
        ):
            await self.db.metrics_raw.insert_one(
                {
                    "task_id": env.task_id,
                    "node_id": env.node_id,
                    "batch_uid": p.batch_uid,
                    "metrics": {},
                    "failed": True,
                    "reason": p.reason_code,
                    "attempt_epoch": env.attempt_epoch,
                    "ts_ms": env.ts_ms,
                    "created_at": self.clock.now_dt(),
                }
            )
        with swallow(
            logger=self.log,
            code="artifact.update_failed",
            msg="artifact update for failed batch failed",
            level=logging.WARNING,
        ):
            await self.db.artifacts.update_one(
                {"task_id": env.task_id, "node_id": env.node_id, "batch_uid": p.batch_uid},
                {"$set": {"status": "failed", "error": p.reason_code, "updated_at": self.clock.now_dt()}},
                upsert=True,
            )

    async def _on_task_done(self, env: Envelope) -> None:
        p = EvTaskDone.model_validate(env.payload)
        if p.metrics:
            with swallow(
                logger=self.log, code="metrics.insert_final", msg="final metrics insert failed", level=logging.WARNING
            ):
                await self.db.metrics_raw.insert_one(
                    {
                        "task_id": env.task_id,
                        "node_id": env.node_id,
                        "batch_uid": p.final_uid or "__final__",
                        "metrics": p.metrics,
                        "final": True,
                        "attempt_epoch": env.attempt_epoch,
                        "ts_ms": env.ts_ms,
                        "created_at": self.clock.now_dt(),
                    }
                )
        if p.artifacts_ref:
            await self.db.artifacts.update_one(
                {
                    "task_id": env.task_id,
                    "node_id": env.node_id,
                    "batch_uid": {"$exists": False},
                },
                {
                    "$set": {"status": "complete", "meta": p.metrics or {}, "updated_at": self.clock.now_dt()},
                    "$setOnInsert": {
                        "task_id": env.task_id,
                        "node_id": env.node_id,
                        "attempt_epoch": env.attempt_epoch,
                        "created_at": self.clock.now_dt(),
                    },
                },
                upsert=True,
            )
        await self.db.tasks.update_one(
            {"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {"$set": {"graph.nodes.$.status": RunState.finished, "graph.nodes.$.finished_at": self.clock.now_dt()}},
        )

        # Quick path: start children with on_done trigger without waiting for the scheduler tick
        with swallow(
            logger=self.log, code="children.on_done", msg="children start on_done failed", level=logging.WARNING
        ):
            tdoc = await self.db.tasks.find_one({"id": env.task_id}, _TASK_PROJ_PLAN)
            if tdoc:
                await self._maybe_start_children(tdoc, env.node_id, trigger_kind="on_done")

    async def _on_task_failed(self, env: Envelope) -> None:
        p = EvTaskFailed.model_validate(env.payload)
        if p.permanent:
            await self._cascade_cancel(env.task_id, reason=f"hard_fail:{p.reason_code}")
            await self.db.tasks.update_one(
                {"id": env.task_id}, {"$set": {"status": RunState.failed, "finished_at": self.clock.now_dt()}}
            )
        else:
            td = await self.db.tasks.find_one({"id": env.task_id}, {"graph": 1})
            node = self._get_node(td, env.node_id) if td else None
            backoff = int(((node or {}).get("retry_policy") or {}).get("backoff_sec", 300))
            await self.db.tasks.update_one(
                {"id": env.task_id, "graph.nodes.node_id": env.node_id},
                {
                    "$set": {
                        "graph.nodes.$.status": RunState.deferred,
                        "graph.nodes.$.next_retry_at_ms": self.clock.now_ms() + backoff * 1000,
                        "graph.nodes.$.last_error": p.reason_code,
                    }
                },
            )

    async def _on_cancelled(self, env: Envelope) -> None:
        await self.db.tasks.update_one(
            {"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {"$set": {"graph.nodes.$.status": RunState.deferred}},
        )

    # ---- monitors
    async def _heartbeat_monitor(self) -> None:
        try:
            while True:
                cur = self.db.tasks.find(
                    {"status": {"$in": [RunState.running, RunState.deferred]}}, {"id": 1, "last_event_recv_ms": 1}
                )
                async for t in cur:
                    last = int(t.get("last_event_recv_ms") or 0)
                    dt_ms = max(0, self.clock.now_ms() - last)
                    if dt_ms >= self.cfg.hb_hard_ms:
                        await self.db.tasks.update_one(
                            {"id": t["id"]},
                            {
                                "$set": {
                                    "status": RunState.failed,
                                    "finished_at": self.clock.now_dt(),
                                    "coordinator.liveness.state": "dead",
                                }
                            },
                        )
                    elif dt_ms >= self.cfg.hb_soft_ms:
                        await self.db.tasks.update_one(
                            {"id": t["id"]},
                            {
                                "$set": {
                                    "status": RunState.deferred,
                                    "coordinator.liveness.state": "suspected",
                                    "coordinator.liveness.suspected_at": self.clock.now_dt(),
                                    "next_retry_at_ms": self.clock.now_ms() + 60_000,
                                }
                            },
                        )
                await self.clock.sleep_ms(self.cfg.hb_monitor_tick_ms)
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("hb.monitor.crashed", event="coord.hb.crash", exc_info=True)

    async def _finalizer_loop(self) -> None:
        try:
            while True:
                await self._finalize_nodes_and_tasks()
                await self.clock.sleep_ms(self.cfg.finalizer_tick_ms)
        except asyncio.CancelledError:
            return
        except Exception:
            self.log.error("finalizer.loop.crashed", event="coord.finalizer.crash", exc_info=True)

    async def _finalize_nodes_and_tasks(self) -> None:
        cur = self.db.tasks.find(
            {"status": {"$in": [RunState.running, RunState.deferred, RunState.cancelling]}},
            {"id": 1, "graph": 1, "coordinator": 1},
        )
        async for t in cur:
            nodes = t.get("graph", {}).get("nodes") or []
            if not nodes:
                continue
            rs = [self._to_runstate(n.get("status")) for n in nodes]
            all_finished = all(s == RunState.finished for s in rs)
            cancelled = (t.get("coordinator") or {}).get("cancelled") is True
            if all_finished or (
                cancelled and all(s in (RunState.finished, RunState.failed, RunState.deferred) for s in rs)
            ):
                result = {"nodes": [{"node_id": n["node_id"], "stats": n.get("stats", {})} for n in nodes]}
                await self.db.tasks.update_one(
                    {"id": t["id"]},
                    {"$set": {"status": RunState.finished, "finished_at": self.clock.now_dt(), "result": result}},
                )
                self.log.debug(
                    "task.finalized",
                    event="coord.task.finalized",
                    task_id=t["id"],
                    cancelled=bool(cancelled),
                    all_finished=bool(all_finished),
                )

    async def _cascade_cancel(self, task_id: str, *, reason: str) -> None:
        with swallow(
            logger=self.log, code="task.flag_cancel", msg="task cancel flag update failed", level=logging.WARNING
        ):
            await self.db.tasks.update_one(
                {"id": task_id},
                {
                    "$set": {
                        "coordinator.cancelled": True,
                        "coordinator.cancel_reason": reason,
                        "coordinator.cancelled_at": self.clock.now_dt(),
                    },
                    "$currentDate": {"updated_at": True},
                },
            )

        doc = await self.db.tasks.find_one({"id": task_id}, {"graph": 1})
        if not doc:
            return
        for n in doc.get("graph", {}).get("nodes") or []:
            if n.get("status") in [RunState.running, RunState.deferred, RunState.queued]:
                with swallow(
                    logger=self.log,
                    code="node.flag_cancel",
                    msg="node cancel flag update failed",
                    level=logging.WARNING,
                ):
                    await self.db.tasks.update_one(
                        {"id": task_id, "graph.nodes.node_id": n["node_id"]},
                        {
                            "$set": {
                                "graph.nodes.$.status": RunState.cancelling,
                                "graph.nodes.$.coordinator.cancelled": True,
                                "graph.nodes.$.cancel_requested_at": self.clock.now_dt(),
                            }
                        },
                    )

                lease = n.get("lease") or {}
                worker_id = lease.get("worker_id")
                if worker_id:
                    sig = {
                        "sig": "CANCEL",
                        "reason": reason,
                        "cancel_token": None,
                        "deadline_ts_ms": self.clock.now_ms() + self.cfg.cancel_grace_ms,
                    }
                    env = Envelope(
                        msg_type=MsgType.event,
                        role=Role.coordinator,
                        dedup_id=stable_hash(
                            {
                                "sig": "CANCEL",
                                "task_id": task_id,
                                "node": n["node_id"],
                                "epoch": int(n.get("attempt_epoch", 0)),
                                "worker": worker_id,
                            }
                        ),
                        task_id=task_id,
                        node_id=n["node_id"],
                        step_type=n["type"],
                        attempt_epoch=int(n.get("attempt_epoch", 0)),
                        ts_ms=self.clock.now_ms(),
                        payload=sig,
                        target_worker_id=worker_id,
                    )
                    await self._enqueue_signal(key_worker_id=worker_id, env=env)
        await self.clock.sleep_ms(self.cfg.cancel_grace_ms)

    async def _resume_inflight(self) -> None:
        """
        One-shot scan at startup for tasks that might need adoption/resume.
        Currently a no-op placeholder, but we keep a trace to make it explicit.
        """
        cnt = 0
        cur = self.db.tasks.find(
            {"status": {"$in": [RunState.running, RunState.deferred, RunState.queued]}},
            {"id": 1},
        )
        async for _ in cur:
            cnt += 1
        self.log.debug("resume.scan.done", event="coord.resume.scan", tasks_seen=cnt)

    async def cancel_task(self, task_id: str, *, reason: str = "user_request") -> bool:
        doc = await self.db.tasks.find_one({"id": task_id}, {"id": 1, "graph": 1, "status": 1})
        if not doc:
            return False
        with swallow(
            logger=self.log, code="task.flag_cancel", msg="task cancel flag update failed", level=logging.WARNING
        ):
            await self.db.tasks.update_one(
                {"id": task_id},
                {
                    "$set": {
                        "coordinator.cancelled": True,
                        "coordinator.cancel_reason": reason,
                        "coordinator.cancelled_at": self.clock.now_dt(),
                    }
                },
            )
        await self._cascade_cancel(task_id, reason=reason)
        return True

    async def _ensure_indexes(self) -> None:
        # Index creation often unsupported in test doubles; keep warnings but don't fail.
        with swallow(
            logger=self.log, code="idx.tasks", msg="create index tasks failed", level=logging.WARNING, expected=True
        ):
            await self.db.tasks.create_index([("status", 1), ("updated_at", 1)], name="ix_task_status_updated")
        with swallow(
            logger=self.log,
            code="idx.worker_registry",
            msg="create index worker_registry failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.worker_registry.create_index([("worker_id", 1)], unique=True, name="uniq_worker")
        with swallow(
            logger=self.log,
            code="idx.artifacts1",
            msg="create index artifacts (task,node) failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.artifacts.create_index([("task_id", 1), ("node_id", 1)], name="ix_artifacts_task_node")
        with swallow(
            logger=self.log,
            code="idx.artifacts2",
            msg="create index artifacts batch failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.artifacts.create_index(
                [("task_id", 1), ("node_id", 1), ("batch_uid", 1)], unique=True, sparse=True, name="uniq_artifact_batch"
            )
        with swallow(
            logger=self.log,
            code="idx.outbox.fp",
            msg="create index outbox fp failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.outbox.create_index([("fp", 1)], unique=True, name="uniq_outbox_fp")
        with swallow(
            logger=self.log,
            code="idx.outbox.state_next",
            msg="create index outbox state_next failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.outbox.create_index([("state", 1), ("next_attempt_at_ms", 1)], name="ix_outbox_state_next_ms")
        with swallow(
            logger=self.log,
            code="idx.metrics.batch",
            msg="create index metrics batch failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.metrics_raw.create_index(
                [("task_id", 1), ("node_id", 1), ("batch_uid", 1)], unique=True, name="uniq_metrics_batch"
            )
        with swallow(
            logger=self.log,
            code="idx.metrics.ttl",
            msg="create ttl index metrics_raw failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.metrics_raw.create_index(
                [("created_at", 1)], name="ttl_metrics_raw", expireAfterSeconds=14 * 24 * 3600
            )
        with swallow(
            logger=self.log,
            code="idx.worker_events",
            msg="create index worker_events failed",
            level=logging.WARNING,
            expected=True,
        ):
            await self.db.worker_events.create_index(
                [("task_id", 1), ("node_id", 1), ("event_hash", 1)], unique=True, name="uniq_worker_event"
            )
