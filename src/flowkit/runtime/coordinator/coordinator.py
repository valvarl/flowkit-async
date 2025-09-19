# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
FlowKit Coordinator (v2) — storage- and transport-agnostic.

Responsibilities:
  - Create tasks from declarative GraphSpec (compile → ExecutionPlan + runtime slice).
  - Evaluate start policies and schedule runnable nodes (Planner + VarsStore).
  - Enforce basic concurrency/fairness (in-memory Scheduler; leases done by TaskStore).
  - Drive reliable command dispatch via OutboxDispatcher (at-least-once).
  - React to worker events (accepted/heartbeat/batch_ok/batch_failed/done/failed/cancelled).
  - Trigger children (static parents and dynamic edges_ex).
  - Execute coordinator-side hooks (low cardinality).

Out of scope:
  - DB schemas and Kafka consumer wiring (done by the app using this class).
  - Worker protocol details beyond the Envelope payload types.

Design notes:
  - This file uses narrow protocols from `flowkit.storage.*` so any DB can be plugged in.
  - Transport is abstracted via `flowkit.transport.bus.Bus` for sending;
    consumption is delegated to the application which must call `on_event()` with Envelopes.
"""

import asyncio
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ...core.utils import stable_hash
from ...graph.compiler import (
    ExecutionPlan,
    prepare_for_task_create_v21,
)
from ...io.validation import normalize_adapter_args, validate_input_adapter
from ...protocol.messages import (
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
    Role,
    RunState,
)
from ...runtime.coordinator.hooks_runner import HooksRunner
from ...runtime.coordinator.metrics import CoordinatorMetrics
from ...runtime.coordinator.outbox import OutboxDispatcher
from ...runtime.coordinator.planner import (
    EvalContext,
    ExternalSnapshot,
    ParentSnapshot,
    Planner,
)
from ...runtime.coordinator.scheduler import QueueItem, Scheduler
from ...runtime.coordinator.vars_store import VarsStore
from ...storage.artifacts import ArtifactStore
from ...storage.kv import KVStore

# Storage interfaces (DB-agnostic)
from ...storage.tasks import NodeSnapshot, TaskDoc, TaskStore
from ...transport.bus import Bus

# Optional plugin registry for hooks (your app may bring its own registry)
try:
    from ...api.registry import PluginRegistry
except Exception:  # pragma: no cover
    PluginRegistry = object  # type: ignore[misc, assignment]


# ---- Coordinator options -----------------------------------------------------


@dataclass
class CoordinatorOpts:
    """
    Coordinator runtime options.

    Notes:
      - All timings are in milliseconds unless otherwise stated.
      - The Scheduler is timer-driven (pull model) to keep Bus completely optional.
    """

    scheduler_tick_ms: int = 200
    hb_soft_timeout_ms: int = 60_000
    hb_hard_timeout_ms: int = 5 * 60_000
    finalizer_tick_ms: int = 1_000
    # When discovering/resuming inflight work, skip re-dispatch for a short window.
    discovery_window_ms: int = 2_000


# ---- Coordinator -------------------------------------------------------------


class Coordinator:
    """
    Storage- and transport-agnostic coordinator.

    The application is expected to:
      1) Construct Coordinator with TaskStore/ArtifactStore/VarsStore/OutboxDispatcher/Bus.
      2) Call `start()` → coordinator starts internal scheduling/finalizer loops.
      3) Wire transport consumers and call `on_event(env)` for each worker event.

    Minimal lifecycle:
        coord = Coordinator(...)
        await coord.start()
        ...
        await coord.stop()
    """

    def __init__(
        self,
        *,
        tasks: TaskStore,
        artifacts: ArtifactStore,
        kv: KVStore,
        outbox: OutboxDispatcher,
        bus: Bus,
        vars_store: VarsStore | None = None,
        hooks: HooksRunner | None = None,
        metrics: CoordinatorMetrics | None = None,
        opts: CoordinatorOpts | None = None,
        registry: PluginRegistry | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.tasks = tasks
        self.artifacts = artifacts
        self.kv = kv
        self.outbox = outbox
        self.bus = bus

        self.vars = vars_store or VarsStore(backend=self._mk_vars_backend(kv))
        self.hooks = hooks or HooksRunner(registry=registry)  # registry may be None if not used
        self.metrics = metrics or CoordinatorMetrics.create()
        self.opts = opts or CoordinatorOpts()
        self.log = logger or logging.getLogger("flowkit.coordinator")

        # Runtime state
        self._running = False
        self._bg: set[asyncio.Task] = set()

        # In-memory scheduler (enforces fairness and per-key limits at dispatch time)
        self._sched = Scheduler()

    # ---- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        """Start scheduling/finalizer/heartbeat background loops."""
        if self._running:
            return
        self._running = True
        self._spawn(self._scheduler_loop(), name="scheduler")
        self._spawn(self._heartbeat_monitor(), name="hb-monitor")
        self._spawn(self._finalizer_loop(), name="finalizer")

    async def stop(self) -> None:
        """Stop background loops. OutboxDispatcher should be stopped by the app."""
        if not self._running:
            return
        self._running = False
        for t in list(self._bg):
            t.cancel()
        self._bg.clear()

    # ---- public API ----------------------------------------------------------

    async def create_task(self, *, graph_input: Mapping[str, Any], params: Mapping[str, Any] | None = None) -> str:
        """
        Create a new task: validate+compile the graph and persist TaskDoc via TaskStore.
        Returns the new task_id.
        """
        spec, plan, runtime = prepare_for_task_create_v21(dict(graph_input))
        task_id = str(uuid.uuid4())

        tdoc = TaskDoc(
            task_id=task_id,
            schema_version=spec.schema_version,
            params=dict(params or {}),
            plan=plan.model_dump(mode="json"),
            runtime=runtime,
            status=RunState.queued.value,
        )
        await self.tasks.create(tdoc)
        self.log.debug("task.created", extra={"task_id": task_id})
        return task_id

    async def cancel_task(self, task_id: str, *, reason: str = "user_request") -> bool:
        """
        Request a best-effort cancellation for the task.
        - Flags the task as 'cancelling' and marks queued nodes accordingly.
        - Sends cancel signals for running nodes with leases (via OutboxDispatcher).
        """
        doc = await self.tasks.get(task_id)
        if not doc:
            return False
        await self.tasks.flag_cancel(task_id=task_id, reason=reason)
        await self._cascade_cancel(doc, reason=reason)
        return True

    # ---- event ingestion (call from your transport consumer) -----------------

    async def on_event(self, env: Envelope) -> None:
        """
        Handle a single Envelope from a worker.

        Expected envelope kinds (EventKind):
            TASK_ACCEPTED, TASK_HEARTBEAT, TASK_RESUMED, BATCH_OK, BATCH_FAILED, TASK_DONE, TASK_FAILED, CANCELLED
        Other envelope kinds are ignored.
        """
        if env.msg_type != MsgType.event or env.role != Role.worker:
            return

        # Touch "last event seen" (best-effort)
        await self.tasks.touch_event(task_id=env.task_id)

        kind = _as_event_kind(env.payload.get("kind"))
        if not kind:
            return

        handlers = {
            EventKind.TASK_ACCEPTED: self._on_task_accepted,
            EventKind.TASK_HEARTBEAT: self._on_task_heartbeat,
            EventKind.TASK_RESUMED: self._on_task_heartbeat,
            EventKind.BATCH_OK: self._on_batch_ok,
            EventKind.BATCH_FAILED: self._on_batch_failed,
            EventKind.TASK_DONE: self._on_task_done,
            EventKind.TASK_FAILED: self._on_task_failed,
            EventKind.CANCELLED: self._on_cancelled,
        }
        h = handlers.get(kind)
        if h:
            await h(env)

    # ---- internal: scheduler -------------------------------------------------

    async def _scheduler_loop(self) -> None:
        try:
            while self._running:
                await self._schedule_tick()
                await asyncio.sleep(self.opts.scheduler_tick_ms / 1000.0)
        except asyncio.CancelledError:  # graceful stop
            return
        except Exception:  # pragma: no cover
            self.log.exception("scheduler.loop.crashed")

    async def _schedule_tick(self) -> None:
        """
        Pull active tasks from TaskStore, evaluate readiness with Planner, and enqueue start commands.

        TaskStore is responsible for persisting state transitions (queued → running, attempts, leases, etc.)
        while the in-memory Scheduler enforces cross-task fairness and per-key concurrency at emission time.
        """
        async for doc in self.tasks.iter_active():
            plan = ExecutionPlan.model_validate(doc.plan)
            planner = Planner(plan=plan)

            # Ensure task-level status bump (queued → running)
            if _runstate(doc.status) == RunState.queued:
                await self.tasks.bump_running(doc.task_id)

            # Load task-level vars once per doc
            vars_map = await self.vars.get_all(doc.task_id)

            # Evaluate all nodes that are not in a terminal state
            for node in await self.tasks.iter_task_nodes(doc.task_id):
                node_rs = _runstate(node.status)
                if node_rs not in (RunState.queued, RunState.deferred):
                    continue

                # Fast path: start_when == first_batch (checked by _node_ready)
                if not await self._node_ready(doc, node):
                    continue

                # Planner-based start policy: parents/externals/vars/params
                ctx = await self._build_eval_ctx(doc, node, planner)
                decision = planner.evaluate_start(node.node_id, ctx)
                if not decision.ready:
                    continue

                # Try to CAS → running (TaskStore controls epochs/leases)
                begin_ok, new_epoch, lease = await self.tasks.begin_node(
                    task_id=doc.task_id,
                    node_id=node.node_id,
                )
                if not begin_ok:
                    continue

                # Build IO plan (source/pull) and enqueue cmd
                io_plan = await self._build_io_plan(doc, node)
                await self._dispatch_start(
                    task=doc,
                    node=node,
                    epoch=new_epoch,
                    io_plan=io_plan,
                )

    # ---- internal: event handlers -------------------------------------------

    async def _on_task_accepted(self, env: Envelope) -> None:
        p = EvTaskAccepted.model_validate(env.payload)
        await self.tasks.update_lease(
            task_id=env.task_id,
            node_id=env.node_id,
            worker_id=p.worker_id,
            lease_id=p.lease_id,
            deadline_ts_ms=int(p.lease_deadline_ts_ms or 0),
        )

    async def _on_task_heartbeat(self, env: Envelope) -> None:
        p = EvHeartbeat.model_validate(env.payload)
        await self.tasks.update_lease(
            task_id=env.task_id,
            node_id=env.node_id,
            worker_id=p.worker_id,
            lease_id=p.lease_id,
            deadline_ts_ms=int(p.lease_deadline_ts_ms or 0),
        )

    async def _on_batch_ok(self, env: Envelope) -> None:
        p = EvBatchOk.model_validate(env.payload)
        # Persist partial artifact if present
        if p.artifacts_ref is not None:
            await self.artifacts.upsert_partial(
                task_id=env.task_id,
                node_id=env.node_id,
                batch_uid=p.batch_uid,
                meta=p.metrics or {},
                ref=p.artifacts_ref,
            )
        # Opportunistic fan-out for "first_batch" consumers and edges_ex on on_batch
        await self._trigger_children_on_event(env, trigger="on_batch")

    async def _on_batch_failed(self, env: Envelope) -> None:
        p = EvBatchFailed.model_validate(env.payload)
        await self.artifacts.mark_failed(
            task_id=env.task_id,
            node_id=env.node_id,
            batch_uid=p.batch_uid,
            reason=p.reason_code,
        )

    async def _on_task_done(self, env: Envelope) -> None:
        p = EvTaskDone.model_validate(env.payload)
        # Final artifact (may be absent — e.g., control nodes)
        if p.artifacts_ref is not None:
            await self.artifacts.upsert_final(
                task_id=env.task_id,
                node_id=env.node_id,
                meta=p.metrics or {},
                ref=p.artifacts_ref,
            )
        await self.tasks.finish_node(task_id=env.task_id, node_id=env.node_id)
        # Fan-out: children with on_done edges (static + dynamic)
        await self._trigger_children_on_event(env, trigger="on_done")

    async def _on_task_failed(self, env: Envelope) -> None:
        p = EvTaskFailed.model_validate(env.payload)
        if p.permanent:
            await self.tasks.fail_node(task_id=env.task_id, node_id=env.node_id, reason=p.reason_code)
            # Hard-fail task cascade
            doc = await self.tasks.get(env.task_id)
            if doc:
                await self.tasks.fail_task(task_id=env.task_id, reason=f"hard_fail:{p.reason_code}")
                await self._cascade_cancel(doc, reason=f"hard_fail:{p.reason_code}")
        else:
            # Soft failure → defer with backoff (TaskStore decides the next_retry_at_ms)
            await self.tasks.defer_node(task_id=env.task_id, node_id=env.node_id, reason=p.reason_code)

    async def _on_cancelled(self, env: Envelope) -> None:
        # Worker reported cancellation completed (coordinator initiated earlier)
        await self.tasks.defer_node(task_id=env.task_id, node_id=env.node_id, reason="cancelled")

    # ---- internal: helpers ---------------------------------------------------

    def _spawn(self, coro, *, name: str | None = None) -> None:
        t = asyncio.create_task(coro, name=name or getattr(coro, "__name__", "task"))
        self._bg.add(t)

        def _done(task: asyncio.Task) -> None:
            self._bg.discard(task)
            if task.cancelled():
                return
            if exc := task.exception():
                self.log.exception("coordinator.task.crashed", exc_info=exc)

        t.add_done_callback(_done)

    async def _build_eval_ctx(self, doc: TaskDoc, node: NodeSnapshot, planner: Planner) -> EvalContext:
        """Build expression environment for start/gate/edges evaluation."""
        # Parent snapshots
        parents = {}
        for pid in planner.plan.parents_by_child.get(node.node_id, []):
            ps = await self.tasks.get_node(doc.task_id, pid)
            parents[pid] = ParentSnapshot(
                done=_runstate(ps.status) == RunState.finished if ps else False,
                failed=_runstate(ps.status) == RunState.failed if ps else False,
                batch=False,  # streaming predicates may be plugged later
                exists=bool(ps),
            )
        # External snapshots (the resolver can be injected later; default all False)
        externals = {name: ExternalSnapshot(ready=False) for name in (planner.plan.graph_externals or {}).keys()}
        return EvalContext(
            params=dict(doc.params or {}),
            vars=await self.vars.get_all(doc.task_id),
            parents=parents,
            externals=externals,
        )

    async def _node_ready(self, doc: TaskDoc, node: NodeSnapshot) -> bool:
        """
        Additional readiness gate for "first_batch" sources.

        If the node declares 'start_when=first_batch' in its runtime IO, we wait
        until at least one partial/final artifact exists from any of its parents.
        """
        io = node.io or {}
        start_when = str((io.get("start") or {}).get("when") or "").lower()
        if start_when != "first_batch":
            return True
        parents = node.parents or []
        if not parents:
            return False
        return await self.artifacts.any_ready_from(task_id=doc.task_id, parent_ids=parents)

    async def _build_io_plan(self, doc: TaskDoc, node: NodeSnapshot) -> dict[str, Any] | None:
        """
        Produce `input_inline` for CmdTaskStart when applicable.

        Rules:
          - If node.io.input already present → validate adapter & args; return normalized.
          - Else if node has parents → set pull.from_artifacts(from_nodes=parents).
          - Else → None (source node; workers will prepare their own inputs).
        """
        io = node.io or {}
        inp = io.get("input") or {}
        if inp:
            # Expect normalized structure from ExecutionPlan → pass-through with minimal checks
            adapter = (inp.get("sources") or [{}])[0].get("adapter", {}).get("name") if inp.get("sources") else None
            args = (inp.get("sources") or [{}])[0].get("adapter", {}).get("args") if inp.get("sources") else {}
            if adapter:
                from_nodes, kwargs_norm = normalize_adapter_args(args or {})
                ok, why = validate_input_adapter(adapter, {**kwargs_norm, "from_nodes": from_nodes}, strict=False)
                if not ok:
                    raise ValueError(f"node {node.node_id}: {why}")
                return {"input_adapter": adapter, "input_args": {**kwargs_norm, "from_nodes": from_nodes}}
            # If sources[] missing adapter, return None — worker side may be fully declarative.
            return None

        parents = list(node.parents or [])
        if not parents:
            return None
        adapter = "pull.from_artifacts"
        from_nodes, kwargs_norm = normalize_adapter_args({"from_nodes": parents})
        ok, why = validate_input_adapter(adapter, {**kwargs_norm, "from_nodes": from_nodes}, strict=False)
        if not ok:
            raise ValueError(f"node {node.node_id}: {why}")
        return {"input_adapter": adapter, "input_args": {**kwargs_norm, "from_nodes": parents}}

    async def _dispatch_start(
        self,
        *,
        task: TaskDoc,
        node: NodeSnapshot,
        epoch: int,
        io_plan: dict[str, Any] | None,
    ) -> None:
        """Enqueue CmdTaskStart for the given node."""
        cmd = CmdTaskStart(
            cmd=CommandKind.TASK_START,
            input_inline=io_plan,
            cancel_token=str(uuid.uuid4()),
        )
        env = Envelope(
            msg_type=MsgType.cmd,
            role=Role.coordinator,
            dedup_id=stable_hash(
                {"cmd": str(CommandKind.TASK_START), "task_id": task.task_id, "node_id": node.node_id, "epoch": epoch}
            ),
            task_id=task.task_id,
            node_id=node.node_id,
            step_type=node.type,
            attempt_epoch=epoch,
            payload=cmd.model_dump(),
        )
        # Per-key fairness using Scheduler: priority/concurrency hints come from node policies in ExecutionPlan
        cq = node.concurrency or {}
        item = QueueItem(
            task_id=task.task_id,
            node_id=node.node_id,
            step_type=node.type,
            priority=int(cq.get("priority", 0) or 0),
            concurrency_key=cq.get("concurrency_key"),
            concurrency_limit=cq.get("limit"),
        )
        self._sched.offer(item)
        picked = self._sched.pop_ready(max_n=8)  # small batch to amortize CPU
        for it in picked:
            topic_key = f"{it.task_id}:{it.node_id}"
            await self.outbox.enqueue(topic=self._topic_cmd(it.step_type), key=topic_key, env=env)

        # Metrics
        self.metrics.scheduled_total.labels(step_type=node.type).inc()

    async def _trigger_children_on_event(self, env: Envelope, *, trigger: str) -> None:
        """
        Schedule children upon parent worker event (on_batch/on_done).

        - Static children: respect start policies (Planner).
        - Dynamic edges_ex: evaluate 'when' and only trigger enabled edges.
        """
        doc = await self.tasks.get(env.task_id)
        if not doc:
            return
        plan = ExecutionPlan.model_validate(doc.plan)
        planner = Planner(plan=plan)

        # Evaluate dynamic edges for this trigger
        # (coordinator evaluates, then children will pass their own readiness gates)
        if plan.dynamic_edges:
            ctx = await self._build_eval_ctx(doc, await self.tasks.get_node(env.task_id, env.node_id), planner)
            for e in planner.iter_enabled_edges(ctx):
                if e.get("from") == env.node_id and e.get("on") == f"on_{trigger.split('_')[-1]}":
                    ch = e.get("to")
                    if not ch:
                        continue
                    ch_node = await self.tasks.get_node(env.task_id, ch)
                    if ch_node and await self._node_ready(doc, ch_node):
                        begin_ok, new_epoch, _lease = await self.tasks.begin_node(task_id=env.task_id, node_id=ch)
                        if begin_ok:
                            io_plan = await self._build_io_plan(doc, ch_node)
                            await self._dispatch_start(task=doc, node=ch_node, epoch=new_epoch, io_plan=io_plan)

        # Static children (parents_by_child)
        for child_id in plan.children_by_parent.get(env.node_id, []) or []:
            ch_node = await self.tasks.get_node(env.task_id, child_id)
            if not ch_node:
                continue
            if await self._node_ready(doc, ch_node):
                ctx = await self._build_eval_ctx(doc, ch_node, planner)
                dec = planner.evaluate_start(child_id, ctx)
                if not dec.ready:
                    continue
                begin_ok, new_epoch, _lease = await self.tasks.begin_node(task_id=env.task_id, node_id=child_id)
                if not begin_ok:
                    continue
                io_plan = await self._build_io_plan(doc, ch_node)
                await self._dispatch_start(task=doc, node=ch_node, epoch=new_epoch, io_plan=io_plan)

    async def _cascade_cancel(self, doc: TaskDoc, *, reason: str) -> None:
        """
        Best-effort cancellation for all non-terminal nodes:
          - mark nodes 'cancelling' in TaskStore
          - send CANCEL signal to leased workers via Outbox
        """
        async for n in self.tasks.iter_task_nodes(doc.task_id):
            rs = _runstate(n.status)
            if rs in (RunState.finished, RunState.failed):
                continue
            await self.tasks.mark_cancelling(doc.task_id, n.node_id, reason=reason)
            if n.lease and n.lease.worker_id:
                payload = {
                    "sig": "CANCEL",
                    "reason": reason,
                    "cancel_token": None,
                    "deadline_ts_ms": 0,  # your worker may enforce a grace period
                }
                env = Envelope(
                    msg_type=MsgType.event,
                    role=Role.coordinator,
                    dedup_id=stable_hash(
                        {"sig": "CANCEL", "task_id": doc.task_id, "node": n.node_id, "epoch": int(n.attempt_epoch or 0)}
                    ),
                    task_id=doc.task_id,
                    node_id=n.node_id,
                    step_type=n.type,
                    attempt_epoch=int(n.attempt_epoch or 0),
                    payload=payload,
                    target_worker_id=n.lease.worker_id,
                )
                await self.outbox.enqueue(topic=self._topic_signals(), key=n.lease.worker_id, env=env)

    # ---- monitors ------------------------------------------------------------

    async def _heartbeat_monitor(self) -> None:
        try:
            while self._running:
                async for t in self.tasks.iter_active():
                    dt_ms = await self.tasks.ms_since_last_event(t.task_id)
                    if (
                        dt_ms >= self.opts.hard_timeout_ms
                        if hasattr(self.opts, "hard_timeout_ms")
                        else self.opts.hb_hard_timeout_ms
                    ):
                        await self.tasks.fail_task(task_id=t.task_id, reason="liveness:hard_timeout")
                    elif dt_ms >= self.opts.hb_soft_timeout_ms:
                        await self.tasks.defer_task(task_id=t.task_id, reason="liveness:soft_timeout")
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return
        except Exception:  # pragma: no cover
            self.log.exception("heartbeat.monitor.crashed")

    async def _finalizer_loop(self) -> None:
        try:
            while self._running:
                async for t in self.tasks.iter_active():
                    if await self.tasks.is_task_complete(t.task_id):
                        await self.tasks.finish_task(t.task_id)
                await asyncio.sleep(self.opts.finalizer_tick_ms / 1000.0)
        except asyncio.CancelledError:
            return
        except Exception:  # pragma: no cover
            self.log.exception("finalizer.loop.crashed")

    # ---- adapters / misc -----------------------------------------------------

    def _mk_vars_backend(self, kv: KVStore):
        """
        Minimal adapter turning KVStore into VarsBackend for VarsStore.
        The KVStore implementation should expose CAS semantics (version/etag).
        """

        class _Backend:
            def __init__(self, kvs: KVStore) -> None:
                self.kv = kvs
                self.ns = "task_vars"

            async def load(self, task_id: str) -> Mapping[str, Any]:
                v = await self.kv.get(self.ns, task_id)
                return v or {}

            async def cas_update(self, task_id: str, updater):
                cur, ver = await self.kv.get_with_version(self.ns, task_id)
                cur_map = dict(cur or {})
                new_map = updater(cur_map)
                return await self.kv.compare_and_set(self.ns, task_id, expected_version=ver, value=new_map)

        return _Backend(kv)

    # Topic helpers — the app can override these by subclassing if needed
    def _topic_cmd(self, step_type: str) -> str:
        return f"flowkit.cmd.{step_type}"

    def _topic_signals(self) -> str:
        return "flowkit.signals"

    # (status/reply/announce topics are not used here; ingestion is external)


# ---- small helpers -----------------------------------------------------------


def _runstate(v: Any) -> RunState | None:
    if isinstance(v, RunState):
        return v
    if isinstance(v, str):
        try:
            return RunState(v)
        except Exception:
            return None
    return None


def _as_event_kind(v: Any) -> EventKind | None:
    if isinstance(v, EventKind):
        return v
    if isinstance(v, str):
        try:
            return EventKind(v)
        except Exception:
            return None
    return None
