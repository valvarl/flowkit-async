# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
FlowKit Coordinator (MVP v2)

- Plans → schedules → dispatches start commands
- Reacts to worker events (accepted/heartbeat/batch/done/failed/cancelled)
- Triggers children on on_batch/on_done
- Uses common OutboxDispatcher and Observatory
"""

import asyncio
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ...core.utils import stable_hash
from ...graph.compiler import ExecutionPlan, prepare_for_task_create_v21
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
from ...storage.artifacts import ArtifactStore
from ...storage.kv import KVStore
from ...storage.tasks import NodeSnapshot, TaskDoc, TaskStore
from ...transport.bus import Bus
from ..common.observability import Observatory
from ..common.outbox import OutboxDispatcher
from .hooks_runner import HooksRunner
from .metrics import CoordinatorMetrics
from .planner import EvalContext, ExternalSnapshot, ParentSnapshot, Planner
from .scheduler import QueueItem, Scheduler
from .vars_store import VarsStore

# ---- Options ----------------------------------------------------------------


@dataclass
class CoordinatorOpts:
    scheduler_tick_ms: int = 200
    hb_soft_timeout_ms: int = 60_000
    hb_hard_timeout_ms: int = 5 * 60_000
    finalizer_tick_ms: int = 1_000
    discovery_window_ms: int = 2_000
    dispatch_tick_ms: int = 50
    dispatch_batch: int = 64


# ---- Coordinator -------------------------------------------------------------


class Coordinator:
    """
    The application should:
      1) Construct Coordinator with TaskStore/ArtifactStore/KV/Outbox/Bus/Observatory.
      2) await start()
      3) Wire transport consumers and call on_event(env) for worker events.
    """

    def __init__(
        self,
        *,
        tasks: TaskStore,
        artifacts: ArtifactStore,
        kv: KVStore,
        outbox: OutboxDispatcher,
        bus: Bus,
        observatory: Observatory | None = None,
        vars_store: VarsStore | None = None,
        hooks: HooksRunner | None = None,
        metrics: CoordinatorMetrics | None = None,
        opts: CoordinatorOpts | None = None,
    ) -> None:
        self.tasks = tasks
        self.artifacts = artifacts
        self.kv = kv
        self.outbox = outbox
        self.bus = bus

        self.obs = observatory or Observatory.create(service_name="coordinator")
        self.vars = vars_store or VarsStore(backend=self._mk_vars_backend(kv))
        self.hooks = hooks or HooksRunner(registry=None, observatory=self.obs)
        self.metrics = metrics or CoordinatorMetrics.create(self.obs.meter)
        self.opts = opts or CoordinatorOpts()

        self._running = False
        self._bg: set[asyncio.Task] = set()
        self._sched = Scheduler()

        # pending envs prepared during schedule; (task_id, node_id) -> Envelope
        self._pending: dict[tuple[str, str], Envelope] = {}

    # ---- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._spawn(self._scheduler_loop(), name="scheduler")
        self._spawn(self._dispatch_loop(), name="dispatch")
        self._spawn(self._heartbeat_monitor(), name="hb-monitor")
        self._spawn(self._finalizer_loop(), name="finalizer")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for t in list(self._bg):
            t.cancel()
        self._bg.clear()

    # ---- API -----------------------------------------------------------------

    async def create_task(self, *, graph_input: Mapping[str, Any], params: Mapping[str, Any] | None = None) -> str:
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
        with self.obs.scoped(task_id=task_id).log_context(event="task.created"):
            self.obs.logger.info("task.created")
        return task_id

    async def cancel_task(self, task_id: str, *, reason: str = "user_request") -> bool:
        doc = await self.tasks.get(task_id)
        if not doc:
            return False
        await self.tasks.flag_cancel(task_id=task_id, reason=reason)
        await self._cascade_cancel(doc, reason=reason)
        return True

    # ---- event ingestion -----------------------------------------------------

    async def on_event(self, env: Envelope) -> None:
        if env.msg_type != MsgType.event or env.role != Role.worker:
            return
        await self.tasks.touch_event(task_id=env.task_id)

        kind = _as_event_kind(env.payload.get("kind"))
        if not kind:
            return

        if kind is EventKind.TASK_ACCEPTED:
            await self._on_task_accepted(env)
        elif kind in (EventKind.TASK_HEARTBEAT, EventKind.TASK_RESUMED):
            await self._on_task_heartbeat(env)
        elif kind is EventKind.BATCH_OK:
            await self._on_batch_ok(env)
        elif kind is EventKind.BATCH_FAILED:
            await self._on_batch_failed(env)
        elif kind is EventKind.TASK_DONE:
            await self._on_task_done(env)
        elif kind is EventKind.TASK_FAILED:
            await self._on_task_failed(env)
        elif kind is EventKind.CANCELLED:
            await self._on_cancelled(env)

    # ---- scheduler & dispatch loops -----------------------------------------

    async def _scheduler_loop(self) -> None:
        try:
            while self._running:
                await self._schedule_tick()
                await asyncio.sleep(self.opts.scheduler_tick_ms / 1000.0)
        except asyncio.CancelledError:
            return
        except Exception:
            with self.obs.scoped().log_context(event="scheduler.loop.crashed"):
                self.obs.logger.exception("scheduler.loop.crashed")

    async def _dispatch_loop(self) -> None:
        tick_ms = self.opts.dispatch_tick_ms
        batch = self.opts.dispatch_batch
        try:
            while self._running:
                picked = self._sched.pop_ready(max_n=batch)
                if not picked:
                    await asyncio.sleep(tick_ms / 1000.0)
                    continue
                for it in picked:
                    key = (it.task_id, it.node_id)
                    env = self._pending.pop(key, None)
                    if not env:
                        # Nothing to send — stale queue item
                        self._sched.release(it.concurrency_key)
                        continue
                    topic_key = f"{it.task_id}:{it.node_id}"
                    await self.outbox.enqueue(topic=self._topic_cmd(it.step_type), key=topic_key, env=env)
                    self.metrics.dispatched_total.labels(step_type=it.step_type).inc()
        except asyncio.CancelledError:
            return
        except Exception:
            with self.obs.scoped().log_context(event="dispatch.loop.crashed"):
                self.obs.logger.exception("dispatch.loop.crashed")

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
                ctx = await self._build_eval_ctx(doc, node, planner, vars_map)
                decision = planner.evaluate_start(node.node_id, ctx)
                if not decision.ready:
                    continue

                # Try to CAS → running (TaskStore controls epochs/leases)
                begin_ok, new_epoch, _lease = await self.tasks.begin_node(
                    task_id=doc.task_id,
                    node_id=node.node_id,
                )
                if not begin_ok:
                    continue

                # Build IO plan (source/pull) and enqueue cmd
                io_plan = await self._build_io_plan(doc, node)
                await self._prepare_start(
                    task=doc,
                    node=node,
                    epoch=new_epoch,
                    io_plan=io_plan,
                )

    # ---- event handlers ------------------------------------------------------

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
            task_id=env.task_id, node_id=env.node_id, batch_uid=p.batch_uid, reason=p.reason_code
        )

    async def _on_task_done(self, env: Envelope) -> None:
        p = EvTaskDone.model_validate(env.payload)
        # Final artifact (may be absent — e.g., control nodes)
        if p.artifacts_ref is not None:
            await self.artifacts.upsert_final(
                task_id=env.task_id, node_id=env.node_id, meta=p.metrics or {}, ref=p.artifacts_ref
            )
        await self.tasks.finish_node(task_id=env.task_id, node_id=env.node_id)
        await self._release_concurrency(env)
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
        await self._release_concurrency(env)

    async def _on_cancelled(self, env: Envelope) -> None:
        # Worker reported cancellation completed (coordinator initiated earlier)
        await self.tasks.defer_node(task_id=env.task_id, node_id=env.node_id, reason="cancelled")
        await self._release_concurrency(env)

    # ---- helpers -------------------------------------------------------------

    def _spawn(self, coro, *, name: str | None = None) -> None:
        t = asyncio.create_task(coro, name=name or getattr(coro, "__name__", "task"))
        self._bg.add(t)

        def _done(task: asyncio.Task) -> None:
            self._bg.discard(task)
            if task.cancelled():
                return
            if exc := task.exception():
                self.obs.logger.exception("coordinator.task.crashed", exc_info=exc)

        t.add_done_callback(_done)

    async def _build_eval_ctx(
        self, doc: TaskDoc, node: NodeSnapshot, planner: Planner, vars_map: Mapping[str, Any]
    ) -> EvalContext:
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
            vars=dict(vars_map or {}),
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

    async def _prepare_start(
        self, *, task: TaskDoc, node: NodeSnapshot, epoch: int, io_plan: dict[str, Any] | None
    ) -> None:
        """Prepare CmdTaskStart and queue into the in-memory scheduler."""
        cmd = CmdTaskStart(cmd=CommandKind.TASK_START, input_inline=io_plan, cancel_token=str(uuid.uuid4()))
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
        cq = node.concurrency or {}
        item = QueueItem(
            task_id=task.task_id,
            node_id=node.node_id,
            step_type=node.type,
            priority=int(cq.get("priority", 0) or 0),
            concurrency_key=cq.get("concurrency_key"),
            concurrency_limit=cq.get("limit"),
        )
        self._pending[(task.task_id, node.node_id)] = env
        self._sched.offer(item)
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
            ctx = await self._build_eval_ctx(
                doc,
                await self.tasks.get_node(env.task_id, env.node_id),
                planner,
                await self.vars.get_all(env.task_id),
            )
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
                            await self._prepare_start(task=doc, node=ch_node, epoch=new_epoch, io_plan=io_plan)

        # Static children (parents_by_child)
        for child_id in plan.children_by_parent.get(env.node_id, []) or []:
            ch_node = await self.tasks.get_node(env.task_id, child_id)
            if not ch_node:
                continue
            if await self._node_ready(doc, ch_node):
                ctx = await self._build_eval_ctx(doc, ch_node, planner, await self.vars.get_all(env.task_id))
                dec = planner.evaluate_start(child_id, ctx)
                if not dec.ready:
                    continue
                begin_ok, new_epoch, _lease = await self.tasks.begin_node(task_id=env.task_id, node_id=child_id)
                if not begin_ok:
                    continue
                io_plan = await self._build_io_plan(doc, ch_node)
                await self._prepare_start(task=doc, node=ch_node, epoch=new_epoch, io_plan=io_plan)

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

    async def _release_concurrency(self, env: Envelope) -> None:
        # Release concurrency slot for the node's key (if any)
        node = await self.tasks.get_node(env.task_id, env.node_id)
        if not node:
            return
        cq = node.concurrency or {}
        self._sched.release(cq.get("concurrency_key"))

    # ---- monitors ------------------------------------------------------------

    async def _heartbeat_monitor(self) -> None:
        try:
            while self._running:
                async for t in self.tasks.iter_active():
                    dt_ms = await self.tasks.ms_since_last_event(t.task_id)
                    if dt_ms >= self.opts.hb_hard_timeout_ms:
                        await self.tasks.fail_task(task_id=t.task_id, reason="liveness:hard_timeout")
                    elif dt_ms >= self.opts.hb_soft_timeout_ms:
                        await self.tasks.defer_task(task_id=t.task_id, reason="liveness:soft_timeout")
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return
        except Exception:
            self.obs.logger.exception("heartbeat.monitor.crashed")

    async def _finalizer_loop(self) -> None:
        try:
            while self._running:
                async for t in self.tasks.iter_active():
                    if await self.tasks.is_task_complete(t.task_id):
                        await self.tasks.finish_task(t.task_id)
                await asyncio.sleep(self.opts.finalizer_tick_ms / 1000.0)
        except asyncio.CancelledError:
            return
        except Exception:
            self.obs.logger.exception("finalizer.loop.crashed")

    # ---- Topic helpers -------------------------------------------------------

    def _topic_cmd(self, step_type: str) -> str:
        return f"flowkit.cmd.{step_type}"

    def _topic_signals(self) -> str:
        return "flowkit.signals"

    # ---- Vars backend adapter ------------------------------------------------

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


# ---- helpers ----------------------------------------------------------------


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
