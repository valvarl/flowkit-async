from __future__ import annotations

import asyncio
import copy
import uuid
from typing import Any

from aiokafka import AIOKafkaConsumer

from ..bus.kafka import KafkaBus
from ..core.config import CoordinatorConfig
from ..core.time import Clock, SystemClock
from ..core.utils import stable_hash
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
    ) -> None:
        self.db = db
        self.cfg = copy.deepcopy(cfg) if cfg is not None else CoordinatorConfig.load()
        if worker_types:
            self.cfg.worker_types = list(worker_types)

        self.clock: Clock = clock or SystemClock()
        self.bus = KafkaBus(self.cfg)
        self.outbox = OutboxDispatcher(db=db, bus=self.bus, cfg=self.cfg, clock=self.clock)
        self.adapters = adapters or dict(default_adapters(db=db, clock=self.clock))

        self._tasks: set[asyncio.Task] = set()
        self._running = False

        self._announce_consumer: AIOKafkaConsumer | None = None
        self._status_consumers: dict[str, AIOKafkaConsumer] = {}
        self._query_reply_consumer: AIOKafkaConsumer | None = None

        self._gid = f"coord.{uuid.uuid4().hex[:6]}"

    # ---- lifecycle
    async def start(self) -> None:
        await self._ensure_indexes()
        await self.bus.start()
        await self._start_consumers()
        await self.outbox.start()
        self._running = True
        self._spawn(self._scheduler_loop())
        self._spawn(self._heartbeat_monitor())
        self._spawn(self._finalizer_loop())
        self._spawn(self._resume_inflight())

    async def stop(self) -> None:
        self._running = False
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()
        await self.outbox.stop()
        await self.bus.stop()

    def _spawn(self, coro):
        t = asyncio.create_task(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    # ---- consumers
    async def _start_consumers(self) -> None:
        self._announce_consumer = await self.bus.new_consumer(
            [self.cfg.topic_worker_announce], group_id=f"{self._gid}.announce", manual_commit=True
        )
        self._spawn(self._run_announce_consumer(self._announce_consumer))

        for t in self.cfg.worker_types:
            topic = self.bus.topic_status(t)
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
                env = Envelope.model_validate(msg.value)
                if env.msg_type == MsgType.reply and env.role == Role.worker:
                    self.bus.push_reply(env.corr_id, env)
                await c.commit()
        except asyncio.CancelledError:
            return

    async def _run_announce_consumer(self, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                env = Envelope.model_validate(msg.value)
                payload = env.payload
                _k = payload.get("kind")
                try:
                    kind = _k if isinstance(_k, EventKind) else EventKind(_k)
                except Exception:
                    kind = None
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
                    elif kind == EventKind.WORKER_OFFLINE:
                        await self.db.worker_registry.update_one(
                            {"worker_id": payload["worker_id"]},
                            {"$set": {"status": "offline", "last_seen": self.clock.now_dt()}},
                        )
                    else:
                        await self.db.worker_registry.update_one(
                            {"worker_id": payload.get("worker_id")},
                            {"$set": {"last_seen": self.clock.now_dt()}},
                            upsert=True,
                        )
                finally:
                    await c.commit()
        except asyncio.CancelledError:
            return

    async def _run_status_consumer(self, step_type: str, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                try:
                    env = Envelope.model_validate(msg.value)
                except Exception:
                    await c.commit()
                    continue

                try:
                    await self._record_worker_event(env)
                except Exception:
                    pass

                try:
                    tdoc = await self.db.tasks.find_one({"id": env.task_id}, {"graph": 1, "status": 1})
                    if not tdoc:
                        await c.commit()
                        continue
                    node = self._get_node(tdoc, env.node_id)
                    if not node or env.attempt_epoch != int(node.get("attempt_epoch", 0)):
                        await c.commit()
                        continue
                except Exception:
                    await c.commit()
                    continue

                try:
                    await self.db.tasks.update_one(
                        {"id": env.task_id},
                        {"$max": {"last_event_recv_ms": self.clock.now_ms()}, "$currentDate": {"updated_at": True}},
                    )
                except Exception:
                    pass

                _k = env.payload.get("kind")
                try:
                    kind = _k if isinstance(_k, EventKind) else EventKind(_k)
                except Exception:
                    kind = None
                try:
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
                    try:
                        await c.commit()
                    except Exception:
                        pass
        except asyncio.CancelledError:
            return

    # ---- DAG utils
    def _get_node(self, task_doc: dict[str, Any], node_id: str) -> dict[str, Any] | None:
        # mypy: make node list type explicit to avoid Any
        nodes: list[dict[str, Any]] = (task_doc.get("graph") or {}).get("nodes") or []
        for n in nodes:
            if n.get("node_id") == node_id:
                return n
        return None

    def _children_of(self, task_doc: dict[str, Any], node_id: str) -> list[str]:
        g = task_doc.get("graph", {})
        edges = g.get("edges") or []
        out = [dst for (src, dst) in edges if src == node_id]
        out.extend(ex.get("to") for ex in (g.get("edges_ex") or []) if ex.get("from") == node_id)
        return list(dict.fromkeys(out))

    def _edges_ex_from(self, task_doc: dict[str, Any], node_id: str) -> list[dict[str, Any]]:
        return [ex for ex in (task_doc.get("graph", {}).get("edges_ex") or []) if ex.get("from") == node_id]

    # ---- Public API
    async def create_task(self, *, params: dict[str, Any], graph: dict[str, Any]) -> str:
        task_id = str(uuid.uuid4())
        graph.setdefault("nodes", [])
        graph.setdefault("edges", [])
        graph.setdefault("edges_ex", [])
        doc = TaskDoc(
            id=task_id,
            pipeline_id=task_id,
            status=RunState.queued,
            params=params,
            graph=graph,
            status_history=[{"from": None, "to": RunState.queued, "at": self.clock.now_dt()}],
            started_at=self.clock.now_dt().isoformat(),
            last_event_recv_ms=self.clock.now_ms(),
        ).model_dump(mode="json")
        await self.db.tasks.insert_one(doc)
        return task_id

    # ---- Scheduler loop
    async def _scheduler_loop(self) -> None:
        try:
            while True:
                await self._schedule_ready_nodes()
                await self.clock.sleep_ms(self.cfg.scheduler_tick_ms)
        except asyncio.CancelledError:
            return

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
        status = self._to_runstate(node.get("status"))
        if status not in (RunState.queued, RunState.deferred):
            return False
        if status == RunState.deferred:
            nra = int(node.get("next_retry_at_ms") or 0)
            if nra and nra > self.clock.now_ms():
                return False

        io = node.get("io") or {}
        start_when = (io.get("start_when") or "").lower()
        if start_when == "first_batch":
            return await self._first_batch_available(task_doc, node)

        deps = node.get("depends_on") or []
        dep_states = [self._to_runstate((self._get_node(task_doc, d) or {}).get("status")) for d in deps]
        fan_in = (node.get("fan_in") or "all").lower()
        if fan_in == "all":
            return all(s == RunState.finished for s in dep_states)
        if fan_in == "any":
            return any(s == RunState.finished for s in dep_states)
        if fan_in.startswith("count:"):
            try:
                k = int(fan_in.split(":", 1)[1])
            except Exception:
                k = len(deps)
            return sum(1 for s in dep_states if s == RunState.finished) >= k
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
        except Exception as e:
            backoff = int((node.get("retry_policy") or {}).get("backoff_sec") or 300)
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
        topic = self.bus.topic_cmd(env.step_type)
        key = f"{env.task_id}:{env.node_id}"
        await self.outbox.enqueue(topic=topic, key=key, env=env)

    async def _enqueue_query(self, env: Envelope) -> None:
        key = env.task_id
        await self.outbox.enqueue(topic=self.cfg.topic_query, key=key, env=env)

    async def _enqueue_signal(self, *, key_worker_id: str, env: Envelope) -> None:
        await self.outbox.enqueue(topic=self.cfg.topic_signals, key=key_worker_id, env=env)

    async def _preflight_and_maybe_start(self, task_doc: dict[str, Any], node: dict[str, Any]) -> None:
        task_id = task_doc["id"]
        node_id = node["node_id"]
        new_epoch = int(node.get("attempt_epoch", 0)) + 1

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
                return
        except Exception:
            pass

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
            pass

        replies = self.bus.collect_replies(discover_env.corr_id)

        active = [
            r
            for r in replies
            if (
                r.payload.get("run_state") in ("running", "finishing")
                and r.payload.get("attempt_epoch") == node.get("attempt_epoch")
            )
        ]
        if active:
            await self.db.tasks.update_one(
                {"id": task_id, "graph.nodes.node_id": node_id}, {"$set": {"graph.nodes.$.status": RunState.running}}
            )
            return

        complete = any((r.payload.get("artifacts") or {}).get("complete") for r in replies)
        if complete:
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

        try:
            fresh = await self.db.tasks.find_one({"id": task_id}, {"graph": 1})
            fresh_node = self._get_node(fresh, node_id) if fresh else None
            last = int((fresh_node or {}).get("last_event_recv_ms") or 0)
            status = self._to_runstate((fresh_node or {}).get("status"))
            if status not in (RunState.deferred,):
                if max(0, self.clock.now_ms() - last) < self.cfg.discovery_window_ms:
                    return
        except Exception:
            pass

        # CAS: transition node to running only if it's still queued/deferred and attempt_epoch unchanged
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
            # someone else advanced it
            return

        cancel_token = str(uuid.uuid4())
        cmd = CmdTaskStart(
            cmd=CommandKind.TASK_START,
            input_ref=node.get("io", {}).get("input_ref"),
            input_inline=node.get("io", {}).get("input_inline"),
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
        await self._enqueue_cmd(env)

    # ---- event handlers
    async def _record_worker_event(self, env: Envelope) -> None:
        evh = env.dedup_id
        try:
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
        except Exception:
            pass
        await self.db.tasks.update_one(
            {"id": env.task_id},
            {"$max": {"last_event_recv_ms": self.clock.now_ms()}, "$currentDate": {"updated_at": True}},
        )

    async def _on_task_accepted(self, env: Envelope) -> None:
        p = EvTaskAccepted.model_validate(env.payload)
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
        await self.db.tasks.update_one(
            {"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {
                "$set": {
                    "graph.nodes.$.lease.worker_id": p.worker_id,
                    "graph.nodes.$.lease.lease_id": p.lease_id,
                    "graph.nodes.$.lease.deadline_ts_ms": p.lease_deadline_ts_ms,
                },
                "$max": {"graph.nodes.$.last_event_recv_ms": self.clock.now_ms()},
            },
        )

    async def _maybe_start_children_on_first_batch(self, parent_task: dict[str, Any], parent_node_id: str) -> None:
        task_id = parent_task["id"]
        if (parent_task.get("coordinator") or {}).get("cancelled") is True:
            return
        edges_ex = self._edges_ex_from(parent_task, parent_node_id)
        direct_children = self._children_of(parent_task, parent_node_id)
        if not direct_children:
            return

        for child_id in direct_children:
            child = self._get_node(parent_task, child_id)
            if not child:
                continue

            rule_async = any(
                ex
                for ex in edges_ex
                if ex.get("to") == child_id and ex.get("mode") == "async" and ex.get("trigger") == "on_batch"
            )
            sw = (child.get("io", {}) or {}).get("start_when", "ready")
            wants_first_batch = str(sw).lower() == "first_batch"

            if not (rule_async or wants_first_batch):
                continue

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
                {"$set": {"graph.nodes.$.streaming.started_on_first_batch": True, "graph.nodes.$.next_retry_at_ms": 0}},
            )
            if res:
                fresh = await self.db.tasks.find_one({"id": task_id}, {"id": 1, "graph": 1})
                ch_node = self._get_node(fresh, child_id) if fresh else None
                if fresh and ch_node:
                    if rule_async:
                        await self._preflight_and_maybe_start(fresh, ch_node)
                    else:
                        if await self._node_ready(fresh, ch_node):
                            await self._preflight_and_maybe_start(fresh, ch_node)

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
            try:
                await self.db.metrics_raw.insert_one(doc)
            except Exception:
                pass

        try:
            tdoc = await self.db.tasks.find_one({"id": env.task_id}, {"id": 1, "graph": 1})
            if tdoc:
                await self._maybe_start_children_on_first_batch(tdoc, env.node_id)
        except Exception:
            pass

    async def _on_batch_failed(self, env: Envelope) -> None:
        p = EvBatchFailed.model_validate(env.payload)
        try:
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
        except Exception:
            pass
        try:
            await self.db.artifacts.update_one(
                {"task_id": env.task_id, "node_id": env.node_id, "batch_uid": p.batch_uid},
                {"$set": {"status": "failed", "error": p.reason_code, "updated_at": self.clock.now_dt()}},
                upsert=True,
            )
        except Exception:
            pass

    async def _on_task_done(self, env: Envelope) -> None:
        p = EvTaskDone.model_validate(env.payload)
        if p.metrics:
            try:
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
            except Exception:
                pass
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

    async def _finalizer_loop(self) -> None:
        try:
            while True:
                await self._finalize_nodes_and_tasks()
                await self.clock.sleep_ms(self.cfg.finalizer_tick_ms)
        except asyncio.CancelledError:
            return

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

    async def _cascade_cancel(self, task_id: str, *, reason: str) -> None:
        try:
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
        except Exception:
            pass

        doc = await self.db.tasks.find_one({"id": task_id}, {"graph": 1})
        if not doc:
            return
        for n in doc.get("graph", {}).get("nodes") or []:
            if n.get("status") in [RunState.running, RunState.deferred, RunState.queued]:
                try:
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
                except Exception:
                    pass

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
        cur = self.db.tasks.find(
            {"status": {"$in": [RunState.running, RunState.deferred, RunState.queued]}},
            {"id": 1, "graph": 1, "status": 1},
        )
        async for _ in cur:
            pass

    async def cancel_task(self, task_id: str, *, reason: str = "user_request") -> bool:
        doc = await self.db.tasks.find_one({"id": task_id}, {"id": 1, "graph": 1, "status": 1})
        if not doc:
            return False
        try:
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
        except Exception:
            pass
        await self._cascade_cancel(task_id, reason=reason)
        return True

    async def _ensure_indexes(self) -> None:
        try:
            await self.db.tasks.create_index([("status", 1), ("updated_at", 1)], name="ix_task_status_updated")
            await self.db.worker_registry.create_index([("worker_id", 1)], unique=True, name="uniq_worker")
            await self.db.artifacts.create_index([("task_id", 1), ("node_id", 1)], name="ix_artifacts_task_node")
            await self.db.artifacts.create_index(
                [("task_id", 1), ("node_id", 1), ("batch_uid", 1)], unique=True, sparse=True, name="uniq_artifact_batch"
            )
            await self.db.outbox.create_index([("fp", 1)], unique=True, name="uniq_outbox_fp")
            await self.db.outbox.create_index([("state", 1), ("next_attempt_at_ms", 1)], name="ix_outbox_state_next_ms")
            await self.db.metrics_raw.create_index(
                [("task_id", 1), ("node_id", 1), ("batch_uid", 1)], unique=True, name="uniq_metrics_batch"
            )
            await self.db.metrics_raw.create_index(
                [("created_at", 1)], name="ttl_metrics_raw", expireAfterSeconds=14 * 24 * 3600
            )
            await self.db.worker_events.create_index(
                [("task_id", 1), ("node_id", 1), ("event_hash", 1)], unique=True, name="uniq_worker_event"
            )
        except Exception:
            pass
