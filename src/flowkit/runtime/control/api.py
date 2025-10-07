# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Operator Control API (FastAPI)
==============================

Endpoints:
  - POST /tasks                       -> create task from GraphSpec/params
  - POST /tasks/{task_id}/cancel      -> cascade cancel
  - GET  /tasks/{task_id}             -> inspect task (doc + nodes)
  - GET  /tasks?state=active|all      -> list tasks (currently active via TaskStore.iter_active)
  - GET  /healthz                     -> health check
  - GET  /metrics                     -> Prometheus text exposition (optional)

Design notes:
  - No hard dependency on prometheus_client: /metrics degrades gracefully.
  - Storage-agnostic: uses TaskStore protocol exposed via Coordinator.
  - Request validation is intentionally lightweight; deep GraphSpec validation is
    delegated to Coordinator.create_task() which compiles the spec.
"""

from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Response, status
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from ...core.logging import get_logger, log_context
from ...observability.metrics import get_registry as _get_prom_registry

# We only type-reference Coordinator/TaskStore; no import cycles on startup.
try:
    from ..coordinator.coordinator import Coordinator
except Exception:  # pragma: no cover
    Coordinator = object  # type: ignore[assignment]


# ------------------------------------------------------------------------------
# Pydantic schemas
# ------------------------------------------------------------------------------


class CreateTaskRequest(BaseModel):
    graph: dict[str, Any] = Field(..., description="GraphSpec JSON")
    params: dict[str, Any] | None = Field(default=None, description="Task-level params override")


class CreateTaskResponse(BaseModel):
    task_id: str


class CancelTaskRequest(BaseModel):
    reason: str | None = Field(default=None)


class TaskNodeView(BaseModel):
    node_id: str
    type: str
    status: str
    attempt_epoch: int
    lease: dict[str, Any] | None = None
    parents: list[str] = Field(default_factory=list)


class TaskView(BaseModel):
    task_id: str
    status: str
    params: dict[str, Any]
    meta: dict[str, Any]
    created_ms: int | None = None
    last_event_ms: int | None = None
    reason: str | None = None
    nodes: list[TaskNodeView] = Field(default_factory=list)


class ListTasksResponse(BaseModel):
    items: list[TaskView]
    state: str
    owner: str | None = None
    note: str | None = None


# ------------------------------------------------------------------------------
# App wiring
# ------------------------------------------------------------------------------

_log = get_logger("control.api")


@dataclass
class _Deps:
    coord: Coordinator


def _must_get_deps(app: FastAPI) -> _Deps:
    deps = getattr(app.state, "_flowkit_deps", None)
    if not isinstance(deps, _Deps):
        raise RuntimeError(
            "Control API is not initialized with Coordinator. "
            "Call create_app(coordinator) or set_coordinator(coordinator)."
        )
    return deps


def create_app(coordinator: Coordinator) -> FastAPI:
    app = FastAPI(title="FlowKit Control API", version="0.1")
    set_coordinator(coordinator, app=app)
    _install_routes(app)
    return app


# Optional global app for `uvicorn flowkit.runtime.control.api:app`
app = FastAPI(title="FlowKit Control API", version="0.1")


def set_coordinator(coordinator: Coordinator, *, app: FastAPI | None = None) -> None:
    target = app or globals().get("app")
    if not isinstance(target, FastAPI):
        raise RuntimeError("No FastAPI app available to bind Coordinator")
    target.state._flowkit_deps = _Deps(coord=coordinator)
    _install_routes(target)


def _install_routes(app: FastAPI) -> None:
    # Avoid double registration
    if getattr(app.state, "_flowkit_routes_installed", False):
        return
    _register_routes(app)
    app.state._flowkit_routes_installed = True
    _log.info("control api routes installed")


# ------------------------------------------------------------------------------
# Route handlers
# ------------------------------------------------------------------------------


def _register_routes(app: FastAPI) -> None:

    @app.get("/healthz", response_class=JSONResponse, tags=["meta"])
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics", response_class=PlainTextResponse, tags=["meta"])
    async def metrics() -> Response:
        """
        Prometheus exposition (text/plain). If prometheus_client is missing, return a stub.
        """
        try:
            # Lazy import to keep optional dependency
            import prometheus_client as prom  # type: ignore
        except Exception:
            body = "# metrics disabled: prometheus_client is not installed\n"
            return PlainTextResponse(status_code=200, content=body, media_type="text/plain; version=0.0.4")
        # Use default registry exposed by our metrics facade
        registry = _get_prom_registry()
        data = prom.generate_latest(registry)  # type: ignore[attr-defined]
        return Response(content=data, media_type=prom.CONTENT_TYPE_LATEST)  # type: ignore[attr-defined]

    @app.post("/tasks", response_model=CreateTaskResponse, status_code=status.HTTP_201_CREATED, tags=["tasks"])
    async def create_task(req: CreateTaskRequest) -> CreateTaskResponse:
        deps = _must_get_deps(app)
        with log_context(endpoint="POST /tasks"):
            try:
                tid = await deps.coord.create_task(graph_input=req.graph, params=req.params or {})
            except Exception as ex:
                _log.info("task.create.failed", exc_info=True)
                raise HTTPException(status_code=400, detail=f"invalid graph or params: {ex}") from ex
            _log.info("task.create.ok", task_id=tid)
            return CreateTaskResponse(task_id=tid)

    @app.post("/tasks/{task_id}/cancel", status_code=status.HTTP_202_ACCEPTED, tags=["tasks"])
    async def cancel_task(task_id: str, req: CancelTaskRequest | None = None) -> dict[str, Any]:
        deps = _must_get_deps(app)
        reason = (req.reason if req else None) or "user_request"
        with log_context(endpoint="POST /tasks/{id}/cancel", task_id=task_id, reason=reason):
            ok = await deps.coord.cancel_task(task_id, reason=reason)
            if not ok:
                raise HTTPException(status_code=404, detail="task not found")
            _log.info("task.cancel.requested", task_id=task_id, reason=reason)
            return {"ok": True, "task_id": task_id, "reason": reason}

    @app.get("/tasks/{task_id}", response_model=TaskView, tags=["tasks"])
    async def get_task(task_id: str) -> TaskView:
        deps = _must_get_deps(app)
        tdoc = await deps.coord.tasks.get(task_id)
        if not tdoc:
            raise HTTPException(status_code=404, detail="task not found")
        nodes = []
        async for n in deps.coord.tasks.iter_task_nodes(task_id):
            nodes.append(
                TaskNodeView(
                    node_id=n.node_id,
                    type=n.type,
                    status=str(n.status),
                    attempt_epoch=int(n.attempt_epoch or 0),
                    lease=(
                        {
                            "worker_id": n.lease.worker_id,
                            "lease_id": n.lease.lease_id,
                            "deadline_ms": n.lease.deadline_ms,
                        }
                        if n.lease
                        else None
                    ),
                    parents=list(n.parents or []),
                )
            )
        return TaskView(
            task_id=tdoc.task_id,
            status=str(tdoc.status),
            params=dict(tdoc.params or {}),
            meta=dict(tdoc.meta or {}),
            created_ms=int(tdoc.created_ms or 0),
            last_event_ms=int(tdoc.last_event_ms or 0),
            reason=(tdoc.meta or {}).get("reason"),
            nodes=nodes,
        )

    @app.get("/tasks", response_model=ListTasksResponse, tags=["tasks"])
    async def list_tasks(
        state: str = Query(default="active", description="active|all (currently both return active tasks)"),
        owner: str | None = Query(default=None, description="filter by TaskDoc.meta.owner if present"),
    ) -> ListTasksResponse:
        deps = _must_get_deps(app)
        items: list[TaskView] = []
        async for tdoc in deps.coord.tasks.iter_active():
            if owner:
                if (tdoc.meta or {}).get("owner") != owner:
                    continue
            # Summarize nodes
            nodes: list[TaskNodeView] = []
            async for n in deps.coord.tasks.iter_task_nodes(tdoc.task_id):
                nodes.append(
                    TaskNodeView(
                        node_id=n.node_id,
                        type=n.type,
                        status=str(n.status),
                        attempt_epoch=int(n.attempt_epoch or 0),
                        lease=(
                            {
                                "worker_id": n.lease.worker_id,
                                "lease_id": n.lease.lease_id,
                                "deadline_ms": n.lease.deadline_ms,
                            }
                            if n.lease
                            else None
                        ),
                        parents=list(n.parents or []),
                    )
                )
            items.append(
                TaskView(
                    task_id=tdoc.task_id,
                    status=str(tdoc.status),
                    params=dict(tdoc.params or {}),
                    meta=dict(tdoc.meta or {}),
                    created_ms=int(tdoc.created_ms or 0),
                    last_event_ms=int(tdoc.last_event_ms or 0),
                    reason=(tdoc.meta or {}).get("reason"),
                    nodes=nodes,
                )
            )
        note = None
        if state not in ("active", "all"):
            note = "Only active tasks are supported by this storage adapter; returning active tasks."
        elif state == "all":
            note = "Returning active tasks. Listing terminal tasks is not supported in this mode."
        return ListTasksResponse(items=items, state=state, owner=owner, note=note)
