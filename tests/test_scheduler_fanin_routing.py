# tests/test_fanin_and_merge.py
"""
Fan-in behavior (ANY/ALL/COUNT:N) and coordinator_fn merge smoke tests.

Covers:
  - ANY fan-in: downstream starts as soon as any parent streams a first batch.
  - ALL fan-in: downstream starts only after all parents are done (no start_when).
  - COUNT:N fan-in: xfail placeholder until the coordinator supports it.
  - Edges vs routing priority: xfail placeholder for precedence logic.
  - coordinator_fn merge: runs without a worker and feeds downstream via artifacts.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from flowkit.core.log import log_context
from tests.helpers import BROKER
from tests.helpers.graph import node_by_id, prime_graph, wait_node_running, wait_task_finished
from tests.helpers.handlers import build_analyzer_handler, build_indexer_handler

# Only the roles required by this module
pytestmark = pytest.mark.worker_types("indexer,analyzer")


# ───────────────────────── Fixtures ─────────────────────────


@pytest_asyncio.fixture
async def workers_indexer_analyzer(env_and_imports, inmemory_db, worker_factory, tlog):
    """
    Minimal worker set for fan-in scenarios:
      - one 'indexer' worker (processes all upstream indexer nodes sequentially)
      - one 'analyzer' worker (downstream consumer)
    Handlers come from conftest.handlers (already DB-injected).
    """
    tlog.debug("fixture.start", event="fixture.start", fixture="workers_indexer_analyzer")
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )
    tlog.debug("fixture.ready", event="fixture.ready", fixture="workers_indexer_analyzer")
    # worker_factory will auto-stop workers on teardown
    yield
    tlog.debug("fixture.teardown", event="fixture.teardown", fixture="workers_indexer_analyzer")


# ───────────────────────── Helpers (local builders only) ─────────────────────────


def _make_indexer_node(node_id: str, total: int, batch: int):
    """Synthetic upstream indexer node with inline input and no deps."""
    return {
        "node_id": node_id,
        "type": "indexer",
        "depends_on": [],
        "fan_in": "all",
        "io": {"input_inline": {"batch_size": batch, "total_skus": total}},
        "status": None,
        "attempt_epoch": 0,
    }


# ───────────────────────── Tests ─────────────────────────


@pytest.mark.asyncio
async def test_fanin_any_starts_early(env_and_imports, inmemory_db, coord, workers_indexer_analyzer, tlog):
    """
    Fan-in ANY: downstream should start as soon as at least one parent streams
    (start_when='first_batch'), even if other parents are not yet finished.
    We run a single 'indexer' worker so upstream parents execute sequentially.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="fanin_any_starts_early")

    u1 = _make_indexer_node("u1", total=4, batch=4)
    u2 = _make_indexer_node("u2", total=8, batch=4)
    u3 = _make_indexer_node("u3", total=12, batch=6)

    d_any = {
        "node_id": "d_any",
        "type": "analyzer",
        "depends_on": ["u1", "u2", "u3"],
        "fan_in": "any",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u1", "u2", "u3"], "poll_ms": 40},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {
        "schema_version": "1.0",
        "nodes": [u1, u2, u3, d_any],
        "edges": [["u1", "d_any"], ["u2", "d_any"], ["u3", "d_any"]],
    }
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("task.created", event="task.created", test_name="fanin_any_starts_early", task_id=task_id)

    with log_context(task_id=task_id):
        # Snapshot when d_any entered running.
        doc_at_start = await wait_node_running(inmemory_db, task_id, "d_any", timeout=8.0)
        s1 = str(next(n for n in doc_at_start["graph"]["nodes"] if n["node_id"] == "u1")["status"])
        s2 = str(next(n for n in doc_at_start["graph"]["nodes"] if n["node_id"] == "u2")["status"])
        s3 = str(next(n for n in doc_at_start["graph"]["nodes"] if n["node_id"] == "u3")["status"])
        tlog.debug("fanin.any.start_observed", event="fanin.any.start_observed", u1=s1, u2=s2, u3=s3)

        # Not all parents should be finished at that moment.
        finished_flags = [x.endswith("finished") for x in (s1, s2, s3)]
        assert sum(1 for x in finished_flags if x) < 3, "ANY should not wait for all parents"

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        st = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
        tlog.debug("fanin.any.final.status", event="fanin.any.final.status", statuses=st)
        assert st["d_any"] == str(cd.RunState.finished)


@pytest.mark.asyncio
async def test_fanin_all_waits_all_parents(env_and_imports, inmemory_db, coord, workers_indexer_analyzer, tlog):
    """
    Fan-in ALL: without start_when hint, downstream should only start after
    both parents are finished.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="fanin_all_waits_all_parents")

    a = _make_indexer_node("a", total=6, batch=3)
    b = _make_indexer_node("b", total=10, batch=5)

    d_all = {
        "node_id": "d_all",
        "type": "analyzer",
        "depends_on": ["a", "b"],
        "fan_in": "all",
        "io": {
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["a", "b"], "poll_ms": 40},
            }
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {"schema_version": "1.0", "nodes": [a, b, d_all], "edges": [["a", "d_all"], ["b", "d_all"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("task.created", event="task.created", test_name="fanin_all_waits_all_parents", task_id=task_id)

    with log_context(task_id=task_id):
        doc_at_start = await wait_node_running(inmemory_db, task_id, "d_all", timeout=8.0)
        sa = str(next(n for n in doc_at_start["graph"]["nodes"] if n["node_id"] == "a")["status"])
        sb = str(next(n for n in doc_at_start["graph"]["nodes"] if n["node_id"] == "b")["status"])
        tlog.debug("fanin.all.start_observed", event="fanin.all.start_observed", a=sa, b=sb)

        # By the time d_all starts, both parents must have finished.
        assert sa.endswith("finished") and sb.endswith("finished")

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        st = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
        tlog.debug("fanin.all.final.status", event="fanin.all.final.status", statuses=st)
        assert st["d_all"] == str(cd.RunState.finished)


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Fan-in 'count:n' is not implemented yet", strict=False)
async def test_fanin_count_n(env_and_imports, inmemory_db, coord, workers_indexer_analyzer, tlog):
    """
    Fan-in COUNT:N placeholder: downstream should start when at least N parents are ready.
    Marked xfail until coordinator supports 'count:n'.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="fanin_count_n")

    p1 = _make_indexer_node("p1", total=4, batch=4)
    p2 = _make_indexer_node("p2", total=6, batch=3)
    p3 = _make_indexer_node("p3", total=8, batch=4)

    d_cnt = {
        "node_id": "d_cnt",
        "type": "analyzer",
        "depends_on": ["p1", "p2", "p3"],
        "fan_in": "count:2",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["p1", "p2", "p3"], "poll_ms": 40},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {
        "schema_version": "1.0",
        "nodes": [p1, p2, p3, d_cnt],
        "edges": [["p1", "d_cnt"], ["p2", "d_cnt"], ["p3", "d_cnt"]],
    }
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("task.created", event="task.created", test_name="fanin_count_n", task_id=task_id)

    with log_context(task_id=task_id):
        doc_at_start = await wait_node_running(inmemory_db, task_id, "d_cnt", timeout=8.0)
        s1 = str(next(n for n in doc_at_start["graph"]["nodes"] if n["node_id"] == "p1")["status"])
        s2 = str(next(n for n in doc_at_start["graph"]["nodes"] if n["node_id"] == "p2")["status"])
        s3 = str(next(n for n in doc_at_start["graph"]["nodes"] if n["node_id"] == "p3")["status"])
        ready = [x.endswith("finished") for x in (s1, s2, s3)]
        assert sum(1 for x in ready if x) >= 2

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        assert str(next(n for n in tdoc["graph"]["nodes"] if n["node_id"] == "d_cnt")["status"]) == str(
            cd.RunState.finished
        )


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Edges vs routing priority not implemented/covered yet", strict=False)
async def test_edges_vs_routing_priority(env_and_imports, inmemory_db, coord, workers_indexer_analyzer, tlog):
    """
    If explicit graph edges are present and a node also has routing.on_success,
    edges should take precedence (routing target should not run).
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="edges_vs_routing_priority")

    src = _make_indexer_node("src", total=3, batch=3)

    only_edges = {
        "node_id": "only_edges",
        "type": "analyzer",
        "depends_on": ["src"],
        "fan_in": "all",
        "io": {
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["src"], "poll_ms": 40},
            }
        },
        "status": None,
        "attempt_epoch": 0,
    }
    should_not_run = {
        "node_id": "should_not_run",
        "type": "analyzer",
        "depends_on": ["src"],
        "fan_in": "all",
        "io": {
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["src"], "poll_ms": 40},
            }
        },
        "status": None,
        "attempt_epoch": 0,
    }

    src["routing"] = {"on_success": ["should_not_run"]}  # should be ignored due to explicit edges

    graph = {
        "schema_version": "1.0",
        "nodes": [src, only_edges, should_not_run],
        "edges": [["src", "only_edges"]],
    }
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("task.created", event="task.created", test_name="edges_vs_routing_priority", task_id=task_id)

    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        st = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
        tlog.debug("edges_routes.final.status", event="edges_routes.final.status", statuses=st)

        assert st["only_edges"] == str(cd.RunState.finished)
        assert st["should_not_run"] in (
            None,
            str(cd.RunState.queued),
        ), "routing.on_success must not trigger when explicit edges exist"


@pytest.mark.asyncio
async def test_coordinator_fn_merge_without_worker(env_and_imports, inmemory_db, coord, workers_indexer_analyzer, tlog):
    """
    coordinator_fn node should run without a worker and produce artifacts that
    a downstream analyzer can consume via pull.from_artifacts.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="coordinator_fn_merge_without_worker")

    u1 = _make_indexer_node("u1", total=5, batch=5)
    u2 = _make_indexer_node("u2", total=7, batch=7)

    merge = {
        "node_id": "merge",
        "type": "coordinator_fn",
        "depends_on": ["u1", "u2"],
        "fan_in": "all",
        "io": {"fn": "merge.generic", "fn_args": {"from_nodes": ["u1", "u2"], "target": {"key": "merged-k"}}},
        "status": None,
        "attempt_epoch": 0,
    }

    sink = {
        "node_id": "sink",
        "type": "analyzer",
        "depends_on": ["merge"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["merge"], "poll_ms": 40},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {
        "schema_version": "1.0",
        "nodes": [u1, u2, merge, sink],
        "edges": [["u1", "merge"], ["u2", "merge"], ["merge", "sink"]],
    }
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug("task.created", event="task.created", test_name="coordinator_fn_merge_without_worker", task_id=task_id)

    with log_context(task_id=task_id):
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        st = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
        tlog.debug("merge.final.status", event="merge.final.status", statuses=st)
        assert st["merge"] == str(cd.RunState.finished)
        assert st["sink"] == str(cd.RunState.finished)

        # Merge artifacts must exist and be complete.
        cnt = await inmemory_db.artifacts.count_documents(
            {"task_id": task_id, "node_id": "merge", "status": "complete"}
        )
        tlog.debug("merge.artifacts.count", event="merge.artifacts.count", count=int(cnt))
        assert cnt >= 1


@pytest.mark.asyncio
@pytest.mark.xfail(reason="routing.on_failure is not implemented yet", strict=False)
async def test_routing_on_failure_triggers_remediator_only(
    env_and_imports, inmemory_db, coord, workers_indexer_analyzer, tlog
):
    """
    On upstream TASK_FAILED(permanent=True), only the 'on_failure' remediator should run; 'on_success' must not.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="routing_on_failure_triggers_remediator_only")

    # Upstream that we will fail manually via a forged status event.
    u = _make_indexer_node("u", total=5, batch=5)
    u["routing"] = {"on_success": ["succ"], "on_failure": ["remed"]}

    succ = {
        "node_id": "succ",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u"], "poll_ms": 40},
            }
        },
        "status": None,
        "attempt_epoch": 0,
    }
    remed = {
        "node_id": "remed",
        "type": "analyzer",
        "depends_on": [],  # should be triggered by routing.on_failure (no explicit edge)
        "fan_in": "all",
        "io": {"input_inline": {"input_adapter": "noop", "input_args": {}}},
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {
        "schema_version": "1.0",
        "nodes": [u, succ, remed],
        "edges": [["u", "succ"]],  # only on-success path is wired explicitly
    }
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)

    # Forge a permanent failure from the upstream without starting it for real.
    env = {
        "msg_type": "event",
        "role": "worker",
        "dedup_id": f"test:{task_id}:u:0:fail",
        "task_id": task_id,
        "node_id": "u",
        "step_type": "indexer",
        "attempt_epoch": 0,  # matches current attempt_epoch (not started)
        "ts_ms": 0,
        "payload": {"kind": "TASK_FAILED", "permanent": True, "reason_code": "test_fail"},
    }
    await BROKER.produce("status.indexer.v1", env)

    # Give the coordinator some time to process the failure.
    await cd.asyncio.sleep(0.2)  # uses patched fast tick settings via fixtures

    doc = await inmemory_db.tasks.find_one({"id": task_id}, {"graph": 1})
    s_succ = (node_by_id(doc, "succ") or {}).get("status")
    s_rem = (node_by_id(doc, "remed") or {}).get("status")

    # Expected (when implemented): remediator finished, succ did not run.
    assert str(s_rem).endswith("finished")
    assert not str(s_succ).endswith("running") and not str(s_succ).endswith("finished")


@pytest.mark.asyncio
async def test_fanout_one_upstream_two_downstreams_mixed_start_when(
    env_and_imports, inmemory_db, coord, workers_indexer_analyzer, monkeypatch, tlog
):
    """
    One upstream → two downstreams: A has start_when=first_batch (starts early),
    B has no start_when (waits for completion).
    """
    cd, _ = env_and_imports
    monkeypatch.setenv("TEST_IDX_PROCESS_SLEEP_SEC", "0.12")
    tlog.debug("test.start", event="test.start", test_name="test_fanout_one_upstream_two_downstreams_mixed_start_when")

    u = _make_indexer_node("u", total=12, batch=4)  # 3 batches
    a_fast = {
        "node_id": "a_fast",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u"], "poll_ms": 30},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }
    b_wait = {
        "node_id": "b_wait",
        "type": "analyzer",
        "depends_on": ["u"],
        "fan_in": "all",
        "io": {
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u"], "poll_ms": 30},
            }
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = {
        "schema_version": "1.0",
        "nodes": [u, a_fast, b_wait],
        "edges": [["u", "a_fast"], ["u", "b_wait"]],
    }
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)

    # A should start while U is still running.
    doc_when_a_runs = await wait_node_running(inmemory_db, task_id, "a_fast", timeout=8.0)
    st_u = (node_by_id(doc_when_a_runs, "u") or {}).get("status")
    st_b = (node_by_id(doc_when_a_runs, "b_wait") or {}).get("status")
    assert not str(st_u).endswith("finished")
    assert not str(st_b).endswith("running")

    # Eventually both must finish.
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    tlog.debug("fanout.mixed.final", event="fanout.mixed.final", statuses=final)
    assert final["u"] == cd.RunState.finished
    assert final["a_fast"] == cd.RunState.finished
    assert final["b_wait"] == cd.RunState.finished
