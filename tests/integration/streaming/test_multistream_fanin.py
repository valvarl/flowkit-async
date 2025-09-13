"""Multistream fan-in to a single downstream consumer."""

from __future__ import annotations

import pytest
from tests.helpers.graph import make_graph, node_by_id, prime_graph, wait_node_running, wait_task_finished
from tests.helpers.handlers import build_analyzer_handler, build_indexer_handler

from flowkit.core.log import log_context

from ._helpers import _get_count, _make_indexer, _node_status

pytestmark = [pytest.mark.streaming, pytest.mark.worker_types("indexer,enricher,ocr,analyzer")]


@pytest.mark.asyncio
async def test_multistream_fanin_stream_to_one_downstream(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    """
    Multi-stream fan-in: three upstream indexers stream into one analyzer.
    Analyzer should start early on first batch and eventually see the full flow.
    """
    cd, _ = env_and_imports
    tlog.debug("test.start", event="test.start", test_name="multistream_fanin_stream_to_one_downstream")

    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    # Three sources with several batches each.
    u1 = _make_indexer("u1", total=9, batch=3)  # 3 batches
    u2 = _make_indexer("u2", total=8, batch=4)  # 2 batches
    u3 = _make_indexer("u3", total=12, batch=3)  # 4 batches

    d = {
        "node_id": "d",
        "type": "analyzer",
        "depends_on": ["u1", "u2", "u3"],
        "fan_in": "any",
        "io": {
            "start_when": "first_batch",
            "input_inline": {
                "input_adapter": "pull.from_artifacts",
                "input_args": {"from_nodes": ["u1", "u2", "u3"], "poll_ms": 25},
            },
        },
        "status": None,
        "attempt_epoch": 0,
    }

    graph = make_graph(nodes=[u1, u2, u3, d], edges=[("u1", "d"), ("u2", "d"), ("u3", "d")], agg={"after": "d"})
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tlog.debug(
        "task.created", event="task.created", test_name="multistream_fanin_stream_to_one_downstream", task_id=task_id
    )

    with log_context(task_id=task_id):
        doc_when_d_runs = await wait_node_running(inmemory_db, task_id, "d", timeout=8.0)
        st_u = {nid: str(_node_status(doc_when_d_runs, nid)) for nid in ("u1", "u2", "u3")}
        tlog.debug(
            "multistream.start_observed", event="multistream.start_observed", upstream=st_u, downstream="running"
        )

        # Not all upstreams should be finished at the analyzer start moment.
        assert sum(1 for s in st_u.values() if s.endswith("finished")) < 3

        # Final state must be finished for all nodes.
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)
        final = {n["node_id"]: str(n["status"]) for n in tdoc["graph"]["nodes"]}
        tlog.debug("multistream.final", event="multistream.final", statuses=final)

        assert node_by_id(tdoc, "d")["status"] == cd.RunState.finished
        assert node_by_id(tdoc, "u1")["status"] == cd.RunState.finished
        assert node_by_id(tdoc, "u2")["status"] == cd.RunState.finished
        assert node_by_id(tdoc, "u3")["status"] == cd.RunState.finished

        # Analyzer aggregated count should be at least the total items across sources.
        got = _get_count(tdoc, "d")
        tlog.debug("multistream.count", event="multistream.count", count=got)
        assert got >= (9 + 8 + 12)
