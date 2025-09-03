from __future__ import annotations

import math

import pytest

from tests.helpers.graph import prime_graph, wait_task_finished, node_by_id
from tests.helpers.handlers import build_indexer_handler, build_analyzer_handler

# нам нужны только эти роли
pytestmark = pytest.mark.worker_types("indexer,analyzer")


# ───────────────────────── Graph builders ─────────────────────────

def graph_partial_and_collect(*, total: int, batch_size: int, rechunk: int) -> dict:
    """
    w1=indexer -> w2=analyzer (читает через pull.from_artifacts.rechunk:size).
    Analyzer считает элементы; проверять будем по node.stats.
    """
    return {
        "schema_version": "1.0",
        "nodes": [
            {
                "node_id": "w1",
                "type": "indexer",
                "depends_on": [],
                "fan_in": "all",
                "io": {"input_inline": {"batch_size": batch_size, "total_skus": total}},
            },
            {
                "node_id": "w2",
                "type": "analyzer",
                "depends_on": ["w1"],
                "fan_in": "any",
                "io": {
                    "start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts.rechunk:size",
                        # indexer пишет список под ключом "skus"
                        "input_args": {"from_nodes": ["w1"], "size": rechunk, "poll_ms": 25, "meta_list_key": "skus"},
                    },
                },
            },
        ],
        "edges": [["w1", "w2"]],
        "edges_ex": [{"from": "w1", "to": "w2", "mode": "async", "trigger": "on_batch"}],
    }


def graph_merge_generic() -> dict:
    """
    Два indexer-узла -> coordinator_fn:merge.generic -> (без воркера).
    """
    return {
        "schema_version": "1.0",
        "nodes": [
            {"node_id": "a", "type": "indexer", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}},
            {"node_id": "b", "type": "indexer", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}},
            {
                "node_id": "m",
                "type": "coordinator_fn",
                "depends_on": ["a", "b"],
                "fan_in": "all",
                "io": {"fn": "merge.generic", "fn_args": {"from_nodes": ["a", "b"], "target": {"key": "merged"}}},
            },
        ],
        "edges": [["a", "m"], ["b", "m"]],
    }


# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_partial_shards_and_stream_counts(env_and_imports, inmemory_db, worker_factory, coord_cfg, worker_cfg):
    """
    Источник w1 (indexer) отдаёт батчи с batch_uid → воркер создаёт partial-артефакты,
    завершение помечает complete. Analyzer читает через rechunk и накапливает счётчик.
    Проверяем:
      * число partial-шардов у w1 == ceil(total/batch_size)
      * есть complete у w1
      * у w2 в node.stats.count == total
    """
    cd, _ = env_and_imports

    # поднимем воркеров
    await worker_factory(
        ("indexer", build_indexer_handler(db=inmemory_db)),
        ("analyzer", build_analyzer_handler(db=inmemory_db)),
    )

    # параметры
    total, bs, rechunk = 11, 4, 3
    graph = prime_graph(cd, graph_partial_and_collect(total=total, batch_size=bs, rechunk=rechunk))

    coord = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
    await coord.start()
    try:
        task_id = await coord.create_task(params={}, graph=graph)
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

        # partial-шарды для w1: ожидаем ceil(11/4)=3
        shards = 0
        cur = inmemory_db.artifacts.find({"task_id": task_id, "node_id": "w1", "status": "partial"})
        async for _ in cur:
            shards += 1
        assert shards == math.ceil(total / bs), f"expected {math.ceil(total/bs)} partial shards, got {shards}"

        # complete у w1
        a_w1 = await inmemory_db.artifacts.find_one({"task_id": task_id, "node_id": "w1", "status": "complete"})
        assert a_w1 is not None, "expected w1 complete artifact"

        # у w2 накопленная метрика count == total
        n_w2 = node_by_id(tdoc, "w2")
        got = int((n_w2.get("stats") or {}).get("count") or 0)
        assert got == total, f"analyzer should observe {total} items, got {got}"
    finally:
        await coord.stop()


@pytest.mark.asyncio
async def test_merge_generic_creates_complete_artifact(env_and_imports, inmemory_db, worker_factory, coord_cfg, worker_cfg):
    """
    coordinator_fn: merge.generic объединяет результаты узлов 'a' и 'b'.
    Проверяем:
      * таска завершается
      * есть complete-артефакт у merge-узла 'm'
      * у узлов 'a' и 'b' есть артефакты (partial/complete — не важно)
    """
    cd, _ = env_and_imports

    # один воркер indexer обработает оба корня
    await worker_factory(("indexer", build_indexer_handler(db=inmemory_db)))

    graph = prime_graph(cd, graph_merge_generic())

    coord = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
    await coord.start()
    try:
        task_id = await coord.create_task(params={}, graph=graph)
        await wait_task_finished(inmemory_db, task_id, timeout=12.0)

        # merge.complete
        a_m = await inmemory_db.artifacts.find_one({"task_id": task_id, "node_id": "m", "status": "complete"})
        assert a_m is not None, "expected merge node 'm' to publish complete artifact"

        # источники тоже что-то оставили
        cnt_a = 0
        async for _ in inmemory_db.artifacts.find({"task_id": task_id, "node_id": "a"}):
            cnt_a += 1
        cnt_b = 0
        async for _ in inmemory_db.artifacts.find({"task_id": task_id, "node_id": "b"}):
            cnt_b += 1
        assert cnt_a > 0 and cnt_b > 0, "upstreams should have artifacts too"
    finally:
        await coord.stop()
