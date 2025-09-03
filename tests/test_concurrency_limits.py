from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Set, Tuple

import pytest
import pytest_asyncio

from tests.helpers import AIOKafkaConsumerMock, dbg
from tests.helpers.graph import prime_graph, wait_task_finished
from tests.helpers.handlers import build_sleepy_handler

# Только нужные роли для ускорения
pytestmark = pytest.mark.worker_types("slow,fast")


# ───────────────────────── Small helpers ─────────────────────────

async def wait_all_tasks_finished(db, ids: List[str], timeout: float = 12.0) -> None:
    """Дождаться завершения всех задач по списку task_id (finished)."""
    async def _one(tid: str):
        await wait_task_finished(db, tid, timeout=timeout)
    await asyncio.gather(*(_one(t) for t in ids))

async def gather_running_counts(db) -> Tuple[int, Dict[str, int]]:
    """
    Возвращает:
     - общее число running-узлов;
     - распределение по типам.
    """
    total = 0
    per_type: Dict[str, int] = {}
    cur = db.tasks.find({})
    async for t in cur:
        for n in (t.get("graph", {}).get("nodes") or []):
            st = n.get("status")
            if str(st).endswith("running"):
                total += 1
                k = n.get("type")
                per_type[k] = per_type.get(k, 0) + 1
    return total, per_type


# ───────────────────────── Coordinator factory ─────────────────────────

@pytest_asyncio.fixture
async def coord_factory(env_and_imports, inmemory_db, coord_cfg):
    """
    Возвращает фабрику координаторов, позволяя задать лимиты до старта.
    Лимиты пробуем настраивать максимально совместимо:
      - если есть атрибут модуля (cd.MAX_GLOBAL_RUNNING / cd.MAX_TYPE_CONCURRENCY) — используем его;
      - иначе, если есть в cfg — устанавливаем там;
      - иначе тест xfail'ится (фича не поддерживается).
    """
    cd, _ = env_and_imports
    started = []

    def _apply_limit(name: str, value):
        # 1) модульные атрибуты (старый способ)
        if hasattr(cd, name):
            setattr(cd, name, value)
            return True
        # 2) поля в конфиге (новый способ)
        if hasattr(coord_cfg, name.lower()):
            setattr(coord_cfg, name.lower(), value)
            return True
        return False

    async def _spawn(**limits):
        # применяем заданные лимиты перед стартом
        unsupported = [k for k, v in limits.items() if not _apply_limit(k, v)]
        if unsupported:
            # если просили поставить лимит, а координатор его не знает — считаем фичу не реализованной
            pytest.xfail(f"Coordinator doesn't support limits: {unsupported}")

        c = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
        await c.start()
        started.append(c)
        return c

    try:
        yield _spawn
    finally:
        # аккуратно тушим всех поднятых координаторов
        for c in reversed(started):
            try:
                await c.stop()
            except Exception:
                pass


# ───────────────────────── Workers fixture ─────────────────────────

@pytest_asyncio.fixture
async def sleepy_handlers(inmemory_db):
    """
    Готовые test-handlers: slow (0.30s/батч) и fast (0.10s/батч).
    """
    slow = build_sleepy_handler(db=inmemory_db, role="slow", batches=1, sleep_s=0.30)
    fast = build_sleepy_handler(db=inmemory_db, role="fast", batches=1, sleep_s=0.10)
    return {"slow": slow, "fast": fast}


# ───────────────────────── Graph builders ─────────────────────────

def graph_many_roots(role: str, n: int) -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "nodes": [
            {"node_id": f"{role}-{i}", "type": role, "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}
            for i in range(n)
        ],
        "edges": [],
    }

def graph_two_types(slow_n: int, fast_n: int) -> Dict[str, Any]:
    nodes = [
        {"node_id": f"slow-{i}", "type": "slow", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}
        for i in range(slow_n)
    ] + [
        {"node_id": f"fast-{i}", "type": "fast", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}
        for i in range(fast_n)
    ]
    return {"schema_version": "1.0", "nodes": nodes, "edges": []}


# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_max_global_running_limit(env_and_imports, inmemory_db, coord_factory, worker_factory, sleepy_handlers):
    cd, _ = env_and_imports
    coord = await coord_factory(MAX_GLOBAL_RUNNING=2)  # глобальный лимит = 2

    # 5 независимых узлов типа "slow"
    g = prime_graph(cd, graph_many_roots("slow", 5))
    tid = await coord.create_task(params={}, graph=g)

    # 3 воркера slow (ресурсов хватит, чтобы превысить лимит — если бы он не работал)
    await worker_factory(("slow", sleepy_handlers["slow"]))
    await worker_factory(("slow", sleepy_handlers["slow"]))
    await worker_factory(("slow", sleepy_handlers["slow"]))

    # наблюдаем динамику параллелизма
    max_running = 0

    async def monitor():
        nonlocal max_running
        end = time.time() + 6.0
        while time.time() < end:
            total, _ = await gather_running_counts(inmemory_db)
            max_running = max(max_running, total)
            await asyncio.sleep(0.03)

    mon = asyncio.create_task(monitor())
    await wait_all_tasks_finished(inmemory_db, [tid], timeout=10.0)
    mon.cancel()
    try:
        await mon
    except Exception:
        pass

    assert max_running <= 2, f"MAX_GLOBAL_RUNNING violated, max_running={max_running}"


@pytest.mark.asyncio
async def test_max_type_concurrency_limits(env_and_imports, inmemory_db, coord_factory, worker_factory, sleepy_handlers):
    cd, _ = env_and_imports

    # Если координатор не поддерживает per-type лимиты — xfail из фабрики
    coord = await coord_factory(MAX_GLOBAL_RUNNING=99, MAX_TYPE_CONCURRENCY={"slow": 1, "fast": 2})

    g = prime_graph(cd, graph_two_types(slow_n=4, fast_n=5))
    tid = await coord.create_task(params={}, graph=g)

    # поднимем воркеров с запасом
    await worker_factory(("slow", sleepy_handlers["slow"]))
    await worker_factory(("slow", sleepy_handlers["slow"]))
    await worker_factory(("fast", sleepy_handlers["fast"]))
    await worker_factory(("fast", sleepy_handlers["fast"]))
    await worker_factory(("fast", sleepy_handlers["fast"]))

    max_running_total = 0
    max_running_slow = 0
    max_running_fast = 0

    async def monitor():
        nonlocal max_running_total, max_running_slow, max_running_fast
        end = time.time() + 8.0
        while time.time() < end:
            total, per_type = await gather_running_counts(inmemory_db)
            max_running_total = max(max_running_total, total)
            max_running_slow = max(max_running_slow, per_type.get("slow", 0))
            max_running_fast = max(max_running_fast, per_type.get("fast", 0))
            await asyncio.sleep(0.03)

    mon = asyncio.create_task(monitor())
    await wait_all_tasks_finished(inmemory_db, [tid], timeout=12.0)
    mon.cancel()
    try:
        await mon
    except Exception:
        pass

    assert max_running_slow <= 1, f"slow type concurrency violated: {max_running_slow}"
    assert max_running_fast <= 2, f"fast type concurrency violated: {max_running_fast}"


@pytest.mark.asyncio
async def test_multi_workers_same_type_rr_distribution(env_and_imports, inmemory_db, coord_factory, worker_factory, sleepy_handlers):
    cd, _ = env_and_imports
    coord = await coord_factory(MAX_GLOBAL_RUNNING=99)

    # 6 независимых узлов типа "slow"
    g = prime_graph(cd, graph_many_roots("slow", 6))
    tid = await coord.create_task(params={}, graph=g)

    # два воркера одного типа
    await worker_factory(("slow", sleepy_handlers["slow"]))
    await worker_factory(("slow", sleepy_handlers["slow"]))

    # шпионим статусный топик 'slow' — собираем TASK_ACCEPTED по worker_id
    consumer = AIOKafkaConsumerMock("status.slow.v1", group_id="test.spy")
    await consumer.start()
    seen_workers: Set[str] = set()

    async def spy():
        end = time.time() + 8.0
        while time.time() < end:
            rec = await consumer.getone()
            env = rec.value
            if env.get("msg_type") == "event" and (env.get("payload") or {}).get("kind") == "TASK_ACCEPTED":
                if env.get("task_id") == tid:
                    w = (env.get("payload") or {}).get("worker_id")
                    if w:
                        seen_workers.add(w)
            await asyncio.sleep(0.001)

    spy_task = asyncio.create_task(spy())
    await wait_all_tasks_finished(inmemory_db, [tid], timeout=10.0)
    spy_task.cancel()
    try:
        await spy_task
    except Exception:
        pass
    await consumer.stop()

    # хотя бы два разных воркера должны были принять разные узлы
    assert len(seen_workers) >= 2, f"expected distribution across >=2 workers, got {seen_workers}"


@pytest.mark.asyncio
async def test_concurrent_tasks_respect_global_limit(env_and_imports, inmemory_db, coord_factory, worker_factory, sleepy_handlers):
    cd, _ = env_and_imports
    coord = await coord_factory(MAX_GLOBAL_RUNNING=2)  # глобальный лимит = 2

    # Две параллельные задачи, каждая с набором независимых узлов slow
    g1 = prime_graph(cd, graph_many_roots("slow", 4))
    g2 = prime_graph(cd, graph_many_roots("slow", 5))
    t1 = await coord.create_task(params={}, graph=g1)
    t2 = await coord.create_task(params={}, graph=g2)

    # 3 воркера slow
    await worker_factory(("slow", sleepy_handlers["slow"]))
    await worker_factory(("slow", sleepy_handlers["slow"]))
    await worker_factory(("slow", sleepy_handlers["slow"]))

    max_running = 0

    async def monitor():
        nonlocal max_running
        end = time.time() + 10.0
        while time.time() < end:
            total, _ = await gather_running_counts(inmemory_db)
            max_running = max(max_running, total)
            await asyncio.sleep(0.03)

    mon = asyncio.create_task(monitor())
    await wait_all_tasks_finished(inmemory_db, [t1, t2], timeout=14.0)
    mon.cancel()
    try:
        await mon
    except Exception:
        pass

    assert max_running <= 2, f"global limit violated across concurrent tasks: {max_running}"
