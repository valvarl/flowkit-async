# conftest.py
from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
import pytest_asyncio

from flowkit.core.log import (
    bind_context,
    configure_from_env,
    enable_stdout_logging,
    get_logger,
    log_context,
)
from tests.helpers import install_inmemory_db, setup_env_and_imports
from tests.helpers.kafka import ChaosConfig, enable_chaos


def pytest_configure(config):
    config.addinivalue_line("markers", "cfg(coord=None, worker=None): per-test Coordinator/Worker config overrides")
    config.addinivalue_line("markers", "worker_types(name): override worker types for this test module")
    config.addinivalue_line("markers", "use_handlers(names): restrict which test handlers to instantiate")
    config.addinivalue_line("markers", "use_outbox: enable real Outbox dispatcher (no bypass)")
    config.addinivalue_line("markers", "chaos: enable Kafka chaos mode (jitter/dup/drop) for this test")


def pytest_addoption(parser):
    parser.addoption(
        "--log-json",
        action="store_true",
        default=False,
        help="Emit Flowkit logs in JSON format during tests",
    )


@pytest.fixture(scope="session", autouse=True)
def _configure_flowkit_logging(request):
    configure_from_env()
    prefer_json = request.config.getoption("--log-json")
    # Если явно не включили через env, включим сами (по умолчанию человеко-читаемо)
    if os.getenv("FLOWKIT_LOG_STDOUT", "").lower() not in ("1", "true", "yes", "on"):
        enable_stdout_logging(
            level="DEBUG",
            json_output=prefer_json,
            pretty=not prefer_json,
            route_errors_to_stderr=True,  # удобно для CI
        )
    bind_context(role="pytest")


@pytest.fixture(scope="session")
def session_run_id():
    return uuid.uuid4().hex[:8]


@pytest.fixture(autouse=True)
def _test_log_context(request, session_run_id):
    log = get_logger("test")
    with log_context(pytest_nodeid=request.node.nodeid, test=request.node.name, test_run=session_run_id):
        log.debug("pytest.test.start", event="pytest.test.start")
        yield
        # outcome пишем из makereport ниже


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    if rep.when == "call":
        log = get_logger("test")
        with log_context(pytest_nodeid=item.nodeid, test=item.name):
            log.debug(
                "pytest.test.finish",
                event="pytest.test.finish",
                outcome=rep.outcome,
                duration=getattr(rep, "duration", None),
            )


def _cfg_overrides_from_marker(request):
    m = request.node.get_closest_marker("cfg")
    o_coord = (m.kwargs.get("coord") if m else {}) or {}
    o_worker = (m.kwargs.get("worker") if m else {}) or {}
    return o_coord, o_worker


_FAST_COORD = {
    "scheduler_tick_sec": 0.05,
    "discovery_window_sec": 0.05,
    "finalizer_tick_sec": 0.05,
    "hb_monitor_tick_sec": 0.2,
    "cancel_grace_sec": 0.1,
    "outbox_dispatch_tick_sec": 0.05,
}
_FAST_WORKER = {
    "hb_interval_sec": 0.2,
    "lease_ttl_sec": 2,
    "db_cancel_poll_ms": 50,
    "pull_poll_ms_default": 50,
    "pull_empty_backoff_ms_max": 300,
}


def _worker_types_from_marker(request, default: str) -> str:
    m = request.node.get_closest_marker("worker_types")
    if m and m.args:
        return m.args[0]
    return default


@pytest.fixture(scope="function")
def _outbox_env(monkeypatch, request):
    use_real = bool(request.node.get_closest_marker("use_outbox"))
    if use_real:
        monkeypatch.setenv("TEST_USE_OUTBOX", "1")
    else:
        monkeypatch.delenv("TEST_USE_OUTBOX", raising=False)
    return use_real


@pytest.fixture(scope="function")
def env_and_imports(monkeypatch, request, _outbox_env):
    """
    Common bootstrap: in-mem Kafka, optional real Outbox (via @use_outbox),
    worker types by marker.
    """
    wt = _worker_types_from_marker(request, default="indexer,enricher,ocr,analyzer,source,flaky,a,b,c,noop,sleepy")
    cd, wu = setup_env_and_imports(monkeypatch, worker_types=wt)

    # Toggle chaos if requested by the test
    if request.node.get_closest_marker("chaos"):
        # Safe defaults: small jitter and duplications; drops disabled to avoid flakiness
        enable_chaos(
            ChaosConfig(
                broker_delay_range=(0.0, 0.003),
                consumer_poll_delay_range=(0.0, 0.002),
                dup_prob_by_topic={"status.": 0.08, "cmd.": 0.05},
                drop_prob_by_topic={},  # keep off by default
            )
        )
    else:
        enable_chaos(None)

    return cd, wu


@pytest.fixture
def tlog():
    return get_logger("test")


@pytest.fixture(scope="function")
def inmemory_db(env_and_imports):
    """Single injection point for DB."""
    return install_inmemory_db()


@pytest.fixture
def coord_cfg(env_and_imports, request):
    cd, _ = env_and_imports
    wt = _worker_types_from_marker(request, default="indexer,enricher,ocr,analyzer,source,flaky,a,b,c,noop,sleepy")
    o_coord, _ = _cfg_overrides_from_marker(request)
    overrides = {"worker_types": [s.strip() for s in wt.split(",") if s.strip()], **_FAST_COORD, **o_coord}
    return cd.CoordinatorConfig.load(overrides=overrides)


@pytest.fixture
def worker_cfg(env_and_imports, request):
    _, wu = env_and_imports
    _, o_worker = _cfg_overrides_from_marker(request)
    overrides = {**_FAST_WORKER, **o_worker}
    return wu.WorkerConfig.load(overrides=overrides)


@pytest_asyncio.fixture
async def coord(env_and_imports, inmemory_db, coord_cfg):
    cd, _ = env_and_imports
    c = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
    await c.start()
    try:
        yield c
    finally:
        await c.stop()


@pytest_asyncio.fixture
async def worker_factory(env_and_imports, inmemory_db, worker_cfg):
    _, wu = env_and_imports
    started = []

    async def _spawn(*specs: tuple[str, Any]):
        ws = []
        for role, handler in specs:
            w = wu.Worker(db=inmemory_db, cfg=worker_cfg, roles=[role], handlers={role: handler})
            await w.start()
            started.append(w)
            ws.append(w)
        return ws

    try:
        yield _spawn
    finally:
        for w in reversed(started):
            try:
                await w.stop()
            except Exception:
                pass
