import pytest

from tests.helpers import install_inmemory_db, setup_env_and_imports
from tests.helpers.handlers import make_test_handlers

def _cfg_overrides_from_marker(request):
    m = request.node.get_closest_marker("cfg")
    o_coord = (m.kwargs.get("coord") if m else {}) or {}
    o_worker = (m.kwargs.get("worker") if m else {}) or {}
    return o_coord, o_worker

# Базовые быстрые значения для всех тестов
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

@pytest.fixture
def coord_cfg(env_and_imports, request):
    cd, _ = env_and_imports
    # получаем worker_types из маркера, чтобы не ходить в ENV
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

def _worker_types_from_marker(request, default: str) -> str:
    m = request.node.get_closest_marker("worker_types")
    if m and m.args:
        return m.args[0]
    return default

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch, request):
    """
    Common bootstrap: in-mem Kafka, fast coordinator loops, outbox bypass.
    Worker types can be overridden via @pytest.mark.worker_types("a,b,c").
    """
    wt = _worker_types_from_marker(request, default="indexer,enricher,ocr,analyzer,source,flaky,a,b,c,noop,sleepy")
    cd, wu = setup_env_and_imports(monkeypatch, worker_types=wt)
    return cd, wu

@pytest.fixture(scope="function")
def inmemory_db(monkeypatch, env_and_imports):
    return install_inmemory_db()

@pytest.fixture
def handlers(env_and_imports, request, inmemory_db):
    """
    Returns a dict of handlers; you can limit via:
    @pytest.mark.use_handlers(["indexer","analyzer"])
    """
    _, wu = env_and_imports
    m = request.node.get_closest_marker("use_handlers")
    include = m.args[0] if m and m.args else None

    # make_test_handlers inspects caller locals for `_TESTS_DB`
    _TESTS_DB = inmemory_db  # noqa: F841 - accessed via inspect in helper
    return make_test_handlers(wu, inmemory_db, include=include)

