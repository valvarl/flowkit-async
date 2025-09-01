import os

import pytest

from tests.helpers import (apply_overrides, install_inmemory_db,
                           setup_env_and_imports)
from tests.helpers.handlers import make_test_handlers


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
    cd, wu = env_and_imports
    return install_inmemory_db(monkeypatch, cd, wu)

@pytest.fixture
def handlers(env_and_imports, request):
    """
    Returns a dict of handlers; you can limit via:
    @pytest.mark.use_handlers(["indexer","analyzer"])
    """
    _, wu = env_and_imports
    m = request.node.get_closest_marker("use_handlers")
    include = m.args[0] if m and m.args else None
    return make_test_handlers(wu, include=include)

@pytest.fixture
def set_constants(monkeypatch, env_and_imports):
    """
    Best-effort constant override for coordinator/worker.
    Usage:
        set_constants(coord={"HEARTBEAT_SOFT_SEC": 0.4}, worker={"HEARTBEAT_INTERVAL_SEC": 1.0})
    """
    cd, wu = env_and_imports
    def _apply(coord=None, worker=None):
        apply_overrides(monkeypatch, cd=cd, wu=wu, coord_overrides=coord, worker_overrides=worker)
        return cd, wu
    return _apply
