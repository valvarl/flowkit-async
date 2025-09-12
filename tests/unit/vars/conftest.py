from __future__ import annotations

import pytest

from flowkit.coordinator.adapters import CoordinatorAdapters
from flowkit.core.time import ManualClock

pytestmark = [pytest.mark.unit, pytest.mark.vars]


@pytest.fixture
def manual_clock():
    return ManualClock()


@pytest.fixture
def adapters(inmemory_db, manual_clock):
    """
    Fresh CoordinatorAdapters for unit tests (no external detectors by default).
    """
    return CoordinatorAdapters(db=inmemory_db, clock=manual_clock)


@pytest.fixture
def limits(adapters):
    """
    Expose adapter private limits in a single place (tests rely on them).
    """
    return adapters._limits
