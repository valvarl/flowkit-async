"""
Unit test: default RoleHandler.classify_error marks config/programming errors as permanent.
We don't pin the exact reason string, only the permanence contract.
"""

from __future__ import annotations

import pytest

from flowkit.worker.handlers.base import RoleHandler  # type: ignore

pytestmark = [pytest.mark.unit, pytest.mark.adapters]


def test_programming_errors_are_permanent():
    h = RoleHandler()
    excs = [TypeError("x"), ValueError("y"), KeyError("z")]
    for e in excs:
        reason, permanent = h.classify_error(e)
        assert permanent is True
        assert isinstance(reason, str) and len(reason) > 0
