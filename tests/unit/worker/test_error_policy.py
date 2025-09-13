from __future__ import annotations

import pytest

from flowkit.worker.handlers.base import RoleHandler  # type: ignore

pytestmark = [pytest.mark.unit]


def test_classify_error_permanent_for_common_programming_faults():
    h = RoleHandler()
    for exc in (TypeError("x"), ValueError("y"), KeyError("z")):
        reason, permanent = h.classify_error(exc)
        assert reason == "unexpected_error"
        assert permanent is True


def test_classify_error_non_permanent_for_other_exceptions():
    h = RoleHandler()
    reason, permanent = h.classify_error(RuntimeError("boom"))
    assert reason == "unexpected_error"
    assert permanent is False
