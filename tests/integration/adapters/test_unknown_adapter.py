from __future__ import annotations

import pytest
from tests.helpers.graph import make_graph, prime_graph, wait_task_status

from flowkit.protocol.messages import RunState
from flowkit.worker.handlers.base import Batch, RoleHandler  # type: ignore

pytestmark = [pytest.mark.integration, pytest.mark.adapters, pytest.mark.worker_types("probe")]


class ProbeGuard(RoleHandler):
    role = "probe"

    def __init__(self):
        self.iter_called = False

    async def iter_batches(self, loaded):
        self.iter_called = True
        yield Batch(batch_uid="x", payload={})


@pytest.mark.asyncio
async def test_cmd_unknown_adapter_permanent_fail(env_and_imports, inmemory_db, coord, worker_factory, tlog):
    cd, _ = env_and_imports
    handler = ProbeGuard()
    await worker_factory(("probe", handler))
    probe = {
        "node_id": "probe",
        "type": "probe",
        "depends_on": [],
        "fan_in": "all",
        "io": {
            "input_inline": {
                "input_adapter": "does.not.exist",
                "input_args": {"poll_ms": 10},
            }
        },
        "status": None,
        "attempt_epoch": 0,
    }
    g = prime_graph(cd, make_graph(nodes=[probe], edges=[]))
    tid = await coord.create_task(params={}, graph=g)
    await wait_task_status(inmemory_db, tid, RunState.failed.value, timeout=6.0)
    assert handler.iter_called is False
