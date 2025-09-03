# tests/test_outbox_dispatcher.py
from __future__ import annotations

import asyncio
import time
from datetime import datetime

import pytest
import pytest_asyncio

from tests.helpers import setup_env_and_imports, install_inmemory_db, dbg


# ───────────────────────── Local fixtures ─────────────────────────

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    """
    Enable real Outbox (no bypass) for this test file and import fresh modules.
    """
    monkeypatch.setenv("TEST_USE_OUTBOX", "1")
    return setup_env_and_imports(monkeypatch)


@pytest.fixture(scope="function")
def inmemory_db(env_and_imports):
    return install_inmemory_db()


@pytest_asyncio.fixture
async def coord(env_and_imports, inmemory_db, coord_cfg):
    """
    Coordinator bound to the in-memory DB and test config (fast ticks from conftest).
    """
    cd, _ = env_and_imports
    c = cd.Coordinator(db=inmemory_db, cfg=coord_cfg)
    dbg("COORD.STARTING")
    await c.start()
    dbg("COORD.STARTED")
    try:
        yield c
    finally:
        dbg("COORD.STOPPING")
        await c.stop()
        dbg("COORD.STOPPED")


# ───────────────────────── Helpers ─────────────────────────

def _dt_to_ms(dt: datetime | None) -> int:
    return int(dt.timestamp() * 1000) if dt else 0


# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_outbox_retry_backoff(env_and_imports, inmemory_db, coord, monkeypatch):
    """
    Two consecutive _raw_send failures → outbox goes to 'retry' with exponential backoff
    (respecting cfg min/max, with jitter). On the 3rd try it becomes 'sent'.
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.retry", "k1"

    attempts: dict[str, int] = {}
    orig_raw = coord.bus._raw_send

    async def flaky_raw_send(t: str, k: bytes, env):
        kk = f"{t}:{k.decode()}"
        attempts[kk] = attempts.get(kk, 0) + 1
        dbg("RAW_SEND.TRY", kk=kk, attempt=attempts[kk])
        if attempts[kk] <= 2:
            raise RuntimeError("broker temporary down")
        await orig_raw(t, k, env)

    monkeypatch.setattr(coord.bus, "_raw_send", flaky_raw_send, raising=True)

    env = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="d-1",
        task_id="t",
        node_id="n",
        step_type="echo",
        attempt_epoch=1,
        ts_ms=coord.clock.now_ms(),
        payload={"kind": "TEST"},
    )

    await coord.outbox.enqueue(topic=topic, key=key, env=env)

    async def wait_state(expect: str, timeout: float = 5.0):
        t0 = time.time()
        while time.time() - t0 < timeout:
            d = await inmemory_db.outbox.find_one({"topic": topic, "key": key})
            if d and d.get("state") == expect:
                return d
            await asyncio.sleep(0.03)
        raise AssertionError(f"outbox not in '{expect}'")

    # After 1st failure: retry, attempts=1, backoff >= ~80% of cfg.min (20% jitter allowed).
    d1 = await wait_state("retry", timeout=3.0)
    assert int(d1.get("attempts", 0)) == 1
    base1 = max(coord.cfg.outbox_backoff_min_ms, (2 ** 1) * 100)
    base1 = min(base1, coord.cfg.outbox_backoff_max_ms)
    min_expected_1 = int(base1 * 0.8)
    gap1 = int(d1.get("next_attempt_at_ms", 0)) - _dt_to_ms(d1.get("updated_at"))
    assert gap1 >= min_expected_1, f"backoff too small: got {gap1}ms, expected ≥ {min_expected_1}ms"

    # After 2nd failure: still retry, attempts=2, backoff with same rule.
    async def wait_attempts_ge(n: int, timeout: float = 6.0):
        t0 = time.time()
        while time.time() - t0 < timeout:
            d = await inmemory_db.outbox.find_one({"topic": topic, "key": key})
            if d and d.get("state") == "retry" and int(d.get("attempts", 0)) >= n:
                return d
            await asyncio.sleep(0.03)
        raise AssertionError(f"outbox attempts not >= {n}")

    d2 = await wait_attempts_ge(2, timeout=6.0)
    assert int(d2.get("attempts", 0)) == 2
    base2 = max(coord.cfg.outbox_backoff_min_ms, (2 ** 2) * 100)
    base2 = min(base2, coord.cfg.outbox_backoff_max_ms)
    min_expected_2 = int(base2 * 0.8)
    gap2 = int(d2.get("next_attempt_at_ms", 0)) - _dt_to_ms(d2.get("updated_at"))
    assert gap2 >= min_expected_2, f"backoff too small: got {gap2}ms, expected ≥ {min_expected_2}ms"

    # 3rd attempt succeeds → 'sent'
    d3 = await wait_state("sent", timeout=6.0)
    assert attempts[f"{topic}:{key}"] == 3, attempts


@pytest.mark.asyncio
async def test_outbox_exactly_once_fp_uniqueness(env_and_imports, inmemory_db, coord, monkeypatch):
    """
    Two enqueues with the same (topic,key,dedup_id) should result in a single outbox record
    and exactly one real send (simulate unique index on 'fp').
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.once", "k2"

    # Simulate unique index on 'fp' at the DB layer.
    seen_fp = set()
    orig_insert = inmemory_db.outbox.insert_one

    async def unique_insert_one(doc):
        fp = doc.get("fp")
        if fp in seen_fp:
            dbg("DB.OUTBOX.DUP_FP_BLOCK", fp=fp)
            raise RuntimeError("duplicate key on fp")
        seen_fp.add(fp)
        await orig_insert(doc)

    inmemory_db.outbox.insert_one = unique_insert_one  # type: ignore

    sent_calls: list[tuple[str, str, str]] = []
    orig_raw = coord.bus._raw_send

    async def counting_raw_send(t, k, env):
        sent_calls.append((t, k.decode(), env.dedup_id))
        await orig_raw(t, k, env)

    monkeypatch.setattr(coord.bus, "_raw_send", counting_raw_send, raising=True)

    env1 = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="same-dedup",
        task_id="t",
        node_id="n",
        step_type="echo",
        attempt_epoch=1,
        ts_ms=coord.clock.now_ms(),
        payload={"kind": "TEST"},
    )
    env2 = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="same-dedup",
        task_id="t",
        node_id="n",
        step_type="echo",
        attempt_epoch=1,
        ts_ms=coord.clock.now_ms(),
        payload={"kind": "TEST"},
    )

    await coord.outbox.enqueue(topic=topic, key=key, env=env1)
    await coord.outbox.enqueue(topic=topic, key=key, env=env2)  # duplicate fp → ignored by unique insert

    # Wait for the send to happen
    t0 = time.time()
    while time.time() - t0 < 3.0 and not any(s for s in sent_calls if s[0] == topic and s[1] == key):
        await asyncio.sleep(0.02)

    # Exactly one send
    sent_cnt = sum(1 for s in sent_calls if s[0] == topic and s[1] == key)
    assert sent_cnt == 1, f"expected exactly one send, got {sent_cnt}"

    # Exactly one outbox record (state=sent) for (topic,key)
    docs = [r for r in inmemory_db.outbox.rows if r.get("topic") == topic and r.get("key") == key]
    assert len(docs) == 1
    assert docs[0].get("state") == "sent"
