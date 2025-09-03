# tests/test_outbox_dispatcher.py
from __future__ import annotations

import asyncio
import time
from datetime import datetime

import pytest

pytestmark = pytest.mark.use_outbox


def _dt_to_ms(dt: datetime | None) -> int:
    return int(dt.timestamp() * 1000) if dt else 0


@pytest.mark.asyncio
async def test_outbox_retry_backoff(env_and_imports, inmemory_db, coord, monkeypatch):
    """
    First two _raw_send calls fail → outbox goes to 'retry' with exponential backoff (with jitter),
    then on the 3rd attempt becomes 'sent'.
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.retry", "k1"

    attempts: dict[str, int] = {}
    orig_raw = coord.bus._raw_send

    async def flaky_raw_send(t: str, k: bytes, env):
        kk = f"{t}:{k.decode()}"
        attempts[kk] = attempts.get(kk, 0) + 1
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

    async def wait_state(expect: str, timeout: float = 5.0):  # noqa: ASYNC109
        t0 = time.time()
        while time.time() - t0 < timeout:
            d = await inmemory_db.outbox.find_one({"topic": topic, "key": key})
            if d and d.get("state") == expect:
                return d
            await asyncio.sleep(0.03)
        raise AssertionError(f"outbox not in '{expect}'")

    # After 1st failure
    d1 = await wait_state("retry", timeout=3.0)
    assert int(d1.get("attempts", 0)) == 1
    base1 = min(max(coord.cfg.outbox_backoff_min_ms, (2**1) * 100), coord.cfg.outbox_backoff_max_ms)
    min_expected_1 = int(base1 * 0.8)  # allow jitter -20%
    gap1 = int(d1.get("next_attempt_at_ms", 0)) - _dt_to_ms(d1.get("updated_at"))
    assert gap1 >= min_expected_1

    # After 2nd failure
    async def wait_attempts_ge(n: int, timeout: float = 6.0):  # noqa: ASYNC109
        t0 = time.time()
        while time.time() - t0 < timeout:
            d = await inmemory_db.outbox.find_one({"topic": topic, "key": key})
            if d and d.get("state") == "retry" and int(d.get("attempts", 0)) >= n:
                return d
            await asyncio.sleep(0.03)
        raise AssertionError(f"outbox attempts not >= {n}")

    d2 = await wait_attempts_ge(2, timeout=6.0)
    assert int(d2.get("attempts", 0)) == 2
    base2 = min(max(coord.cfg.outbox_backoff_min_ms, (2**2) * 100), coord.cfg.outbox_backoff_max_ms)
    min_expected_2 = int(base2 * 0.8)
    gap2 = int(d2.get("next_attempt_at_ms", 0)) - _dt_to_ms(d2.get("updated_at"))
    assert gap2 >= min_expected_2

    # 3rd attempt → sent
    await wait_state("sent", timeout=6.0)
    assert attempts[f"{topic}:{key}"] == 3, attempts


@pytest.mark.asyncio
async def test_outbox_exactly_once_fp_uniqueness(env_and_imports, inmemory_db, coord, monkeypatch):
    """
    Two enqueues with identical (topic,key,dedup_id) → single outbox row and exactly one real send.
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.once", "k2"

    # emulate unique index on fp
    seen_fp = set()
    orig_insert = inmemory_db.outbox.insert_one

    async def unique_insert_one(doc):
        fp = doc.get("fp")
        if fp in seen_fp:
            raise RuntimeError("duplicate key on fp")
        seen_fp.add(fp)
        await orig_insert(doc)

    inmemory_db.outbox.insert_one = unique_insert_one  # type: ignore

    sent_calls: list[tuple[str, str, str]] = []
    sent_evt = asyncio.Event()
    orig_raw = coord.bus._raw_send

    async def counting_raw_send(t, k, env):
        sent_calls.append((t, k.decode(), env.dedup_id))
        # signal once the expected send happens
        if t == topic and k.decode() == key:
            sent_evt.set()
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
    await coord.outbox.enqueue(topic=topic, key=key, env=env2)  # duplicate → ignored

    # wait for the send to happen
    await asyncio.wait_for(sent_evt.wait(), timeout=3.0)

    # exactly one send
    sent_cnt = sum(1 for s in sent_calls if s[0] == topic and s[1] == key)
    assert sent_cnt == 1

    # exactly one outbox row (state=sent)
    docs = [r for r in inmemory_db.outbox.rows if r.get("topic") == topic and r.get("key") == key]
    assert len(docs) == 1
    assert docs[0].get("state") == "sent"
