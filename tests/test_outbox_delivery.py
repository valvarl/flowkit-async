# tests/test_outbox_dispatcher.py
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime

import pytest

from flowkit.core.log import log_context
from tests.helpers.kafka import BROKER

pytestmark = pytest.mark.use_outbox


def _dt_to_ms(dt: datetime | None) -> int:
    return int(dt.timestamp() * 1000) if dt else 0


def _wait_outbox_doc(
    db,
    *,
    topic: str,
    key: str,
    pred: Callable[[dict], bool],
    timeout: float = 6.0,
):
    """Poll the outbox row until `pred(doc)` returns True."""

    async def _impl():
        try:
            async with asyncio.timeout(timeout):
                while True:
                    d = await db.outbox.find_one({"topic": topic, "key": key})
                    if d and pred(d):
                        return d
                    await asyncio.sleep(0.03)
        except TimeoutError:
            raise AssertionError("outbox doc did not reach expected condition") from None

    return _impl()


@pytest.mark.asyncio
async def test_outbox_retry_backoff(env_and_imports, inmemory_db, coord, monkeypatch, tlog):
    """
    First two send() calls fail → outbox goes to 'retry' with exponential backoff (with jitter),
    then on the 3rd attempt becomes 'sent'.
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.retry", "k1"
    tlog.debug("test.start", event="test.start", test_name="outbox_retry_backoff", topic=topic, key=key)

    attempts: dict[str, int] = {}
    orig_send = coord.bus.send

    async def flaky_send(t: str, k: bytes | None, env):
        kk = f"{t}:{(k or b'').decode()}"
        attempts[kk] = attempts.get(kk, 0) + 1
        if attempts[kk] <= 2:
            raise RuntimeError("broker temporary down")
        await orig_send(t, k, env)

    monkeypatch.setattr(coord.bus, "send", flaky_send, raising=True)

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

    with log_context(topic=topic, key=key):
        await coord.outbox.enqueue(topic=topic, key=key, env=env)
        tlog.debug("outbox.enqueue", event="outbox.enqueue")

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
        tlog.debug(
            "outbox.retry.observed",
            event="outbox.retry.observed",
            attempts=1,
            gap_ms=gap1,
            min_expected_ms=min_expected_1,
        )
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
        tlog.debug(
            "outbox.retry.observed",
            event="outbox.retry.observed",
            attempts=2,
            gap_ms=gap2,
            min_expected_ms=min_expected_2,
        )
        assert gap2 >= min_expected_2

        # 3rd attempt → sent
        await wait_state("sent", timeout=6.0)
        tlog.debug("outbox.sent", event="outbox.sent", attempts=attempts.get(f"{topic}:{key}", 0))
        assert attempts[f"{topic}:{key}"] == 3, attempts


@pytest.mark.asyncio
async def test_outbox_exactly_once_fp_uniqueness(env_and_imports, inmemory_db, coord, monkeypatch, tlog):
    """
    Two enqueues with identical (topic,key,dedup_id) → single outbox row and exactly one real send.
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.once", "k2"
    tlog.debug("test.start", event="test.start", test_name="outbox_exactly_once_fp_uniqueness", topic=topic, key=key)

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
    orig_send = coord.bus.send

    async def counting_send(t, k, env):
        sent_calls.append((t, (k or b"").decode(), env.dedup_id))
        if t == topic and (k or b"").decode() == key:
            sent_evt.set()
        await orig_send(t, k, env)

    monkeypatch.setattr(coord.bus, "send", counting_send, raising=True)

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

    with log_context(topic=topic, key=key):
        await coord.outbox.enqueue(topic=topic, key=key, env=env1)
        await coord.outbox.enqueue(topic=topic, key=key, env=env2)  # duplicate → ignored
        tlog.debug("outbox.enqueue.twice", event="outbox.enqueue.twice")

        await asyncio.wait_for(sent_evt.wait(), timeout=3.0)

        # exactly one send
        sent_cnt = sum(1 for s in sent_calls if s[0] == topic and s[1] == key)
        tlog.debug("outbox.sent.count", event="outbox.sent.count", count=sent_cnt)
        assert sent_cnt == 1

        # exactly one outbox row (state=sent)
        docs = [r for r in inmemory_db.outbox.rows if r.get("topic") == topic and r.get("key") == key]
        assert len(docs) == 1
        assert docs[0].get("state") == "sent"


@pytest.mark.asyncio
@pytest.mark.use_outbox
async def test_outbox_crash_between_send_and_mark_sent(env_and_imports, inmemory_db, coord, monkeypatch, tlog):
    """
    Send succeeds, coordinator crashes before mark(sent) → after restart the outbox may resend,
    but there is only ONE effective delivery for (topic,key,dedup_id).
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.crash", "k-crash"
    tlog.debug(
        "test.start", event="test.start", test_name="outbox_crash_between_send_and_mark_sent", topic=topic, key=key
    )

    sent_calls: list[tuple[str, str, str]] = []
    orig_send = coord.bus.send

    async def counting_send(t, k, env):
        sent_calls.append((t, (k or b"").decode(), getattr(env, "dedup_id", "")))
        return await orig_send(t, k, env)

    monkeypatch.setattr(coord.bus, "send", counting_send, raising=True)

    # Broker-side idempotence by (topic, key, dedup_id)
    delivered = set()
    orig_produce = BROKER.produce

    async def dedup_produce(t, value):
        dedup_id = (value or {}).get("dedup_id") or ((value or {}).get("payload") or {}).get("dedup_id")
        fp = (t, key, dedup_id)  # bind test key explicitly
        if fp in delivered:
            return
        delivered.add(fp)
        await orig_produce(t, value)

    monkeypatch.setattr(BROKER, "produce", dedup_produce, raising=True)

    # Intercept the transition to 'sent' and simulate a crash before it.
    orig_update = inmemory_db.outbox.update_one
    crashed_once = {"done": False}

    async def intercept_update(filter_q, update_q, *args, **kwargs):
        set_map = (update_q or {}).get("$set") or {}
        if not crashed_once["done"] and set_map.get("state") == "sent":
            crashed_once["done"] = True
            tlog.debug("coord.crash.before_mark_sent", event="coord.crash.before_mark_sent")
            await coord.stop()
            raise asyncio.CancelledError("crash before mark(sent)")
        return await orig_update(filter_q, update_q, *args, **kwargs)

    inmemory_db.outbox.update_one = intercept_update  # type: ignore

    env = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="crash-dedup-1",
        task_id="t1",
        node_id="n1",
        step_type="echo",
        attempt_epoch=1,
        ts_ms=coord.clock.now_ms(),
        payload={"kind": "TEST"},
    )

    with log_context(topic=topic, key=key):
        await coord.outbox.enqueue(topic=topic, key=key, env=env)

        # Wait until at least one real send happened.
        await _wait_outbox_doc(
            inmemory_db,
            topic=topic,
            key=key,
            pred=lambda _d: len(sent_calls) >= 1,
            timeout=6.0,
        )
        tlog.debug("send.observed", event="send.observed", sends=len(sent_calls))

        # Bring the coordinator back.
        if hasattr(coord, "restart"):
            await coord.restart()
        else:
            await coord.start()

        # Give the dispatcher a moment to re-scan and (likely) resend.
        await asyncio.sleep(0.4)

        assert (topic, key, "crash-dedup-1") in delivered
        eff = sum(1 for fp in delivered if fp == (topic, key, "crash-dedup-1"))
        tlog.debug("delivery.effective", event="delivery.effective", effective=eff, sends=len(sent_calls))
        assert eff == 1, f"Expected exactly one effective delivery, got {eff}; raw sends: {sent_calls}"


@pytest.mark.asyncio
@pytest.mark.use_outbox
async def test_outbox_dedup_survives_restart(env_and_imports, inmemory_db, coord, monkeypatch, tlog):
    """
    Deduplication by (topic,key,dedup_id) persists across coordinator restart:
    re-enqueue of the same envelope after restart does not create a second outbox row or send.
    """
    cd, _ = env_and_imports
    topic, key, dedup_id = "test.topic.dedup", "k-dedup", "dedup-const"
    tlog.debug("test.start", event="test.start", test_name="outbox_dedup_survives_restart", topic=topic, key=key)

    sent_calls: list[tuple[str, str, str]] = []
    orig_send = coord.bus.send

    async def counting_send(t, k, env):
        sent_calls.append((t, (k or b"").decode(), getattr(env, "dedup_id", "")))
        return await orig_send(t, k, env)

    monkeypatch.setattr(coord.bus, "send", counting_send, raising=True)

    env = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id=dedup_id,
        task_id="t2",
        node_id="n2",
        step_type="echo",
        attempt_epoch=1,
        ts_ms=coord.clock.now_ms(),
        payload={"kind": "TEST"},
    )

    with log_context(topic=topic, key=key):
        # First enqueue → sent
        await coord.outbox.enqueue(topic=topic, key=key, env=env)
        await _wait_outbox_doc(inmemory_db, topic=topic, key=key, pred=lambda d: d.get("state") == "sent", timeout=6.0)
        tlog.debug("first.sent", event="first.sent")

        # Restart the coordinator
        if hasattr(coord, "restart"):
            await coord.restart()
        else:
            await coord.stop()
            await coord.start()

        # Re-enqueue the same envelope (same fp)
        await coord.outbox.enqueue(topic=topic, key=key, env=env)
        await asyncio.sleep(0.3)

        # Exactly one outbox row for (topic,key) and it is 'sent'
        docs = [r for r in inmemory_db.outbox.rows if r.get("topic") == topic and r.get("key") == key]
        tlog.debug("rows.inspect", event="rows.inspect", rows=len(docs), state=docs[0].get("state") if docs else None)
        assert len(docs) == 1
        assert docs[0].get("state") == "sent"

        # Exactly one physical send for the same triplet
        total_same = sum(1 for t, k, dd in sent_calls if t == topic and k == key and dd == dedup_id)
        assert total_same == 1, f"Expected 1 send, got {total_same} ({sent_calls})"


@pytest.mark.asyncio
@pytest.mark.use_outbox
@pytest.mark.cfg(coord={"outbox_backoff_min_ms": 50, "outbox_backoff_max_ms": 120, "outbox_dispatch_tick_sec": 0.02})
async def test_outbox_backoff_caps_with_jitter(env_and_imports, inmemory_db, coord, monkeypatch, tlog):
    """
    Exponential backoff is capped by max and jitter stays within a reasonable window.
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.backoffcap", "k-cap"
    tlog.debug("test.start", event="test.start", test_name="outbox_backoff_caps_with_jitter", topic=topic, key=key)

    async def always_fail(t, k, env):
        raise RuntimeError("forced broker failure")

    monkeypatch.setattr(coord.bus, "send", always_fail, raising=True)

    env = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="cap-dedup",
        task_id="t3",
        node_id="n3",
        step_type="echo",
        attempt_epoch=1,
        ts_ms=coord.clock.now_ms(),
        payload={"kind": "TEST"},
    )

    await coord.outbox.enqueue(topic=topic, key=key, env=env)

    # Wait until we have at least 4 attempts to ensure growth → cap
    d = await _wait_outbox_doc(
        inmemory_db,
        topic=topic,
        key=key,
        pred=lambda r: r.get("state") == "retry" and int(r.get("attempts", 0)) >= 4,
        timeout=8.0,
    )

    gap = int(d.get("next_attempt_at_ms", 0)) - _dt_to_ms(d.get("updated_at"))
    cap = coord.cfg.outbox_backoff_max_ms
    tlog.debug("backoff.cap.observe", event="backoff.cap.observe", gap_ms=gap, cap_ms=int(cap))
    assert 0 <= gap <= int(cap * 1.25), f"gap {gap} should be <= cap {cap} (± jitter)"
    assert gap >= int(cap * 0.6), f"gap {gap} is too small for capped backoff with jitter"


@pytest.mark.asyncio
@pytest.mark.use_outbox
@pytest.mark.cfg(
    coord={
        "outbox_max_retry": 3,
        "outbox_backoff_min_ms": 30,
        "outbox_backoff_max_ms": 60,
        "outbox_dispatch_tick_sec": 0.02,
    }
)
async def test_outbox_dlq_after_max_retries(env_and_imports, inmemory_db, coord, monkeypatch, tlog):
    """
    After reaching max retries, the outbox row is moved to a terminal state with attempts >= max.
    """
    cd, _ = env_and_imports
    topic, key = "test.topic.dlq", "k-dlq"
    tlog.debug("test.start", event="test.start", test_name="outbox_dlq_after_max_retries", topic=topic, key=key)

    async def always_fail(t, k, env):
        raise RuntimeError("permanent broker failure")

    monkeypatch.setattr(coord.bus, "send", always_fail, raising=True)

    env = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="dlq-dedup",
        task_id="t4",
        node_id="n4",
        step_type="echo",
        attempt_epoch=1,
        ts_ms=coord.clock.now_ms(),
        payload={"kind": "TEST"},
    )

    await coord.outbox.enqueue(topic=topic, key=key, env=env)

    max_val = int(coord.cfg.outbox_max_retry)
    d = await _wait_outbox_doc(
        inmemory_db,
        topic=topic,
        key=key,
        pred=lambda r: int(r.get("attempts", 0)) >= max_val and r.get("state") not in ("new", "retry", "sending"),
        timeout=10.0,
    )

    tlog.debug("outbox.terminal", event="outbox.terminal", attempts=int(d.get("attempts", 0)), state=d.get("state"))
    assert int(d.get("attempts", 0)) >= max_val
    assert d.get("state") in ("dlq", "dead", "failed"), f"unexpected terminal state: {d.get('state')}"
