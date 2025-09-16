from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CoordinatorConfig:
    """Coordinator configuration loaded from JSON/env with derived millisecond fields."""

    # ---- Kafka & topics
    kafka_bootstrap: str = "kafka:9092"
    worker_types: list[str] = field(default_factory=lambda: ["indexer", "enricher", "grouper", "analyzer"])

    topic_cmd_fmt: str = "cmd.{type}.v1"
    topic_status_fmt: str = "status.{type}.v1"
    topic_worker_announce: str = "workers.announce.v1"
    topic_query: str = "query.tasks.v1"
    topic_reply: str = "reply.tasks.v1"
    topic_signals: str = "signals.v1"

    # ---- timings (seconds)
    heartbeat_soft_sec: int = 300
    heartbeat_hard_sec: int = 3600
    lease_ttl_sec: int = 45
    discovery_window_sec: int = 8
    cancel_grace_sec: int = 30
    scheduler_tick_sec: float = 1.0
    finalizer_tick_sec: float = 5.0
    hb_monitor_tick_sec: float = 10.0
    outbox_dispatch_tick_sec: float = 0.25

    # ---- outbox
    outbox_max_retry: int = 12
    outbox_backoff_min_ms: int = 250
    outbox_backoff_max_ms: int = 60_000

    # ---- input adapters policy (fail-fast at coordinator)
    # When True, the coordinator validates input adapters in strict mode,
    # mirroring WorkerConfig.strict_input_adapters.
    strict_input_adapters: bool = False

    # ---- derived (ms) — computed in __post_init__
    hb_soft_ms: int = 0
    hb_hard_ms: int = 0
    lease_ttl_ms: int = 0
    discovery_window_ms: int = 0
    cancel_grace_ms: int = 0
    scheduler_tick_ms: int = 0
    finalizer_tick_ms: int = 0
    hb_monitor_tick_ms: int = 0
    outbox_dispatch_tick_ms: int = 0

    def __post_init__(self) -> None:
        self._derive_ms()

    def _derive_ms(self) -> None:
        """Populate millisecond fields derived from second-based values."""
        self.hb_soft_ms = int(self.heartbeat_soft_sec * 1000)
        self.hb_hard_ms = int(self.heartbeat_hard_sec * 1000)
        self.lease_ttl_ms = int(self.lease_ttl_sec * 1000)
        self.discovery_window_ms = int(self.discovery_window_sec * 1000)
        self.cancel_grace_ms = int(self.cancel_grace_sec * 1000)
        self.scheduler_tick_ms = int(self.scheduler_tick_sec * 1000)
        self.finalizer_tick_ms = int(self.finalizer_tick_sec * 1000)
        self.hb_monitor_tick_ms = int(self.hb_monitor_tick_sec * 1000)
        self.outbox_dispatch_tick_ms = int(self.outbox_dispatch_tick_sec * 1000)

    # ---- topic helpers
    def topic_cmd(self, step_type: str) -> str:
        return self.topic_cmd_fmt.format(type=step_type)

    def topic_status(self, step_type: str) -> str:
        return self.topic_status_fmt.format(type=step_type)

    # ---- loading
    @classmethod
    def load(cls, path: str | Path | None = None, *, overrides: dict[str, Any] | None = None) -> CoordinatorConfig:
        """Load config from JSON (file or default), then apply env and explicit overrides."""
        data: dict[str, Any] = {}
        # 1) base JSON
        if path:
            p = Path(path)
        else:
            # default bundled config relative to repo root
            p = Path(__file__).resolve().parents[3] / "configs" / "coordinator.default.json"
        if p.exists():
            data.update(json.loads(p.read_text(encoding="utf-8")))

        # 2) environment
        if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
            data["kafka_bootstrap"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        if os.getenv("WORKER_TYPES"):
            data["worker_types"] = [s.strip() for s in os.environ["WORKER_TYPES"].split(",") if s.strip()]

        # 3) direct overrides (tests/CLI)
        if overrides:
            data.update(overrides)

        # __post_init__ will derive ms
        return cls(**data)


@dataclass
class WorkerConfig:
    """Worker configuration including adapter policy and timing."""

    # Kafka
    kafka_bootstrap: str = "kafka:9092"

    # Topic names/formats
    topic_cmd_fmt: str = "cmd.{type}.v1"
    topic_status_fmt: str = "status.{type}.v1"
    topic_worker_announce: str = "workers.announce.v1"
    topic_query: str = "query.tasks.v1"
    topic_reply: str = "reply.tasks.v1"
    topic_signals: str = "signals.v1"

    # Identity
    roles: list[str] = field(default_factory=lambda: ["echo"])
    worker_id: str | None = None  # if None -> will be generated
    worker_version: str = "2.0.0"

    # Timing (seconds → ms derivations)
    lease_ttl_sec: int = 60
    hb_interval_sec: int = 20
    announce_interval_sec: int = 60

    # Dedup
    dedup_cache_size: int = 10000
    dedup_ttl_ms: int = 3600_000

    # Pull adapters
    pull_poll_ms_default: int = 300
    pull_empty_backoff_ms_max: int = 4000

    # DB cancel poll
    db_cancel_poll_ms: int = 500

    # ---- derived (computed in post_init) ----
    # When True, certain adapters must receive explicit args (e.g. meta_list_key),
    # otherwise the worker fails early with bad_input_args.
    strict_input_adapters: bool = False
    lease_ttl_ms: int = 60_000
    hb_interval_ms: int = 20_000
    announce_interval_ms: int = 60_000

    def __post_init__(self) -> None:
        self.lease_ttl_ms = int(self.lease_ttl_sec * 1000)
        self.hb_interval_ms = int(self.hb_interval_sec * 1000)
        self.announce_interval_ms = int(self.announce_interval_sec * 1000)

    # ---- topic helpers
    def topic_cmd(self, step_type: str) -> str:
        return self.topic_cmd_fmt.format(type=step_type)

    def topic_status(self, step_type: str) -> str:
        return self.topic_status_fmt.format(type=step_type)

    # ---- loader
    @staticmethod
    def load(path: str | Path | None = None, overrides: dict[str, Any] | None = None) -> WorkerConfig:
        """Load config from JSON (file or default), then apply overrides."""
        data: dict[str, Any] = {}
        if path:
            p = Path(path)
        else:
            # default bundled config relative to repo root
            p = Path(__file__).resolve().parents[3] / "configs" / "worker.default.json"
        if p.exists():
            data.update(json.loads(p.read_text(encoding="utf-8")))
        if overrides:
            data.update(overrides)
        return WorkerConfig(**data)
