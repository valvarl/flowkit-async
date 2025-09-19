from __future__ import annotations

"""
flowkit.core.config
===================

Strongly-typed configurations for Coordinator and Worker.
- No external deps; optional JSON file loading.
- Derives millisecond fields from seconds to avoid repeated conversions.
- Provides small env overrides for convenience.

If a config file path is not provided or not found, sane defaults are used.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _parse_csv_env(name: str) -> list[str]:
    val = os.getenv(name)
    if not val:
        return []
    return [s.strip() for s in val.split(",") if s.strip()]


def _try_load_json(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        # Fail soft (callers may still override)
        pass
    return {}


# ---------------------------------------------------------------------------


@dataclass
class CoordinatorConfig:
    """Coordinator configuration loaded from JSON/env with derived millisecond fields."""

    # ---- Bus / Kafka
    kafka_bootstrap: str = "kafka:9092"
    worker_types: list[str] = field(default_factory=lambda: ["indexer", "enricher", "grouper", "analyzer"])

    # ---- Topics
    topic_cmd_fmt: str = "cmd.{type}.v1"
    topic_status_fmt: str = "status.{type}.v1"
    topic_worker_announce: str = "workers.announce.v1"
    topic_query: str = "query.tasks.v1"
    topic_reply: str = "reply.tasks.v1"
    topic_signals: str = "signals.v1"

    # ---- Timings (seconds)
    heartbeat_soft_sec: int = 300
    heartbeat_hard_sec: int = 3600
    lease_ttl_sec: int = 45
    discovery_window_sec: int = 8
    cancel_grace_sec: int = 30
    scheduler_tick_sec: float = 1.0
    finalizer_tick_sec: float = 5.0
    hb_monitor_tick_sec: float = 10.0
    outbox_dispatch_tick_sec: float = 0.25

    # ---- Outbox
    outbox_max_retry: int = 12
    outbox_backoff_min_ms: int = 250
    outbox_backoff_max_ms: int = 60_000

    # ---- Validation policy
    strict_input_adapters: bool = False

    # ---- Derived (ms)
    hb_soft_ms: int = 0
    hb_hard_ms: int = 0
    lease_ttl_ms: int = 0
    discovery_window_ms: int = 0
    cancel_grace_ms: int = 0
    scheduler_tick_ms: int = 0
    finalizer_tick_ms: int = 0
    hb_monitor_tick_ms: int = 0
    outbox_dispatch_tick_ms: int = 0

    # ---- Methods ------------------------------------------------------------

    def __post_init__(self) -> None:
        if not self.kafka_bootstrap:
            raise ValueError("kafka_bootstrap must be a non-empty string")
        if not isinstance(self.worker_types, list) or not all(isinstance(x, str) and x for x in self.worker_types):
            raise ValueError("worker_types must be a list of non-empty strings")
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

    # Topic helpers
    def topic_cmd(self, step_type: str) -> str:
        return self.topic_cmd_fmt.format(type=step_type)

    def topic_status(self, step_type: str) -> str:
        return self.topic_status_fmt.format(type=step_type)

    # Loader
    @classmethod
    def load(cls, path: Path | str | None = None, *, overrides: dict[str, Any] | None = None) -> CoordinatorConfig:
        """
        Load config from JSON file (if provided), then apply env and overrides.

        Env overrides:
          - KAFKA_BOOTSTRAP_SERVERS
          - WORKER_TYPES (comma-separated)
        """
        data: dict[str, Any] = {}

        # File
        file_path: Path | None = Path(path) if path else None
        data.update(_try_load_json(file_path))

        # Env
        if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
            data["kafka_bootstrap"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        roles = _parse_csv_env("WORKER_TYPES")
        if roles:
            data["worker_types"] = roles

        # Overrides
        if overrides:
            data.update(overrides)

        return cls(**data)


# ---------------------------------------------------------------------------


@dataclass
class WorkerConfig:
    """Worker configuration including adapter policy and timing."""

    # Bus / Kafka
    kafka_bootstrap: str = "kafka:9092"

    # Topics
    topic_cmd_fmt: str = "cmd.{type}.v1"
    topic_status_fmt: str = "status.{type}.v1"
    topic_worker_announce: str = "workers.announce.v1"
    topic_query: str = "query.tasks.v1"
    topic_reply: str = "reply.tasks.v1"
    topic_signals: str = "signals.v1"

    # Identity
    roles: list[str] = field(default_factory=lambda: ["echo"])
    worker_id: str | None = None
    worker_version: str = "2.0.0"

    # Timing (seconds)
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

    # Policies / derived
    strict_input_adapters: bool = False
    lease_ttl_ms: int = 60_000
    hb_interval_ms: int = 20_000
    announce_interval_ms: int = 60_000

    # ---- Methods ------------------------------------------------------------

    def __post_init__(self) -> None:
        if not self.kafka_bootstrap:
            raise ValueError("kafka_bootstrap must be a non-empty string")
        if not isinstance(self.roles, list) or not all(isinstance(x, str) and x for x in self.roles):
            raise ValueError("roles must be a list of non-empty strings")
        if self.dedup_cache_size < 0:
            raise ValueError("dedup_cache_size must be non-negative")
        self.lease_ttl_ms = int(self.lease_ttl_sec * 1000)
        self.hb_interval_ms = int(self.hb_interval_sec * 1000)
        self.announce_interval_ms = int(self.announce_interval_sec * 1000)

    # Topic helpers
    def topic_cmd(self, step_type: str) -> str:
        return self.topic_cmd_fmt.format(type=step_type)

    def topic_status(self, step_type: str) -> str:
        return self.topic_status_fmt.format(type=step_type)

    # Loader
    @staticmethod
    def load(path: Path | str | None = None, overrides: dict[str, Any] | None = None) -> WorkerConfig:
        """
        Load config from JSON file (if provided), then apply overrides.
        Also honors env var KAFKA_BOOTSTRAP_SERVERS for convenience.
        """
        data: dict[str, Any] = {}
        data.update(_try_load_json(Path(path) if path else None))
        if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
            data["kafka_bootstrap"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        if overrides:
            data.update(overrides)
        return WorkerConfig(**data)
