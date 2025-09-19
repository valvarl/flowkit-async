# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
External provider protocol and typed handles.

Externals represent named integration points declared in the graph (kafka topics,
HTTP endpoints, secrets, locks, ratelimits, etc). Providers resolve config into
runtime handles usable by adapters/hooks/expressions.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


class ExternalError(Exception):
    """Base external provider error."""

    ...


@dataclass(frozen=True)
class ExternalReady:
    """
    Lightweight readiness status reflected in expressions as `external.<name>.ready`.
    """

    ready: bool
    detail: str | None = None


# -- Typed handles for common external kinds ---------------------------------


@dataclass(frozen=True)
class KafkaTopic:
    """Resolved Kafka topic handle."""

    bootstrap: str
    topic: str
    client_args: Mapping[str, Any] | None = None  # e.g., security options


@dataclass(frozen=True)
class HttpEndpoint:
    """Resolved HTTP endpoint handle."""

    base_url: str
    headers: Mapping[str, str] | None = None
    timeout_ms: int | None = None


@dataclass(frozen=True)
class SecretStore:
    """Secrets provider handle (Vault, cloud KMS, etc.)."""

    provider: str
    path: str


@dataclass(frozen=True)
class DistributedLock:
    """Distributed lock namespace handle (e.g., Redis, ZK)."""

    kind: str
    namespace: str


@dataclass(frozen=True)
class RateLimiter:
    """Rate limiter handle."""

    window_ms: int
    capacity: int
    bucket: str | None = None


# -- Provider protocol --------------------------------------------------------


@runtime_checkable
class ExternalProvider(Protocol):
    """
    Provider that backs one or more external resources.

    Responsibilities:
      - parse/validate provider config
      - expose readiness/facts for expressions
      - optionally provide a notify/push Source adapter factory (webhooks etc.)
    """

    name: str

    async def open(self, config: Mapping[str, Any]) -> None: ...
    async def ready(self) -> ExternalReady: ...
    async def close(self) -> None: ...

    # Optional: return a SourceAdapter class/factory bound to this external (notify mode)
    def source_factory(self) -> Any: ...
