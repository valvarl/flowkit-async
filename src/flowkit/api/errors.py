# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Error taxonomy for FlowKit public extension API.

These exceptions are intended to be raised by user code (adapters, transform
ops, hook actions) and by the runtime. The Coordinator/Worker will classify
failures using this taxonomy to decide on retries, DLQ routing, or fast-fail.
"""


class FlowkitError(Exception):
    """Base class for all FlowKit public errors."""

    ...


class RetryableError(FlowkitError):
    """
    The operation failed due to a transient condition (timeouts, network hiccups,
    temporary unavailability). The engine is allowed to retry according to policy.
    """

    ...


class PermanentError(FlowkitError):
    """
    The operation failed due to a permanent condition (validation, schema/version
    mismatch, malformed input). Retrying would be pointless.
    """

    ...


class CancelledError(FlowkitError):
    """The operation was cancelled cooperatively (user/system-initiated)."""

    ...


class DeadlineExceeded(FlowkitError):
    """The operation exceeded its deadline (SLA or per-batch timeout)."""

    ...


class SchemaMismatch(FlowkitError):
    """Content/schema is incompatible with the adapter/transform expectations."""

    ...


class NotReady(FlowkitError):
    """An external dependency is not ready yet (e.g., awaiting provisioning)."""

    ...


class Backpressure(FlowkitError):
    """The consumer signalled backpressure; producers should slow down/pause."""

    ...
