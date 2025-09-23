# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from ...api.errors import FlowkitError

"""
Common storage error hierarchy used by all storage contracts and providers.
"""


class StorageError(FlowkitError):
    """Base error for storage layer (provider-agnostic)."""


class RetryableStorageError(StorageError):
    """Transient provider error (network hiccup, serialization timeout, etc.)."""
