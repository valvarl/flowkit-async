# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
MongoDB storage provider registration.

This module registers the "mongodb" DSN scheme in the global plugin registry.
"""

from flowkit.api import PLUGIN_REGISTRY, StorageProvider
from flowkit.storage.base.capabilities import StorageCapabilities

from .factory import mongodb_factory

CAPS = StorageCapabilities(
    transactions=True,
    watch=True,
    kv_ttl=True,
    outbox_fifo=True,
    artifact_promote_cheap=True,
)

PLUGIN_REGISTRY.register_storage_provider(
    "mongodb",
    StorageProvider(
        name="mongodb",
        capabilities=CAPS,
        factory=mongodb_factory,
    ),
)
