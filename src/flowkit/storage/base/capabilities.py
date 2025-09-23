# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StorageCapabilities:
    """
    Feature flags advertised by a provider. The contracts must not rely on them,
    but callers can take advantage when present.
    """

    transactions: bool = False
    watch: bool = False  # change streams / pub-sub
    kv_ttl: bool = False  # native TTL on KV entries
    outbox_fifo: bool = False  # stable FIFO guarantees
    artifact_promote_cheap: bool = False  # bucket move w/o copy
