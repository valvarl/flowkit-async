# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Public API for resolving and managing lifecycle of externals.
"""

# Re-export protocol types for convenience of integrators
from ..api.externals import ExternalError, ExternalProvider, ExternalReady
from .registry import (
    ExternalFactory,
    ExternalsResolver,
    merge_declared,
)

__all__ = [
    "ExternalError",
    "ExternalFactory",
    "ExternalProvider",
    "ExternalReady",
    "ExternalsResolver",
    "merge_declared",
]
