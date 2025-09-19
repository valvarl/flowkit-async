from __future__ import annotations

"""
flowkit.core.utils
==================

Low-level helpers with **no external dependencies**:
- Stable hashing for JSON-like payloads.
- Compact JSON (de)serialization helpers.
- Jitter utilities for backoff/randomization.
- NanoID generator (URL-safe, crypto-strong).
"""

import json
from hashlib import blake2b
from secrets import choice, randbelow
from typing import Any

from .types import DEFAULT_BLAKE2_DIGEST_SIZE, DEFAULT_NANOID_ALPHABET, DEFAULT_NANOID_SIZE


def stable_hash(payload: Any, *, digest_size: int = DEFAULT_BLAKE2_DIGEST_SIZE) -> str:
    """
    Compute a stable hash of an arbitrary JSON-like payload.
    - Uses UTF-8 JSON with sorted keys and no whitespace for deterministic encoding.
    - BLAKE2b with configurable digest size (default 20 bytes -> 40 hex chars).

    NOTE: This is **not** a cryptographic signature; use it for idempotency keys, cache keys, etc.

    Args:
        payload: JSON-serializable object.
        digest_size: BLAKE2b digest size in bytes (1..64).

    Returns:
        Hex string of the digest.
    """
    data = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return blake2b(data, digest_size=digest_size).hexdigest()


def dumps(x: Any) -> bytes:
    """
    Compact JSON dump to UTF-8 bytes (ensure_ascii=False, no spaces).
    Prefer this for message payloads persisted to Kafka/DB.
    """
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def loads(b: bytes) -> Any:
    """Inverse of dumps(): parse UTF-8 JSON bytes back to Python objects."""
    return json.loads(b.decode("utf-8"))


def jitter_ms(base_ms: int, *, pct: float = 0.20, floor_ms: int = 0) -> int:
    """
    Apply symmetric jitter around base value:
        result = base_ms + delta, where delta ∈ [-base_ms*pct, +base_ms*pct]

    Examples:
        jitter_ms(1000) -> value in [800..1200] by default
        jitter_ms(1000, pct=0.5) -> [500..1500]

    Args:
        base_ms: base value in milliseconds.
        pct: relative half-width (0.0..1.0).
        floor_ms: clamp the result to be at least this value.

    Returns:
        Jittered value in milliseconds.
    """
    if base_ms <= 0 or pct <= 0:
        return max(floor_ms, base_ms)
    span = int(base_ms * pct)
    # Uniform integer in [-span, +span]
    delta = randbelow(2 * span + 1) - span
    return max(floor_ms, base_ms + delta)


def nanoid(size: int = DEFAULT_NANOID_SIZE, alphabet: str = DEFAULT_NANOID_ALPHABET) -> str:
    """
    Generate a URL-safe NanoID (cryptographically strong).

    Args:
        size: number of characters.
        alphabet: allowed characters (default URL-safe).

    Returns:
        Random string of given size.
    """
    if size <= 0:
        raise ValueError("size must be positive")
    if not alphabet:
        raise ValueError("alphabet must be a non-empty string")
    return "".join(choice(alphabet) for _ in range(size))
