from __future__ import annotations

"""
flowkit.security.redaction
==========================

Sensitive-data redaction utilities for logs/metrics/events.

Principles:
- Fail-open: if unsure, only redact obvious secrets to avoid destroying useful logs.
- Deterministic and cheap: pure-Python, no heavy regex machinery in hot paths.
- Configurable: allow custom key patterns and text scrubbers.

The main entry points:
- `Redactor`: policy object with key/value heuristics.
- `redact_obj(obj, redactor, in_place=False)`: recursively redact mappings/lists/strings.
- `Redactor.redact_text(s)`: scrub likely tokens from free text (best-effort).
"""

from typing import Any, Mapping, MutableMapping, Iterable, Sequence, Dict, List
import math
import re


__all__ = [
    "Redactor",
    "redact_obj",
]


# Common key names that often hold secrets (case-insensitive).
_DEFAULT_SENSITIVE_KEYS = frozenset(
    s.lower()
    for s in [
        "password",
        "passwd",
        "secret",
        "api_key",
        "apikey",
        "token",
        "access_token",
        "refresh_token",
        "auth",
        "authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "client_secret",
        "private_key",
        "signature",
    ]
)


def _entropy_bits_per_char(s: str) -> float:
    """
    Approximate Shannon entropy per character (bits/char).
    Higher numbers indicate more randomness. Perfectly uniform ascii64 ~ 6 bits/char.
    """
    if not s:
        return 0.0
    freq: Dict[str, int] = {}
    for ch in s:
        freq[ch] = freq.get(ch, 0) + 1
    H = 0.0
    n = len(s)
    for c in freq.values():
        p = c / n
        H -= p * math.log2(p)
    return H


class Redactor:
    """
    Redaction policy with key-based and value-based heuristics.

    Key detection:
        - case-insensitive match against `sensitive_keys` set.
        - optional regexes in `key_regexes`.

    Value detection:
        - long high-entropy tokens (len>=16 and entropy>=3.5 bits/char).
        - JWT-like strings (three base64url parts separated by dots).
        - obvious prefixes (e.g., "Bearer <...>").

    You can override redact_value to return custom replacement.
    """

    def __init__(
        self,
        *,
        sensitive_keys: Iterable[str] | None = None,
        key_regexes: Iterable[str] | None = None,
        entropy_threshold_bits_per_char: float = 3.5,
        min_token_len: int = 16,
        replacement: str = "***",
    ) -> None:
        self.sensitive_keys = frozenset((k.lower() for k in (sensitive_keys or _DEFAULT_SENSITIVE_KEYS)))
        self.key_regexes: List[re.Pattern[str]] = [re.compile(r, re.IGNORECASE) for r in (key_regexes or [])]
        self.entropy_threshold = float(entropy_threshold_bits_per_char)
        self.min_token_len = int(min_token_len)
        self.replacement = replacement

        # Precompile text scrub patterns (best-effort; keep cheap).
        self._re_bearer = re.compile(r"(Bearer\s+)([A-Za-z0-9_\-\.=]+)", re.IGNORECASE)
        # naive JWT token matcher
        self._re_jwt = re.compile(r"([A-Za-z0-9_\-]+=*\.){2}[A-Za-z0-9_\-]+=*")
        # common key=value pairs in text
        self._re_keyval = re.compile(
            r"(?P<key>(?:api[_-]?key|token|secret|password))\s*=\s*(?P<val>[^\s;,]+)",
            re.IGNORECASE,
        )

    # ---- Key heuristics ------------------------------------------------------

    def is_sensitive_key(self, key_path: str) -> bool:
        """
        Return True if the key looks sensitive.

        Args:
            key_path: dotted path ("headers.authorization", "db.password").
        """
        key_lower = key_path.split(".")[-1].lower()
        if key_lower in self.sensitive_keys:
            return True
        for rx in self.key_regexes:
            if rx.search(key_path):
                return True
        return False

    # ---- Value heuristics ----------------------------------------------------

    def is_sensitive_value(self, value: Any) -> bool:
        """
        Return True if the value looks like a secret (best-effort).
        """
        if value is None:
            return False
        if isinstance(value, (bool, int, float)):
            return False
        if isinstance(value, (bytes, bytearray)):
            # bytes payloads are not inspected deeply here
            return len(value) >= self.min_token_len
        if not isinstance(value, str):
            return False

        s = value.strip()

        # Too short -> unlikely to be a secret
        if len(s) < self.min_token_len:
            return False

        # JWT-like?
        if self._re_jwt.search(s):
            return True

        # High entropy?
        if _entropy_bits_per_char(s) >= self.entropy_threshold:
            return True

        return False

    # ---- Redaction primitives ------------------------------------------------

    def redact_value(self, value: Any) -> Any:
        """Replace sensitive value with the configured replacement."""
        return self.replacement

    def redact_text(self, text: str) -> str:
        """
        Best-effort scrubbing for free-form text.

        Redacts:
          - 'Bearer <token>' patterns,
          - JWT-like substrings,
          - common 'key=value' occurrences for sensitive keys.
        """
        out = self._re_bearer.sub(r"\1" + self.replacement, text)
        out = self._re_keyval.sub(lambda m: f"{m.group('key')}={self.replacement}", out)
        if self._re_jwt.search(out):
            out = self._re_jwt.sub(self.replacement, out)
        return out


# ---- Object-level redaction --------------------------------------------------


def redact_obj(obj: Any, *, redactor: Redactor, in_place: bool = False, _path: str = "") -> Any:
    """
    Recursively redact mappings/lists/strings using `redactor` policy.

    Rules:
      - If a key is sensitive => the **entire value** is replaced.
      - Else if a value is sensitive by heuristic => value replaced.
      - Strings are scrubbed with `redactor.redact_text()`.

    Args:
        obj: arbitrary Python object (dict/list/tuple/str/...).
        redactor: Redactor instance.
        in_place: when True and `obj` is a mutable mapping/list, mutate in place.

    Returns:
        Redacted object (may be the same instance when `in_place=True`).
    """
    # Mapping
    if isinstance(obj, MutableMapping):
        target: MutableMapping[str, Any]
        if in_place:
            target = obj
        else:
            target = obj.__class__()  # type: ignore[call-arg]
        for k, v in obj.items():
            key_path = f"{_path}.{k}" if _path else str(k)
            if redactor.is_sensitive_key(key_path) or redactor.is_sensitive_value(v):
                tv = redactor.redact_value(v)
            else:
                tv = redact_obj(v, redactor=redactor, in_place=False, _path=key_path)
            target[k] = tv
        return target

    # List/Tuple
    if isinstance(obj, list):
        if in_place:
            for i, v in enumerate(obj):
                obj[i] = redact_obj(v, redactor=redactor, in_place=False, _path=_path)
            return obj
        return [redact_obj(v, redactor=redactor, in_place=False, _path=_path) for v in obj]

    if isinstance(obj, tuple):
        return tuple(redact_obj(v, redactor=redactor, in_place=False, _path=_path) for v in obj)

    # Bytes: do not attempt to parse; redact only if "sensitive"
    if isinstance(obj, (bytes, bytearray)):
        return redactor.redact_value(obj) if redactor.is_sensitive_value(obj) else obj

    # Strings: scrub inline (bearer/jwt/key=val best-effort)
    if isinstance(obj, str):
        return redactor.redact_text(obj)

    # Other types: only heuristic-sensitive values are replaced
    if redactor.is_sensitive_value(obj):
        return redactor.redact_value(obj)

    return obj
