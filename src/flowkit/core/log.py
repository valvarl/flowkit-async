from __future__ import annotations

import contextvars
import json
import logging
import os
import sys
import threading
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar, Final

__all__ = [
    "bind_context",
    "configure_from_env",
    "disable_stdout_logging",
    "enable_stdout_logging",
    "get_logger",
    "log_context",
    "set_level",
    "swallow",
    "warn_once",
]

# ---------- Context ----------

_log_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("flowkit_log_ctx", default=None)


def _ctx_copy() -> dict[str, Any]:
    ctx = _log_context.get()
    # shallow copy is enough; we only merge scalars/small dicts
    return dict(ctx) if ctx else {}


def bind_context(**fields: Any) -> None:
    """
    Merge fields into the current structured log context.
    Use from long-lived code (e.g., at worker init or per task).
    """
    ctx = _ctx_copy()
    ctx.update({k: v for k, v in fields.items() if v is not None})
    _log_context.set(ctx)


@contextmanager
def log_context(**fields: Any):
    """
    Temporarily add fields to the structured log context.
    Restores the previous context on exit.
    """
    token = _log_context.set({**_ctx_copy(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _log_context.reset(token)


# ---------- JSON formatter ----------

_STD_ATTRS: Final[frozenset[str]] = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "asctime",
    }
)


def _iso_utc_ms(ts: float) -> str:
    # RFC3339 / ISO8601 with milliseconds, UTC
    dt = datetime.fromtimestamp(ts, tz=UTC)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class JsonFormatter(logging.Formatter):
    """
    Minimal, fast JSON formatter. Merges:
      - ts, level, logger
      - message and event (if provided via extra={'event': ...})
      - contextvars (task_id, node_id, worker_id, etc.)
      - extra=... fields (non-standard attributes)
      - exception info when present
    """

    def __init__(self, *, include_stack: bool = False) -> None:
        super().__init__()
        self.include_stack = include_stack

    def format(self, record: logging.LogRecord) -> str:
        # Base envelope
        out: dict[str, Any] = {
            "ts": _iso_utc_ms(record.created),
            "level": record.levelname,
            "logger": record.name,
        }

        # Merge message
        msg = record.getMessage()
        if msg:
            out["message"] = msg

        # Context from contextvars
        ctx = _log_context.get()
        if ctx:
            out.update(ctx)

        # Non-standard extras
        for k, v in record.__dict__.items():
            if k in _STD_ATTRS:
                continue
            # avoid overwriting context or core keys unless explicit
            if k not in out:
                out[k] = v

        # Exception / stack
        if record.exc_info:
            exc_type = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
            out.setdefault("error", {})
            out["error"].update(
                {
                    "type": exc_type,
                    "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                }
            )
            if self.include_stack:
                out["error"]["stack"] = self.formatException(record.exc_info)
        elif record.exc_text:
            out.setdefault("error", {})
            out["error"]["stack"] = record.exc_text

        return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


class HumanFormatter(logging.Formatter):
    """
    Compact human-friendly formatter for local debugging.
    """

    default_time_format = "%Y-%m-%d %H:%M:%S"
    default_msec_format = "%s.%03d"

    def format(self, record: logging.LogRecord) -> str:
        # Timestamp and header
        s = f"{self.formatTime(record)} {record.levelname:<5} {record.name}: {record.getMessage()}"
        # Selected context shortcuts
        ctx = _log_context.get()
        if ctx:
            keys = ("task_id", "node_id", "attempt_epoch", "worker_id")
            compact = {k: ctx.get(k) for k in keys if ctx.get(k) is not None}
            if compact:
                parts = ", ".join(f"{k}={v}" for k, v in compact.items())
                s += f"  [{parts}]"
        if record.exc_info:
            s += "\n" + self.formatException(record.exc_info)
        return s


# ---------- Filters / helpers ----------


class ContextFilter(logging.Filter):
    """
    Inject current contextvars into the log record so handlers can route on them
    if needed (e.g., file-per-task). Non-destructive.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _log_context.get()
        if ctx:
            for k, v in ctx.items():
                if k not in record.__dict__:
                    record.__dict__[k] = v
        return True


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


class _MinLevelFilter(logging.Filter):
    def __init__(self, min_level: int) -> None:
        self.min_level = min_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= self.min_level


class _KwExtraAdapter(logging.LoggerAdapter):
    """
    Logger adapter that moves any unknown kwargs into `extra={...}` so you can write:
        log.info("msg", event="...", task_id=..., node_id=...)
    without TypeError from logging.
    """

    _allowed_passthrough: ClassVar[frozenset[str]] = frozenset({"exc_info", "stack_info", "stacklevel", "extra"})
    _logrecord_attrs: ClassVar[frozenset[str]] = _STD_ATTRS

    def process(self, msg, kwargs):
        extra = kwargs.get("extra")
        if extra is None or not isinstance(extra, dict):
            extra = {}

        moved = {}
        for k in list(kwargs.keys()):
            if k in self._allowed_passthrough:
                continue
            moved[k] = kwargs.pop(k)

        for k, v in moved.items():
            key = k
            if key in self._logrecord_attrs:
                key = f"field_{key}"
            if key not in extra:
                extra[key] = v

        kwargs["extra"] = extra
        return msg, kwargs


# One-shot warning registry (per-process). Thread-safe.
_WARN_ONCE_SEEN: set[str] = set()
_WARN_ONCE_LOCK = threading.Lock()


def warn_once(
    logger: logging.Logger | logging.LoggerAdapter,
    code: str,
    msg: str,
    *,
    level: int = logging.WARNING,
    **extra: Any,
) -> None:
    """
    Log a warning (or any level) only once per process for the given code.
    """
    with _WARN_ONCE_LOCK:
        if code in _WARN_ONCE_SEEN:
            return
        _WARN_ONCE_SEEN.add(code)
    adapter: logging.LoggerAdapter
    if isinstance(logger, logging.LoggerAdapter):
        adapter = logger
    else:
        adapter = _KwExtraAdapter(logger, {})
    adapter.log(level, msg, code=code, **extra)


# ---------- Public configuration API ----------

_FLOWKIT_LOGGER_NAME = "flowkit"
_configured = False
_stdout_handler_key = "_flowkit_stdout_handler"
_stderr_handler_key = "_flowkit_stderr_handler"


def get_logger(name: str | None = None) -> logging.LoggerAdapter:
    """
    Return a namespaced logger adapter that accepts arbitrary keyword fields
    (they will be placed into LogRecord.extra). Silent by default (NullHandler).
    """
    _bootstrap_minimal()
    base = logging.getLogger(_FLOWKIT_LOGGER_NAME)
    target = base.getChild(name) if name else base
    return _KwExtraAdapter(target, {})


def _bootstrap_minimal() -> None:
    """
    Install a NullHandler and a context filter so importing library code is silent by default.
    """
    global _configured
    if _configured:
        return
    lg = logging.getLogger(_FLOWKIT_LOGGER_NAME)
    lg.setLevel(logging.DEBUG)  # debug by default
    # Avoid duplicate handlers on re-imports
    if not any(isinstance(h, logging.NullHandler) for h in lg.handlers):
        lg.addHandler(logging.NullHandler())
    if not any(isinstance(f, ContextFilter) for f in lg.filters):
        lg.addFilter(ContextFilter())
    _configured = True


def set_level(level: int | str) -> None:
    """
    Change library logger level at runtime (affects children).
    """
    if isinstance(level, str):
        level = logging.getLevelName(level.upper())
        if isinstance(level, str):  # unknown name -> numeric not resolved
            raise ValueError("Invalid level name")
    logging.getLogger(_FLOWKIT_LOGGER_NAME).setLevel(level)


def enable_stdout_logging(
    *,
    level: int | str = logging.DEBUG,
    json_output: bool = True,
    include_stack: bool = False,
    pretty: bool = False,
    route_errors_to_stderr: bool = False,
) -> None:
    """
    Attach stream handlers for tests/local/containers.
    - json_output=True -> JsonFormatter; pretty=True -> HumanFormatter
    - route_errors_to_stderr=True -> ERROR+ to stderr, others to stdout
    """
    if isinstance(level, str):
        lvl = logging.getLevelName(level.upper())
        if isinstance(lvl, str):
            raise ValueError("Invalid level name")
        level = lvl

    _bootstrap_minimal()
    lg = logging.getLogger(_FLOWKIT_LOGGER_NAME)

    # Clear previous stream handlers
    disable_stdout_logging()

    # Choose formatter
    fmt: logging.Formatter
    if pretty:
        fmt = HumanFormatter()
    elif json_output:
        fmt = JsonFormatter(include_stack=include_stack)
    else:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    if route_errors_to_stderr:
        # stdout: <= WARNING
        h_out = logging.StreamHandler(sys.stdout)
        h_out.set_name(_stdout_handler_key)
        h_out.setLevel(level)
        h_out.addFilter(_MaxLevelFilter(logging.WARNING))
        h_out.setFormatter(fmt)
        lg.addHandler(h_out)

        # stderr: >= ERROR
        h_err = logging.StreamHandler(sys.stderr)
        h_err.set_name(_stderr_handler_key)
        h_err.setLevel(max(level, logging.ERROR))
        h_err.addFilter(_MinLevelFilter(logging.ERROR))
        h_err.setFormatter(fmt)
        lg.addHandler(h_err)
    else:
        h = logging.StreamHandler(sys.stdout)
        h.set_name(_stdout_handler_key)
        h.setLevel(level)
        h.setFormatter(fmt)
        lg.addHandler(h)


def disable_stdout_logging() -> None:
    """
    Detach previously installed stdout/stderr handlers, if present.
    """
    lg = logging.getLogger(_FLOWKIT_LOGGER_NAME)
    for h in list(lg.handlers):
        if h.get_name() in (_stdout_handler_key, _stderr_handler_key):
            lg.removeHandler(h)


def configure_from_env() -> None:
    """
    Optional: call once in application entrypoints/tests.
    Honors:
      - FLOWKIT_LOG_STDOUT=1|true -> enable stdout
      - FLOWKIT_LOG_LEVEL=DEBUG|INFO|...
      - FLOWKIT_LOG_PRETTY=1 -> human formatter instead of JSON
      - FLOWKIT_LOG_STACK=1 -> include stack in JSON logs
    """
    want_stdout = os.getenv("FLOWKIT_LOG_STDOUT", "").lower() in ("1", "true", "yes", "on")
    level = os.getenv("FLOWKIT_LOG_LEVEL", "DEBUG")
    pretty = os.getenv("FLOWKIT_LOG_PRETTY", "").lower() in ("1", "true", "yes", "on")
    with_stack = os.getenv("FLOWKIT_LOG_STACK", "").lower() in ("1", "true", "yes", "on")

    _bootstrap_minimal()
    set_level(level)

    if want_stdout:
        enable_stdout_logging(level=level, json_output=not pretty, include_stack=with_stack, pretty=pretty)
    else:
        # Ensure no stdout handler lingers
        disable_stdout_logging()


# ---------- Exception swallowing with trace ----------


@contextmanager
def swallow(
    *,
    logger: logging.Logger | logging.LoggerAdapter | None = None,
    level: int = logging.DEBUG,
    code: str,
    msg: str | None = None,
    reraise: bool = False,
    return_value: Any = None,
    extra: Mapping[str, Any] | None = None,
    expected: bool = True,
):
    """
    Context manager to replace `try/except: pass` with structured logging.
    Example:
        with swallow(logger=log, code="db.index.ensure", msg="Index creation failed"):
            await db.create_index(...)
    If reraise=True, exception is rethrown after logging.
    """
    base_logger = logger or get_logger("swallow")
    if isinstance(base_logger, logging.LoggerAdapter):
        log_adapter = base_logger
    else:
        log_adapter = _KwExtraAdapter(base_logger, {})
    try:
        yield
    except Exception as e:
        payload: dict[str, Any] = {"code": code, "expected": expected}
        if extra:
            payload.update(dict(extra))
        log_adapter.log(level, msg or "Suppressed exception", exc_info=e, **payload)
        if reraise:
            raise
        return return_value


# Initialize minimal config on import (silent by default).
_bootstrap_minimal()
# Do NOT auto-enable stdout here; tests/apps may call configure_from_env().
