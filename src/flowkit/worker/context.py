from __future__ import annotations
import asyncio
import os
import signal
import subprocess as subproc
from typing import Any, Callable, Dict, List, Optional

from ..core.time import Clock


class RunContext:
    """
    Cancellation-aware runtime utilities for handlers:
    - shared cancel flag + meta (reason, deadline ts)
    - cancellable awaits
    - subprocess group termination with escalation
    - resource cleanup registry
    """
    def __init__(
        self,
        *,
        cancel_flag: asyncio.Event,
        cancel_meta: Dict[str, Any],
        artifacts_writer,
        clock: Clock,
        task_id: str,
        node_id: str,
        attempt_epoch: int,
        worker_id: str,
    ):
        self._cancel_flag = cancel_flag
        self._cancel_meta = cancel_meta
        self.artifacts = artifacts_writer
        self.clock = clock

        self.task_id = task_id
        self.node_id = node_id
        self.attempt_epoch = attempt_epoch
        self.worker_id = worker_id

        self.kv: Dict[str, Any] = {}
        self._cleanup_callbacks: List[Callable[[], Any]] = []
        self._subprocesses: List[Any] = []
        self._temp_paths: List[str] = []

    # ---- cancellation
    def cancelled(self) -> bool:
        return self._cancel_flag.is_set()

    @property
    def cancel_reason(self) -> Optional[str]:
        return self._cancel_meta.get("reason")

    @property
    def cancel_deadline_ts_ms(self) -> Optional[int]:
        return self._cancel_meta.get("deadline_ts_ms")

    def remaining_ms(self) -> Optional[float]:
        dl = self.cancel_deadline_ts_ms
        if not dl:
            return None
        return max(0.0, float(dl - self.clock.now_ms()))

    async def raise_if_cancelled(self) -> None:
        if self._cancel_flag.is_set():
            raise asyncio.CancelledError()

    async def cancellable(self, coro: Any):
        if self._cancel_flag.is_set():
            raise asyncio.CancelledError()
        task = asyncio.create_task(coro)
        done, pending = await asyncio.wait(
            {task, asyncio.create_task(self._cancel_flag.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            try:
                return await task
            finally:
                for p in pending:
                    p.cancel()
        else:
            task.cancel()
            try:
                await task
            except Exception:
                pass
            raise asyncio.CancelledError()

    # ---- subprocess helpers
    async def run_subprocess(self, *cmd: str, grace_ms: int = 5000) -> int:
        kwargs: Dict[str, Any] = {}
        if os.name == "posix":
            kwargs["preexec_fn"] = os.setsid
        else:
            kwargs["creationflags"] = getattr(subproc, "CREATE_NEW_PROCESS_GROUP", 0)

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, **kwargs
        )
        self.register_subprocess(proc)

        try:
            await self.cancellable(proc.wait())
            return proc.returncode or 0
        except asyncio.CancelledError:
            try:
                await self._terminate_process(proc, grace_ms=grace_ms)
            finally:
                raise

    def register_subprocess(self, proc: Any) -> None:
        self._subprocesses.append(proc)

    def register_temp_path(self, path: str) -> None:
        self._temp_paths.append(path)

    def on_cleanup(self, cb: Callable[[], Any]) -> None:
        self._cleanup_callbacks.append(cb)

    async def _terminate_process(self, proc: Any, *, grace_ms: int = 5000) -> None:
        try:
            if proc.returncode is not None:
                return
            if os.name == "posix":
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except Exception:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
            else:
                try:
                    proc.send_signal(getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM))
                except Exception:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=max(0.001, grace_ms / 1000.0))
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except Exception:
                    pass
        except Exception:
            pass

    async def _cleanup_resources(self) -> None:
        for cb in reversed(self._cleanup_callbacks):
            try:
                res = cb()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
        for proc in list(self._subprocesses):
            try:
                await self._terminate_process(proc, grace_ms=1000)
            except Exception:
                pass
        self._subprocesses.clear()

        for p in list(self._temp_paths):
            try:
                if os.path.isdir(p):
                    for root, dirs, files in os.walk(p, topdown=False):
                        for f in files:
                            try:
                                os.remove(os.path.join(root, f))
                            except Exception:
                                pass
                        for d in dirs:
                            try:
                                os.rmdir(os.path.join(root, d))
                            except Exception:
                                pass
                    os.rmdir(p)
                elif os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        self._temp_paths.clear()
