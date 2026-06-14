from __future__ import annotations

import queue
import threading
from concurrent.futures import Future
from contextlib import contextmanager
from typing import Any, Callable


class DaemonProbeExecutor:
    def __init__(self, max_workers: int) -> None:
        self._task_queue: queue.Queue[tuple[Future, Callable[..., Any], tuple[Any, ...], dict[str, Any]] | None] = queue.Queue()
        self._workers: list[threading.Thread] = []
        self._lock = threading.Lock()
        self._shutdown = False
        for index in range(max(max_workers, 1)):
            worker = threading.Thread(target=self._run_worker, name=f"oldiron-probe-{index + 1}", daemon=True)
            worker.start()
            self._workers.append(worker)

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        future: Future = Future()
        with self._lock:
            if self._shutdown:
                raise RuntimeError("probe_executor_shutdown")
            self._task_queue.put((future, fn, args, kwargs))
        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        with self._lock:
            self._shutdown = True
            if cancel_futures:
                self._cancel_pending_tasks()
            for _worker in self._workers:
                self._task_queue.put(None)
        if wait:
            for worker in self._workers:
                worker.join()

    def _cancel_pending_tasks(self) -> None:
        while True:
            try:
                task = self._task_queue.get_nowait()
            except queue.Empty:
                return
            if task is not None:
                future, _fn, _args, _kwargs = task
                future.cancel()
            self._task_queue.task_done()

    def _run_worker(self) -> None:
        while True:
            task = self._task_queue.get()
            try:
                if task is None:
                    return
                future, fn, args, kwargs = task
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    future.set_result(fn(*args, **kwargs))
                except BaseException as exc:  # noqa: BLE001
                    future.set_exception(exc)
            finally:
                self._task_queue.task_done()

_DEFAULT_LIMIT = 8
_PROBE_EXECUTOR: DaemonProbeExecutor | None = None
_PROBE_EXECUTOR_LIMIT = 0
_PROBE_EXECUTOR_LOCK = threading.Lock()
_REQUEST_SLOT_SEMAPHORE: threading.BoundedSemaphore | None = None
_REQUEST_SLOT_LIMIT = 0
_REQUEST_SLOT_LOCK = threading.Lock()


def configure_protocol_runtime(*, probe_workers: int, request_slots: int) -> None:
    _set_probe_executor_limit(probe_workers)
    _set_request_slot_limit(request_slots)


def get_probe_executor() -> DaemonProbeExecutor:
    with _PROBE_EXECUTOR_LOCK:
        global _PROBE_EXECUTOR
        global _PROBE_EXECUTOR_LIMIT
        if _PROBE_EXECUTOR is None:
            _PROBE_EXECUTOR = DaemonProbeExecutor(max_workers=_DEFAULT_LIMIT)
            _PROBE_EXECUTOR_LIMIT = _DEFAULT_LIMIT
        return _PROBE_EXECUTOR


@contextmanager
def request_slot(*, timeout_seconds: float | None = None, wait_timeout_seconds: float | None = None):
    semaphore = _get_request_slot_semaphore()
    wait_timeout_source = timeout_seconds if wait_timeout_seconds is None else wait_timeout_seconds
    wait_timeout = None if wait_timeout_source is None else max(wait_timeout_source, 0.01)
    acquired = semaphore.acquire(timeout=wait_timeout)
    if not acquired:
        raise RuntimeError("request_slot_timeout")
    try:
        yield
    finally:
        semaphore.release()


def _set_probe_executor_limit(limit: int) -> None:
    bounded = max(int(limit or 0), 1)
    old_executor: DaemonProbeExecutor | None = None
    with _PROBE_EXECUTOR_LOCK:
        global _PROBE_EXECUTOR
        global _PROBE_EXECUTOR_LIMIT
        if _PROBE_EXECUTOR is not None and _PROBE_EXECUTOR_LIMIT == bounded:
            return
        old_executor = _PROBE_EXECUTOR
        _PROBE_EXECUTOR = DaemonProbeExecutor(max_workers=bounded)
        _PROBE_EXECUTOR_LIMIT = bounded
    if old_executor is not None:
        old_executor.shutdown(wait=False, cancel_futures=False)


def _set_request_slot_limit(limit: int) -> None:
    bounded = max(int(limit or 0), 1)
    with _REQUEST_SLOT_LOCK:
        global _REQUEST_SLOT_SEMAPHORE
        global _REQUEST_SLOT_LIMIT
        if _REQUEST_SLOT_SEMAPHORE is not None and _REQUEST_SLOT_LIMIT == bounded:
            return
        _REQUEST_SLOT_SEMAPHORE = threading.BoundedSemaphore(bounded)
        _REQUEST_SLOT_LIMIT = bounded


def _get_request_slot_semaphore() -> threading.BoundedSemaphore:
    with _REQUEST_SLOT_LOCK:
        global _REQUEST_SLOT_SEMAPHORE
        global _REQUEST_SLOT_LIMIT
        if _REQUEST_SLOT_SEMAPHORE is None:
            _REQUEST_SLOT_SEMAPHORE = threading.BoundedSemaphore(_DEFAULT_LIMIT)
            _REQUEST_SLOT_LIMIT = _DEFAULT_LIMIT
        return _REQUEST_SLOT_SEMAPHORE
