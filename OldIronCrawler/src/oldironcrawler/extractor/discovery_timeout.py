from __future__ import annotations

from concurrent.futures import TimeoutError as FutureTimeoutError
import threading
import time
from typing import Callable

from oldironcrawler.extractor.protocol_runtime import DaemonProbeExecutor
from oldironcrawler.extractor.service_discovery import DiscoverySnapshot

_EXECUTOR_LOCK = threading.Lock()
_EXECUTOR: DaemonProbeExecutor | None = None
_EXECUTOR_LIMIT = 0


def discover_value_snapshot_or_homepage(
    discover_func: Callable[..., DiscoverySnapshot],
    protocol,
    website: str,
    rep_learned: dict[str, int],
    email_learned: dict[str, int],
    *,
    rep_target_count: int,
    contact_target_enabled: bool,
    discovery_deadline_monotonic: float | None,
    discovery_workers: int,
) -> DiscoverySnapshot:
    timeout = _remaining_seconds(discovery_deadline_monotonic)
    if timeout is not None and timeout <= 0:
        return _homepage_snapshot(website)
    future = _get_discovery_executor(discovery_workers).submit(
        discover_func,
        protocol,
        website,
        rep_learned,
        email_learned,
        rep_target_count=rep_target_count,
        contact_target_enabled=contact_target_enabled,
        discovery_deadline_monotonic=discovery_deadline_monotonic,
    )
    try:
        return future.result(timeout=timeout)
    except FutureTimeoutError:
        # 发现阶段到点就降级到首页，后台探测晚返回也不再影响本站结果。
        return _homepage_snapshot(website)


def _homepage_snapshot(website: str) -> DiscoverySnapshot:
    return DiscoverySnapshot(
        urls=[website],
        candidates=[],
        rep_urls=[],
        teacher_pool=[],
        email_urls=[website],
        homepage_html="",
    )


def _remaining_seconds(deadline_monotonic: float | None) -> float | None:
    if deadline_monotonic is None:
        return None
    return max(deadline_monotonic - time.monotonic(), 0.0)


def _get_discovery_executor(limit: int) -> DaemonProbeExecutor:
    normalized = min(max(int(limit or 1), 1), 32)
    old_executor: DaemonProbeExecutor | None = None
    with _EXECUTOR_LOCK:
        global _EXECUTOR
        global _EXECUTOR_LIMIT
        if _EXECUTOR is not None and _EXECUTOR_LIMIT == normalized:
            return _EXECUTOR
        old_executor = _EXECUTOR
        _EXECUTOR = DaemonProbeExecutor(max_workers=normalized)
        _EXECUTOR_LIMIT = normalized
        executor = _EXECUTOR
    if old_executor is not None:
        old_executor.shutdown(wait=False, cancel_futures=False)
    return executor
