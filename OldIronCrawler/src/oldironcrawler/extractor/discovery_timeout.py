from __future__ import annotations

from concurrent.futures import Future, TimeoutError as FutureTimeoutError
import time
from typing import Callable

from oldironcrawler.extractor.protocol_runtime import DaemonProbeExecutor
from oldironcrawler.extractor.service_discovery import DiscoverySnapshot


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
    _ = discovery_workers
    executor = DaemonProbeExecutor(max_workers=1)
    future = executor.submit(
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
        return _await_discovery_future(future, discovery_deadline_monotonic)
    except FutureTimeoutError:
        future.cancel()
        # 发现阶段到点就降级到首页，慢站后台任务不能拖住本站结果。
        return _homepage_snapshot(website)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


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


def _await_discovery_future(future: Future, deadline_monotonic: float | None) -> DiscoverySnapshot:
    while True:
        if future.done():
            return future.result(timeout=0)
        remaining = _remaining_seconds(deadline_monotonic)
        if remaining is not None and remaining <= 0:
            raise FutureTimeoutError()
        sleep_seconds = 0.02 if remaining is None else min(remaining, 0.02)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
