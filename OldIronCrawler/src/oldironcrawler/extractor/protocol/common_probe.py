from __future__ import annotations

import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, wait

from oldironcrawler.extractor.protocol.budget import (
    COMMON_PROBE_BATCH_WAIT_CAP_SECONDS as _COMMON_PROBE_BATCH_WAIT_CAP_SECONDS,
    COMMON_PROBE_REQUEST_TIMEOUT_SECONDS as _COMMON_PROBE_REQUEST_TIMEOUT_SECONDS,
    COMMON_PROBE_SLOT_WAIT_SECONDS as _COMMON_PROBE_SLOT_WAIT_SECONDS,
    COMMON_PROBE_TOTAL_WAIT_CAP_SECONDS as _COMMON_PROBE_TOTAL_WAIT_CAP_SECONDS,
)
from oldironcrawler.extractor.protocol.errors import ProtocolPermanentError
from oldironcrawler.extractor.protocol.types import HtmlPage
from oldironcrawler.extractor.protocol_discovery import build_common_probe_urls as _build_common_probe_urls
from oldironcrawler.extractor.protocol_discovery import merge_unique_urls as _merge_unique_urls
from oldironcrawler.extractor.protocol_runtime import get_probe_executor


def reset_prefetched_value_pages(client: object) -> None:
    lock = _get_prefetched_value_pages_lock(client)
    with lock:
        setattr(client, "_prefetched_value_pages", {})


def get_prefetched_value_pages(client: object) -> list[HtmlPage]:
    lock = _get_prefetched_value_pages_lock(client)
    with lock:
        pages = getattr(client, "_prefetched_value_pages", {})
        if not isinstance(pages, dict):
            return []
        return list(pages.values())


def probe_common_value_urls(client: object, _session: object, start_url: str, *, limit: int) -> list[str]:
    return probe_common_value_urls_with_hooks(
        client,
        start_url,
        limit=limit,
        build_probe_urls=_build_common_probe_urls,
        total_wait_cap_seconds=_COMMON_PROBE_TOTAL_WAIT_CAP_SECONDS,
    )


def probe_common_value_urls_with_hooks(
    client: object,
    start_url: str,
    *,
    limit: int,
    build_probe_urls,
    total_wait_cap_seconds: float,
) -> list[str]:
    probe_urls = build_probe_urls(start_url)
    if not probe_urls:
        return []
    result: list[str] = []
    config = getattr(client, "_config")
    probe_target = min(max(config.common_probe_target, 1), max(limit, 1), len(probe_urls))
    batch_size = min(max(config.common_probe_concurrency, 1), len(probe_urls))
    start_index = 0
    empty_batches = 0
    scan_deadline = resolve_common_probe_scan_deadline(client, total_wait_cap_seconds=total_wait_cap_seconds)
    while start_index < len(probe_urls) and len(result) < probe_target:
        if time.monotonic() >= scan_deadline:
            break
        batch = probe_urls[start_index : start_index + batch_size]
        start_index += batch_size
        batch_hits = client._probe_common_value_batch(batch, scan_deadline_monotonic=scan_deadline)
        result = _merge_unique_urls(result, batch_hits, limit=probe_target)
        empty_batches = 0 if batch_hits else empty_batches + 1
        if should_stop_common_probe_scan(
            client,
            batch_count=max(start_index // max(batch_size, 1), 1),
            hit_count=len(result),
            empty_batches=empty_batches,
        ):
            break
    return result


def probe_common_value_batch(
    client: object,
    probe_urls: list[str],
    *,
    scan_deadline_monotonic: float | None = None,
) -> list[str]:
    return probe_common_value_batch_with_hooks(
        client,
        probe_urls,
        scan_deadline_monotonic=scan_deadline_monotonic,
        executor_factory=get_probe_executor,
        wait_func=wait,
    )


def probe_common_value_batch_with_hooks(
    client: object,
    probe_urls: list[str],
    *,
    scan_deadline_monotonic: float | None,
    executor_factory,
    wait_func,
) -> list[str]:
    if not probe_urls:
        return []
    config = getattr(client, "_config")
    futures: dict[Future, str] = {}
    results: list[str] = []
    batch_timeout = min(config.timeout_seconds, _COMMON_PROBE_BATCH_WAIT_CAP_SECONDS)
    wait_deadline = time.monotonic() + client._resolve_timeout(batch_timeout)
    if scan_deadline_monotonic is not None:
        wait_deadline = min(wait_deadline, scan_deadline_monotonic)
    for probe_url in probe_urls:
        futures[executor_factory().submit(client._probe_common_value_url, probe_url)] = probe_url
    while futures:
        remaining = wait_deadline - time.monotonic()
        if remaining <= 0:
            break
        done, _ = wait_func(futures.keys(), timeout=remaining, return_when=FIRST_COMPLETED)
        if not done:
            break
        for future in done:
            futures.pop(future, None)
            try:
                keep = future.result()
            except Exception:  # noqa: BLE001
                continue
            if keep:
                results.append(str(keep))
    for future in futures:
        future.cancel()
    return results


def resolve_common_probe_scan_deadline(
    client: object,
    *,
    total_wait_cap_seconds: float = _COMMON_PROBE_TOTAL_WAIT_CAP_SECONDS,
) -> float:
    budget_deadline = time.monotonic() + total_wait_cap_seconds
    config_deadline = getattr(client, "_config").deadline_monotonic
    if config_deadline is None:
        return budget_deadline
    return min(config_deadline, budget_deadline)


def probe_common_value_url(client: object, probe_url: str) -> str | None:
    session = client._get_or_create_session()
    config = getattr(client, "_config")
    request_deadline = time.monotonic() + min(config.timeout_seconds, _COMMON_PROBE_REQUEST_TIMEOUT_SECONDS)
    try:
        html_text = client._fetch_html(
            session,
            probe_url,
            required=False,
            timeout_seconds=min(config.timeout_seconds, _COMMON_PROBE_REQUEST_TIMEOUT_SECONDS),
            max_retries_override=0,
            request_slot_wait_seconds=_COMMON_PROBE_SLOT_WAIT_SECONDS,
            request_deadline_monotonic=request_deadline,
            allow_httpx_fallback=False,
            use_request_slot=True,
        )
    except ProtocolPermanentError:
        return None
    if not html_text.strip():
        return None
    _record_prefetched_value_page(client, HtmlPage(url=probe_url, html=html_text))
    return probe_url


def has_enough_discovery_hits(client: object, urls: list[str]) -> bool:
    return len(urls) >= max(getattr(client, "_config").common_probe_target, 1)


def should_stop_common_probe_scan(client: object, *, batch_count: int, hit_count: int, empty_batches: int) -> bool:
    config = getattr(client, "_config")
    if hit_count >= max(config.common_probe_target, 1):
        return True
    if empty_batches >= max(config.common_probe_patience_batches, 1):
        return True
    if batch_count < max(config.common_probe_patience_batches, 1):
        return False
    return hit_count < max(config.common_probe_min_hits_after_patience, 1)


def _record_prefetched_value_page(client: object, page: HtmlPage) -> None:
    if not page.url or not page.html.strip():
        return
    lock = _get_prefetched_value_pages_lock(client)
    with lock:
        pages = getattr(client, "_prefetched_value_pages", {})
        if not isinstance(pages, dict):
            pages = {}
        pages[page.url] = page
        setattr(client, "_prefetched_value_pages", pages)


def _get_prefetched_value_pages_lock(client: object) -> threading.Lock:
    lock = getattr(client, "_prefetched_value_pages_lock", None)
    if lock is None:
        lock = threading.Lock()
        setattr(client, "_prefetched_value_pages_lock", lock)
    return lock
