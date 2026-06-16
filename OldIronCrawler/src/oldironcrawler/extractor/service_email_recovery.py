from __future__ import annotations

import time
from types import SimpleNamespace
from urllib.parse import urlparse

from oldironcrawler.extractor.discovery_fallback import _select_common_email_probe_urls
from oldironcrawler.extractor.email_rules import collect_emails_for_pages
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.phone_rules import collect_phones_for_pages
from oldironcrawler.extractor.protocol_client import SiteProtocolClient
from oldironcrawler.extractor.service_discovery import (
    _collect_primary_email_rule_pages,
    _collect_email_rule_pages,
    _fetch_email_recovery_pages,
    _fetch_primary_pages,
    _merge_pages_into_map,
    _select_unfetched_primary_urls,
)
from oldironcrawler.extractor.shell_page import replace_shell_pages_with_evidence
from oldironcrawler.runtime.store import SiteStageMetrics

_COMMON_RECOVERY_PROBE_LIMIT = 8
_COMMON_RECOVERY_FIRST_BATCH = 2
_COMMON_RECOVERY_BATCH_SIZE = 4
_SLOW_EMPTY_INITIAL_FETCH_RECOVERY_CUTOFF_MS = 8000


def fetch_fast_common_email_pages_if_needed(
    protocol: SiteProtocolClient,
    website: str,
    discovered_urls: list[str],
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    *,
    cascade_email_primary: bool,
    collect_email_enabled: bool,
    proxy_url: str,
    timeout_seconds: float,
    deadline_monotonic: float | None,
) -> tuple[int, bool]:
    if not cascade_email_primary or not collect_email_enabled:
        return 0, False
    if not _should_try_common_recovery_first(website):
        return 0, False
    if _page_map_has_rule_emails(website, page_map, fetch_plan):
        return 0, False
    pages, elapsed_ms = _fetch_common_probe_recovery_pages(protocol, website, discovered_urls)
    if not pages:
        return elapsed_ms, False
    _merge_pages_into_map(page_map, pages)
    fast_common_urls = [page.url for page in pages]
    elapsed_ms += replace_shell_pages_with_evidence(
        page_map,
        fast_common_urls,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
        deadline_monotonic=deadline_monotonic,
    )
    return elapsed_ms, _page_map_has_rule_emails(website, page_map, fetch_plan)


def fetch_initial_common_email_pages_if_needed(
    protocol: SiteProtocolClient,
    website: str,
    discovered_urls: list[str],
) -> tuple[list, int]:
    return _fetch_common_probe_recovery_pages(protocol, website, discovered_urls)


def fetch_initial_primary_pages_with_recovery(
    protocol: SiteProtocolClient,
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    primary_urls: list[str],
    *,
    page_concurrency: int,
    page_pool: PageFetchPool | None,
    cascade_email_primary: bool,
    website: str = "",
    discovered_urls: list[str] | None = None,
    fetch_primary_pages_func=_fetch_primary_pages,
    select_unfetched_primary_urls_func=_select_unfetched_primary_urls,
) -> tuple[list, int]:
    if not primary_urls:
        return [], 0
    started = time.monotonic()
    try:
        return fetch_primary_pages_func(
            protocol,
            primary_urls,
            page_concurrency=page_concurrency,
            page_pool=page_pool,
        )
    except Exception as exc:  # noqa: BLE001
        elapsed_ms = int(round((time.monotonic() - started) * 1000))
        should_recover_primary = _should_recover_initial_primary_fetch(
            exc,
            fetch_plan,
            page_map,
            primary_urls,
            cascade_email_primary=cascade_email_primary,
            elapsed_ms=elapsed_ms,
            select_unfetched_primary_urls_func=select_unfetched_primary_urls_func,
        )
        should_try_common = (
            cascade_email_primary
            and bool(str(website or "").strip())
            and _is_recoverable_initial_fetch_error(exc)
            and not _is_slow_empty_initial_fetch_timeout(exc, elapsed_ms)
        )
        if not should_recover_primary and not should_try_common:
            raise
        fallback_pages, fallback_ms = _fetch_initial_fallback_pages(
            protocol,
            fetch_plan,
            page_map,
            primary_urls,
            page_concurrency=page_concurrency,
            page_pool=page_pool,
            should_recover_primary=should_recover_primary,
            elapsed_ms=elapsed_ms,
            started_monotonic=started,
            fetch_primary_pages_func=fetch_primary_pages_func,
            select_unfetched_primary_urls_func=select_unfetched_primary_urls_func,
        )
        if fallback_pages:
            return fallback_pages, elapsed_ms + fallback_ms
        if should_try_common and not page_map:
            common_pages, common_ms = fetch_initial_common_email_pages_if_needed(
                protocol,
                website,
                list(discovered_urls or []),
            )
            if common_pages:
                return common_pages, elapsed_ms + max(fallback_ms, 0) + common_ms
        if page_map:
            return [], elapsed_ms + max(fallback_ms, 0)
        if should_recover_primary and _select_initial_fallback_urls(
            fetch_plan,
            page_map,
            primary_urls,
            select_unfetched_primary_urls_func=select_unfetched_primary_urls_func,
        ):
            raise
        return fallback_pages, elapsed_ms + fallback_ms


def recover_email_pages_if_needed(
    protocol: SiteProtocolClient,
    website: str,
    discovered_urls: list[str],
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    metrics: SiteStageMetrics,
    emails: list[str],
    email_sources: dict[str, list[str]],
    phones: list[str],
    email_rule_pages: list[tuple[str, str]],
    page_concurrency: int,
    page_pool: PageFetchPool | None,
    collect_email_enabled: bool,
    collect_phone_enabled: bool,
) -> tuple[list[str], dict[str, list[str]], list[str], list[tuple[str, str]]]:
    if not collect_email_enabled or emails:
        return emails, email_sources, phones, email_rule_pages
    if _should_try_common_recovery_first(website):
        emails, email_sources, phones, email_rule_pages = _recover_from_common_probe_pages(
            protocol,
            website,
            discovered_urls,
            fetch_plan,
            page_map,
            metrics,
            collect_phone_enabled=collect_phone_enabled,
        )
        if emails:
            return emails, email_sources, phones, email_rule_pages
    recovery_pages, recovery_fetch_ms = _fetch_email_recovery_pages(
        protocol,
        website,
        discovered_urls,
        fetch_plan,
        page_map,
        page_concurrency=page_concurrency,
        page_pool=page_pool,
        primary_fetch_ms=metrics.fetch_pages_ms,
    )
    if recovery_pages:
        metrics.fetch_pages_ms += recovery_fetch_ms
        _merge_pages_into_map(page_map, recovery_pages)
        metrics.fetched_page_count = len(page_map)
        emails, email_sources, phones, email_rule_pages = _collect_recovered_contact_details(
            website,
            page_map,
            fetch_plan,
            metrics,
            collect_phone_enabled=collect_phone_enabled,
        )
    if emails:
        return emails, email_sources, phones, email_rule_pages
    if _should_try_common_recovery_first(website):
        return emails, email_sources, phones, email_rule_pages
    return _recover_from_common_probe_pages(
        protocol,
        website,
        discovered_urls,
        fetch_plan,
        page_map,
        metrics,
        collect_phone_enabled=collect_phone_enabled,
    )


def _should_try_common_recovery_first(website: str) -> bool:
    host = (urlparse(str(website or "").strip()).netloc or "").lower()
    return host.endswith(".jp") or ".co.jp" in host


def _fetch_initial_fallback_pages(
    protocol: SiteProtocolClient,
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    primary_urls: list[str],
    *,
    page_concurrency: int,
    page_pool: PageFetchPool | None,
    should_recover_primary: bool,
    elapsed_ms: int,
    started_monotonic: float,
    fetch_primary_pages_func,
    select_unfetched_primary_urls_func,
) -> tuple[list, int]:
    fallback_urls = _select_initial_fallback_urls(
        fetch_plan,
        page_map,
        primary_urls,
        select_unfetched_primary_urls_func=select_unfetched_primary_urls_func,
    )
    if not should_recover_primary or not fallback_urls:
        return [], 0
    try:
        return fetch_primary_pages_func(
            protocol,
            fallback_urls,
            page_concurrency=page_concurrency,
            page_pool=page_pool,
        )
    except Exception:  # noqa: BLE001
        fallback_ms = int(round((time.monotonic() - started_monotonic) * 1000)) - elapsed_ms
        return [], max(fallback_ms, 0)


def _select_initial_fallback_urls(
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    primary_urls: list[str],
    *,
    select_unfetched_primary_urls_func=_select_unfetched_primary_urls,
) -> list[str]:
    failed_urls = set(primary_urls)
    return [
        url for url in select_unfetched_primary_urls_func(fetch_plan, page_map)
        if url not in failed_urls
    ]


def _should_recover_initial_primary_fetch(
    exc: Exception,
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    primary_urls: list[str],
    *,
    cascade_email_primary: bool,
    elapsed_ms: int = 0,
    select_unfetched_primary_urls_func=_select_unfetched_primary_urls,
) -> bool:
    if not cascade_email_primary:
        return False
    if not _is_recoverable_initial_fetch_error(exc):
        return False
    if page_map:
        return True
    if _is_slow_empty_initial_fetch_timeout(exc, elapsed_ms):
        return False
    return bool(
        _select_initial_fallback_urls(
            fetch_plan,
            page_map,
            primary_urls,
            select_unfetched_primary_urls_func=select_unfetched_primary_urls_func,
        )
    )


def _is_slow_empty_initial_fetch_timeout(exc: Exception, elapsed_ms: int) -> bool:
    if elapsed_ms < _SLOW_EMPTY_INITIAL_FETCH_RECOVERY_CUTOFF_MS:
        return False
    if isinstance(exc, TimeoutError):
        return True
    message = str(exc or "").lower()
    return "timeout" in message or "timed out" in message


def _is_recoverable_initial_fetch_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    message = str(exc or "").lower()
    return any(
        token in message
        for token in (
            "timeout",
            "timed out",
            "empty_page_batch",
            "temporary",
            "curl:",
            "connection",
        )
    )


def _recover_from_common_probe_pages(
    protocol: SiteProtocolClient,
    website: str,
    discovered_urls: list[str],
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    metrics: SiteStageMetrics,
    *,
    collect_phone_enabled: bool,
) -> tuple[list[str], dict[str, list[str]], list[str], list[tuple[str, str]]]:
    common_pages, common_fetch_ms = _fetch_common_probe_recovery_pages(protocol, website, discovered_urls)
    if not common_pages:
        return [], {}, [], []
    metrics.fetch_pages_ms += common_fetch_ms
    _merge_pages_into_map(page_map, common_pages)
    metrics.fetched_page_count = len(page_map)
    return _collect_recovered_contact_details(
        website,
        page_map,
        fetch_plan,
        metrics,
        collect_phone_enabled=collect_phone_enabled,
    )


def _fetch_common_probe_recovery_pages(
    protocol: SiteProtocolClient,
    website: str,
    discovered_urls: list[str],
) -> tuple[list, int]:
    probe_urls = _select_common_email_probe_urls(
        website,
        SimpleNamespace(urls=list(discovered_urls or [])),
        limit=_COMMON_RECOVERY_PROBE_LIMIT,
    )
    fetched_pages: list = []
    elapsed_ms = 0
    for batch_urls in _iter_common_recovery_batches(probe_urls):
        started = time.monotonic()
        try:
            pages = protocol.fetch_pages(
                batch_urls,
                max_workers=min(len(batch_urls), _COMMON_RECOVERY_BATCH_SIZE),
                page_pool=None,
            )
        except Exception:  # noqa: BLE001
            pages = []
        elapsed_ms += int(round((time.monotonic() - started) * 1000))
        fetched_pages.extend(page for page in pages if str(getattr(page, "html", "") or "").strip())
        if _pages_have_rule_emails(website, fetched_pages):
            break
    return fetched_pages, elapsed_ms


def _iter_common_recovery_batches(probe_urls: list[str]):
    first_count = min(max(_COMMON_RECOVERY_FIRST_BATCH, 1), len(probe_urls))
    if first_count:
        yield probe_urls[:first_count]
    index = first_count
    while index < len(probe_urls):
        next_index = min(index + _COMMON_RECOVERY_BATCH_SIZE, len(probe_urls))
        yield probe_urls[index:next_index]
        index = next_index


def _pages_have_rule_emails(website: str, pages: list) -> bool:
    pairs = [(page.url, page.html) for page in pages]
    emails, _sources = collect_emails_for_pages(website, pairs)
    return bool(emails)


def _page_map_has_rule_emails(
    website: str,
    page_map: dict[str, object],
    fetch_plan: dict[str, list[str]],
) -> bool:
    pages = _collect_primary_email_rule_pages(page_map, fetch_plan)
    emails, _sources = collect_emails_for_pages(website, pages)
    return bool(emails)


def _collect_recovered_contact_details(
    website: str,
    page_map: dict[str, object],
    fetch_plan: dict[str, list[str]],
    metrics: SiteStageMetrics,
    *,
    collect_phone_enabled: bool,
) -> tuple[list[str], dict[str, list[str]], list[str], list[tuple[str, str]]]:
    recovered_rule_pages = _collect_email_rule_pages(page_map, fetch_plan)
    started = time.monotonic()
    recovered_emails, recovered_sources = collect_emails_for_pages(website, recovered_rule_pages)
    recovered_phones = collect_phones_for_pages(recovered_rule_pages)[0] if collect_phone_enabled else []
    metrics.email_rule_ms += int(round((time.monotonic() - started) * 1000))
    return recovered_emails, recovered_sources, recovered_phones, recovered_rule_pages
