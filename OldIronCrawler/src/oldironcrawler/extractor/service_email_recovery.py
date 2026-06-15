from __future__ import annotations

import time

from oldironcrawler.extractor.email_rules import collect_emails_for_pages
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.phone_rules import collect_phones_for_pages
from oldironcrawler.extractor.protocol_client import SiteProtocolClient
from oldironcrawler.extractor.service_discovery import (
    _collect_email_rule_pages,
    _fetch_email_recovery_pages,
    _merge_pages_into_map,
)
from oldironcrawler.runtime.store import SiteStageMetrics


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
    if not recovery_pages:
        return emails, email_sources, phones, email_rule_pages
    metrics.fetch_pages_ms += recovery_fetch_ms
    _merge_pages_into_map(page_map, recovery_pages)
    metrics.fetched_page_count = len(page_map)
    recovered_rule_pages = _collect_email_rule_pages(page_map, fetch_plan)
    started = time.monotonic()
    recovered_emails, recovered_sources = collect_emails_for_pages(website, recovered_rule_pages)
    recovered_phones = collect_phones_for_pages(recovered_rule_pages)[0] if collect_phone_enabled else []
    metrics.email_rule_ms += int(round((time.monotonic() - started) * 1000))
    return recovered_emails, recovered_sources, recovered_phones, recovered_rule_pages
