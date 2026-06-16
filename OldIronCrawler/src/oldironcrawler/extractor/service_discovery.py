from __future__ import annotations

from dataclasses import dataclass, field
import time
from urllib.parse import urlparse

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.discovery_fallback import has_non_homepage_email_target, probe_common_email_value_pages
from oldironcrawler.extractor.email_rules import analyze_email_set, collect_emails_for_pages, extract_registrable_domain
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.protocol_client import HtmlPage, SiteProtocolClient
from oldironcrawler.extractor.value_rules import (
    build_candidates,
    build_fetch_plan,
    canonicalize_target_url,
    count_selected_families,
    extract_path_tokens,
    merge_representative_urls,
    select_email_urls,
    select_representative_urls,
)

_DISCOVERY_PRIMARY_LIMIT = 80
_DISCOVERY_SITEMAP_LIMIT = 80
_DISCOVERY_RELATED_LIMIT = 40
_DISCOVERY_FINAL_LIMIT = 160
_DISCOVERY_EMAIL_FAMILY_TARGET = 6
_DISCOVERY_EMAIL_ONLY_FAMILY_TARGET = 2
_EMAIL_PRIMARY_FAST_PROBE_LIMIT = 1
_EMAIL_RECOVERY_LIMIT = 6
_EMAIL_RECOVERY_MAX_PRIMARY_FETCH_MS = 8000
_EMAIL_RECOVERY_SLOW_FETCH_SCORE_MIN = 100
_BRAZIL_EMAIL_RECOVERY_TOKEN_WEIGHTS = {
    "advogado": 120,
    "advogados": 130,
    "equipe": 115,
    "profissionais": 120,
    "profissional": 110,
    "socio": 100,
    "socios": 110,
    "investidores": 100,
    "relacoes": 80,
    "autorizacoes": 70,
    "cadastro": 60,
    "empresas": 50,
}
_BRAZIL_EMAIL_RECOVERY_PHRASES = (
    ("://ri.", 120),
    ("/advogados", 130),
    ("/equipe", 115),
    ("/profissionais", 120),
    ("/relacoes-com-investidores", 120),
    ("/autorizacoes", 70),
    ("/cadastro", 55),
)
_JAPAN_EMAIL_RECOVERY_TOKEN_WEIGHTS = {
    "contact": 130,
    "inquiry": 130,
    "mailform": 125,
    "form": 105,
    "otoiawase": 120,
    "toiawase": 120,
    "site": 90,
    "support": 70,
    "recruit": 55,
    "saiyo": 55,
    "important": 80,
    "info": 35,
    "news": 30,
}
_JAPAN_EMAIL_RECOVERY_PHRASES = (
    ("/contact", 130),
    ("/inquiry", 130),
    ("/mailform", 125),
    ("/site/", 120),
    ("/form", 105),
    ("/support", 70),
    ("/recruit", 55),
    ("/info/important/", 120),
    ("/info/news", 65),
    ("/news/other/", 115),
)
_DISCOVERY_REP_STRONG_TOKENS = {
    "board",
    "chair",
    "chairman",
    "chief",
    "director",
    "executive",
    "founder",
    "governance",
    "impressum",
    "imprint",
    "kontakt",
    "leadership",
    "management",
    "officers",
    "owner",
    "partner",
    "partners",
    "president",
    "principal",
    "solicitor",
    "team",
    "uber",
    "ueber",
}


@dataclass
class DiscoverySnapshot:
    urls: list[str]
    candidates: list
    rep_urls: list[str]
    teacher_pool: list[str]
    email_urls: list[str]
    homepage_html: str = ""
    prefetched_pages: list[HtmlPage] = field(default_factory=list)


def _resolve_discovery_deadline(config: AppConfig, site_deadline_monotonic: float | None) -> float:
    budget_seconds = min(max(float(getattr(config, "discovery_budget_seconds", 45.0) or 45.0), 30.0), 60.0)
    budget_deadline = time.monotonic() + budget_seconds
    if site_deadline_monotonic is None:
        return budget_deadline
    return min(budget_deadline, site_deadline_monotonic)


def _discovery_budget_exceeded(deadline_monotonic: float | None) -> bool:
    return deadline_monotonic is not None and time.monotonic() >= deadline_monotonic


def _discover_value_snapshot(
    protocol: SiteProtocolClient,
    website: str,
    rep_learned: dict[str, int],
    email_learned: dict[str, int],
    *,
    rep_target_count: int = 5,
    contact_target_enabled: bool = True,
    discovery_deadline_monotonic: float | None = None,
) -> DiscoverySnapshot:
    primary = protocol.discover_primary_urls(website, limit=_DISCOVERY_PRIMARY_LIMIT)
    primary_prefetched_pages = list(getattr(primary, "prefetched_pages", []) or [])
    snapshot = _build_discovery_snapshot(
        website,
        primary.urls,
        rep_learned,
        email_learned,
        rep_target_count=rep_target_count,
        homepage_html=primary.homepage_html,
        prefetched_pages=primary_prefetched_pages,
    )
    if _has_enough_discovery_coverage(snapshot, rep_target_count=rep_target_count):
        return snapshot
    if _discovery_budget_exceeded(discovery_deadline_monotonic):
        return snapshot
    sitemap_urls = protocol.discover_sitemap_urls(website, limit=_DISCOVERY_SITEMAP_LIMIT)
    merged = _merge_unique_urls(snapshot.urls, sitemap_urls, limit=_DISCOVERY_FINAL_LIMIT)
    snapshot = _build_discovery_snapshot(
        website,
        merged,
        rep_learned,
        email_learned,
        rep_target_count=rep_target_count,
        homepage_html=primary.homepage_html,
        prefetched_pages=primary_prefetched_pages,
    )
    if _has_enough_discovery_coverage(snapshot, rep_target_count=rep_target_count):
        return snapshot
    if _discovery_budget_exceeded(discovery_deadline_monotonic):
        return snapshot
    related_urls = protocol.discover_related_subdomain_urls(
        website,
        homepage_html=primary.homepage_html,
        direct_urls=merged,
        limit=_DISCOVERY_RELATED_LIMIT,
    )
    merged = _merge_unique_urls(merged, related_urls, limit=_DISCOVERY_FINAL_LIMIT)
    snapshot = _build_discovery_snapshot(
        website,
        merged,
        rep_learned,
        email_learned,
        rep_target_count=rep_target_count,
        homepage_html=primary.homepage_html,
        prefetched_pages=primary_prefetched_pages,
    )
    if _discovery_budget_exceeded(discovery_deadline_monotonic):
        return snapshot
    if contact_target_enabled and not has_non_homepage_email_target(website, snapshot.email_urls):
        fallback_pages = probe_common_email_value_pages(protocol, website, snapshot)
        fallback_urls = [page.url for page in fallback_pages]
        if fallback_urls:
            merged = _merge_unique_urls(snapshot.urls, fallback_urls, limit=_DISCOVERY_FINAL_LIMIT)
            primary_prefetched_pages = _merge_prefetched_pages(primary_prefetched_pages, fallback_pages)
            snapshot = _build_discovery_snapshot(
                website,
                merged,
                rep_learned,
                email_learned,
                rep_target_count=rep_target_count,
                homepage_html=primary.homepage_html,
                prefetched_pages=primary_prefetched_pages,
            )
    return snapshot


def _merge_prefetched_pages(first: list[HtmlPage], second: list[HtmlPage]) -> list[HtmlPage]:
    result: list[HtmlPage] = []
    seen: set[str] = set()
    for page in [*first, *second]:
        url = str(getattr(page, "url", "") or "")
        html = str(getattr(page, "html", "") or "")
        if not url or url in seen or not html.strip():
            continue
        seen.add(url)
        result.append(HtmlPage(url=url, html=html))
    return result


def _build_discovery_snapshot(
    website: str,
    discovered_urls: list[str],
    rep_learned: dict[str, int],
    email_learned: dict[str, int],
    *,
    rep_target_count: int = 5,
    homepage_html: str = "",
    prefetched_pages: list[HtmlPage] | None = None,
) -> DiscoverySnapshot:
    candidates = build_candidates(website, discovered_urls, rep_learned, email_learned)
    rep_urls, teacher_pool = select_representative_urls(candidates, target_count=rep_target_count)
    email_urls = select_email_urls(candidates)
    return DiscoverySnapshot(
        urls=discovered_urls,
        candidates=candidates,
        rep_urls=rep_urls,
        teacher_pool=teacher_pool,
        email_urls=email_urls,
        homepage_html=homepage_html,
        prefetched_pages=list(prefetched_pages or []),
    )


def _has_enough_discovery_coverage(snapshot: DiscoverySnapshot, *, rep_target_count: int = 5) -> bool:
    if rep_target_count > 0:
        if len(snapshot.rep_urls) < rep_target_count:
            return False
        if not _has_high_confidence_representative_coverage(snapshot):
            return False
    if count_selected_families(snapshot.candidates, snapshot.email_urls) < _discovery_email_family_target(rep_target_count):
        return False
    return True


def _discovery_email_family_target(rep_target_count: int) -> int:
    if rep_target_count <= 0:
        return _DISCOVERY_EMAIL_ONLY_FAMILY_TARGET
    return _DISCOVERY_EMAIL_FAMILY_TARGET


def _has_high_confidence_representative_coverage(snapshot: DiscoverySnapshot) -> bool:
    candidate_map = {candidate.url: candidate for candidate in snapshot.candidates}
    for url in snapshot.rep_urls:
        candidate = candidate_map.get(url)
        if candidate is None:
            continue
        if candidate.is_person_detail_page:
            return True
        if any(token in _DISCOVERY_REP_STRONG_TOKENS for token in candidate.tokens):
            return True
    return False


def _plan_fetch_targets(config: AppConfig, website: str, rep_urls: list[str], email_urls: list[str]) -> dict[str, list[str]]:
    return build_fetch_plan(
        website,
        rep_urls,
        email_urls,
        rep_limit=_get_rep_page_limit(config),
        email_soft_limit=_get_email_page_soft_limit(config),
        email_hard_limit=_get_email_page_hard_limit(config),
        total_hard_limit=_get_page_total_hard_limit(config),
    )


def _fetch_primary_pages(
    protocol: SiteProtocolClient,
    primary_urls: list[str],
    *,
    page_concurrency: int,
    page_pool: PageFetchPool | None,
) -> tuple[list, int]:
    if not primary_urls:
        return [], 0
    return _fetch_pages_with_elapsed(
        protocol.fetch_pages,
        primary_urls,
        page_concurrency=page_concurrency,
        page_pool=page_pool,
    )


def _fetch_email_overflow_pages(
    protocol: SiteProtocolClient,
    fetch_plan: dict[str, list[str]],
    *,
    page_concurrency: int,
    page_pool: PageFetchPool | None,
) -> tuple[list, int]:
    if not fetch_plan["email_overflow_urls"]:
        return [], 0
    return _fetch_pages_with_elapsed(
        protocol.fetch_pages,
        fetch_plan["email_overflow_urls"],
        page_concurrency=page_concurrency,
        page_pool=page_pool,
    )


def _fetch_email_recovery_pages(
    protocol: SiteProtocolClient,
    website: str,
    discovered_urls: list[str],
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    *,
    page_concurrency: int,
    page_pool: PageFetchPool | None,
    primary_fetch_ms: int,
) -> tuple[list, int]:
    if not page_map:
        return [], 0
    recovery_urls = _select_email_recovery_urls(
        website,
        discovered_urls,
        fetch_plan,
        page_map,
        limit=_EMAIL_RECOVERY_LIMIT,
    )
    if not recovery_urls:
        return [], 0
    if primary_fetch_ms >= _EMAIL_RECOVERY_MAX_PRIMARY_FETCH_MS and not _has_slow_fetch_recovery_signal(website, recovery_urls):
        return [], 0
    try:
        return _fetch_pages_with_elapsed(
            protocol.fetch_pages,
            recovery_urls,
            page_concurrency=page_concurrency,
            page_pool=page_pool,
        )
    except Exception:  # noqa: BLE001
        return [], 0


def _has_slow_fetch_recovery_signal(website: str, recovery_urls: list[str]) -> bool:
    return any(_score_email_recovery_url(website, url) >= _EMAIL_RECOVERY_SLOW_FETCH_SCORE_MIN for url in recovery_urls)


def _select_email_recovery_urls(
    website: str,
    discovered_urls: list[str],
    fetch_plan: dict[str, list[str]],
    page_map: dict[str, object],
    *,
    limit: int,
) -> list[str]:
    if limit <= 0 or not _is_email_recovery_scope(website, discovered_urls):
        return []
    blocked = _selected_fetch_keys(fetch_plan, page_map)
    scored: list[tuple[int, int, str]] = []
    fallback: list[tuple[int, str]] = []
    for order, url in enumerate(discovered_urls):
        value = str(url or "").strip()
        if not value or canonicalize_target_url(value) in blocked:
            continue
        if not _is_same_registrable_site(website, value):
            continue
        score = _score_email_recovery_url(website, value)
        if score > 0:
            scored.append((-score, order, value))
        elif _is_fetchable_recovery_url(value):
            fallback.append((order, value))
    result = [url for _score, _order, url in sorted(scored)]
    for _order, url in fallback:
        if len(result) >= limit:
            break
        if url not in result:
            result.append(url)
    return result[:limit]


def _selected_fetch_keys(fetch_plan: dict[str, list[str]], page_map: dict[str, object]) -> set[str]:
    selected_urls = [
        *fetch_plan["all_primary_urls"],
        *fetch_plan["email_overflow_urls"],
        *list(page_map.keys()),
    ]
    return {canonicalize_target_url(str(url or "").strip()) for url in selected_urls if str(url or "").strip()}


def _is_email_recovery_scope(website: str, discovered_urls: list[str]) -> bool:
    site_domain = extract_registrable_domain(website)
    if site_domain.endswith(".br"):
        return True
    if site_domain.endswith(".jp"):
        return any(_score_japan_email_recovery_url(url) > 0 for url in discovered_urls)
    return any(_score_brazil_email_recovery_url(url) > 0 for url in discovered_urls)


def _is_same_registrable_site(website: str, url: str) -> bool:
    site_domain = extract_registrable_domain(website)
    url_domain = extract_registrable_domain(url)
    return bool(site_domain and url_domain and site_domain == url_domain)


def _score_brazil_email_recovery_url(url: str) -> int:
    lowered = str(url or "").strip().lower()
    score = 0
    for token in extract_path_tokens(url):
        score += _BRAZIL_EMAIL_RECOVERY_TOKEN_WEIGHTS.get(token, 0)
    for phrase, value in _BRAZIL_EMAIL_RECOVERY_PHRASES:
        if phrase in lowered:
            score += value
    return score


def _score_email_recovery_url(website: str, url: str) -> int:
    site_domain = extract_registrable_domain(website)
    if site_domain.endswith(".br"):
        return _score_brazil_email_recovery_url(url)
    if site_domain.endswith(".jp"):
        return _score_japan_email_recovery_url(url)
    return _score_brazil_email_recovery_url(url)


def _score_japan_email_recovery_url(url: str) -> int:
    lowered = str(url or "").strip().lower()
    tokens = extract_path_tokens(url)
    if "sitemap" in tokens:
        return 0
    score = 0
    for token in tokens:
        score += _JAPAN_EMAIL_RECOVERY_TOKEN_WEIGHTS.get(token, 0)
    for phrase, value in _JAPAN_EMAIL_RECOVERY_PHRASES:
        if phrase in lowered:
            score += value
    return score


def _is_fetchable_recovery_url(url: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    path = str(parsed.path or "").lower()
    if not parsed.scheme or not parsed.netloc:
        return False
    return not path.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf", ".zip"))


def _should_fetch_email_overflow_after_primary_fetch(
    website: str,
    primary_email_rule_pages: list[tuple[str, str]],
    email_overflow_urls: list[str],
    *,
    email_stop_same_domain_count: int,
) -> bool:
    if not email_overflow_urls:
        return False
    emails, _page_hits = collect_emails_for_pages(website, primary_email_rule_pages)
    if emails:
        return False
    same_domain_count = len(analyze_email_set(website, emails).same_domain_emails)
    return same_domain_count < email_stop_same_domain_count


def _merge_pages_into_map(page_map: dict[str, object], pages: list) -> None:
    for page in pages:
        page_map[page.url] = page


def _select_pages_from_map(page_map: dict[str, object], urls: list[str]) -> list:
    return [page_map[url] for url in urls if url in page_map]


def _collect_email_rule_pages(page_map: dict[str, object], fetch_plan: dict[str, list[str]]) -> list[tuple[str, str]]:
    homepage_pages = _select_pages_from_map(page_map, fetch_plan["homepage_primary_urls"])
    email_pages = _select_pages_from_map(
        page_map,
        [*fetch_plan["email_primary_urls"], *fetch_plan["email_overflow_urls"]],
    )
    rep_pages = _select_pages_from_map(page_map, fetch_plan["rep_urls"])
    selected_urls = [
        *fetch_plan["homepage_primary_urls"],
        *fetch_plan["email_primary_urls"],
        *fetch_plan["email_overflow_urls"],
        *fetch_plan["rep_urls"],
    ]
    extra_pages = _select_extra_pages_from_map(page_map, selected_urls)
    return _merge_email_rule_pages(email_pages, homepage_pages, rep_pages, extra_pages)


def _collect_primary_email_rule_pages(page_map: dict[str, object], fetch_plan: dict[str, list[str]]) -> list[tuple[str, str]]:
    homepage_pages = _select_pages_from_map(page_map, fetch_plan["homepage_primary_urls"])
    email_primary_pages = _select_pages_from_map(page_map, fetch_plan["email_primary_urls"])
    rep_pages = _select_pages_from_map(page_map, fetch_plan["rep_urls"])
    selected_urls = [*fetch_plan["homepage_primary_urls"], *fetch_plan["email_primary_urls"], *fetch_plan["rep_urls"]]
    extra_pages = _select_extra_pages_from_map(page_map, selected_urls)
    return _merge_email_rule_pages(email_primary_pages, homepage_pages, rep_pages, extra_pages)


def _select_extra_pages_from_map(page_map: dict[str, object], selected_urls: list[str]) -> list:
    selected = set(selected_urls)
    return [page for url, page in page_map.items() if url not in selected]


def _merge_email_rule_pages(*page_groups: list) -> list[tuple[str, str]]:
    merged_pages: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for pages in page_groups:
        for page in pages:
            if page.url in seen_urls:
                continue
            seen_urls.add(page.url)
            merged_pages.append((page.url, page.html))
    return merged_pages


def _build_reused_primary_pages(
    website: str,
    fetch_plan: dict[str, list[str]],
    homepage_html: str,
    prefetched_pages: list[HtmlPage] | None = None,
) -> list[HtmlPage]:
    reused: list[HtmlPage] = []
    seen: set[str] = set()
    if homepage_html and website in fetch_plan["all_primary_urls"]:
        reused.append(HtmlPage(url=website, html=homepage_html))
        seen.add(website)
    for page in prefetched_pages or []:
        url = str(getattr(page, "url", "") or "")
        html = str(getattr(page, "html", "") or "")
        if not url or url in seen or not html.strip():
            continue
        reused.append(HtmlPage(url=url, html=html))
        seen.add(url)
    return reused


def _filter_network_primary_urls(primary_urls: list[str], reused_pages: list[HtmlPage]) -> list[str]:
    reused_urls = {page.url for page in reused_pages}
    if not reused_urls:
        return list(primary_urls)
    return [url for url in primary_urls if url not in reused_urls]


def _select_initial_primary_urls(
    fetch_plan: dict[str, list[str]],
    *,
    cascade_email_primary: bool,
) -> list[str]:
    if not cascade_email_primary:
        return list(fetch_plan["all_primary_urls"])
    initial_urls = [
        *fetch_plan["rep_urls"],
        *fetch_plan["homepage_primary_urls"],
        *fetch_plan["email_primary_urls"][:_EMAIL_PRIMARY_FAST_PROBE_LIMIT],
    ]
    return _merge_unique_urls(initial_urls, [], limit=len(initial_urls))


def _select_unfetched_primary_urls(fetch_plan: dict[str, list[str]], page_map: dict[str, object]) -> list[str]:
    return [url for url in fetch_plan["all_primary_urls"] if url not in page_map]


def _fetch_pages_with_elapsed(fetch_func, urls: list[str], *, page_concurrency: int, page_pool: PageFetchPool | None) -> tuple[list, int]:
    started = time.monotonic()
    pages = fetch_func(
        urls,
        max_workers=page_concurrency,
        page_pool=page_pool,
    )
    elapsed_ms = int(round((time.monotonic() - started) * 1000))
    return pages, elapsed_ms


def _get_rep_page_limit(config: AppConfig) -> int:
    return max(int(getattr(config, "rep_page_limit", 5) or 5), 1)


def _get_email_page_soft_limit(config: AppConfig) -> int:
    return max(int(getattr(config, "email_page_soft_limit", 8) or 8), 0)


def _get_email_page_hard_limit(config: AppConfig) -> int:
    return max(int(getattr(config, "email_page_hard_limit", 16) or 16), 0)


def _get_page_total_hard_limit(config: AppConfig) -> int:
    return max(int(getattr(config, "page_total_hard_limit", 20) or 20), 1)


def _get_email_stop_same_domain_count(config: AppConfig) -> int:
    return max(int(getattr(config, "email_stop_same_domain_count", 2) or 2), 1)


def _merge_unique_urls(left: list[str], right: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in [*left, *right]:
        value = str(url or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def _merge_page_targets(rep_urls: list[str], email_urls: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in [*rep_urls, *email_urls]:
        if url and url not in seen:
            seen.add(url)
            result.append(url)
    return result
