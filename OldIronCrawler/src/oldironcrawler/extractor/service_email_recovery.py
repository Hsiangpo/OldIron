from __future__ import annotations

from contextlib import redirect_stderr
import html
from io import BytesIO
from io import StringIO
import httpx
import re
import time
from types import SimpleNamespace
from urllib.parse import urljoin
from urllib.parse import urlparse

try:
    from pypdf import PdfReader
except Exception:  # noqa: BLE001
    PdfReader = None

from oldironcrawler.extractor.discovery_fallback import _select_common_email_probe_urls
from oldironcrawler.extractor.email_rules import (
    collect_emails_for_pages,
    extract_frontend_email_asset_urls,
    extract_frontend_lazy_asset_urls,
    extract_registrable_domain,
)
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.protocol.httpx_client import build_httpx_client_kwargs
from oldironcrawler.extractor.phone_rules import collect_phones_for_pages
from oldironcrawler.extractor.protocol_client import HtmlPage, SiteProtocolClient
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
_FRONTEND_ASSET_INITIAL_LIMIT = 3
_FRONTEND_ASSET_TOTAL_LIMIT = 6
_FRONTEND_ASSET_TIMEOUT_SECONDS = 4.0
_FRONTEND_ASSET_MAX_CHARS = 600000
_PDF_EMAIL_ASSET_INITIAL_LIMIT = 2
_PDF_EMAIL_ASSET_TOTAL_LIMIT = 4
_PDF_EMAIL_ASSET_TIMEOUT_SECONDS = 4.0
_PDF_EMAIL_ASSET_MAX_BYTES = 1500000
_PDF_EMAIL_ASSET_TEXT_LIMIT = 600000
_PDF_EMAIL_ASSET_MIN_SCORE = 2
_PDF_SOURCE_PAGE_FETCH_LIMIT = 2
_PDF_LINK_RE = re.compile(r"""(?is)(?:href|src)\s*=\s*["']([^"']+\.pdf(?:\?[^"']*)?)["']""")
_PDF_SOURCE_HINT_WEIGHTS = {
    "canal": 2,
    "codigo": 2,
    "compliance": 4,
    "conduta": 4,
    "contato": 2,
    "etica": 4,
    "ethic": 4,
    "governanca": 2,
    "governance": 2,
    "igualdade": 1,
    "lgpd": 4,
    "ouvidoria": 3,
    "politica": 2,
    "privacidade": 4,
    "privacy": 4,
}
_PDF_URL_HINT_WEIGHTS = {
    "canal": 2,
    "codigo": 2,
    "compliance": 5,
    "conduta": 5,
    "etica": 5,
    "ethic": 5,
    "lgpd": 4,
    "ouvidoria": 3,
    "politica": 2,
    "privacidade": 4,
    "privacy": 4,
}


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
            open_pages, open_ms = _fetch_initial_open_page_recovery_pages(
                protocol,
                website,
                primary_urls,
            )
            if open_pages:
                return _alias_open_recovery_pages_to_primary_urls(open_pages, primary_urls), (
                    elapsed_ms + max(fallback_ms, 0) + open_ms
                )
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
    emails, email_sources, email_rule_pages = recover_frontend_asset_emails_if_needed(
        protocol,
        website,
        page_map,
        metrics,
        email_rule_pages,
    )
    if emails:
        return emails, email_sources, phones, email_rule_pages
    emails, email_sources, email_rule_pages = recover_pdf_asset_emails_if_needed(
        protocol,
        website,
        page_map,
        metrics,
        email_rule_pages,
    )
    if emails:
        return emails, email_sources, phones, email_rule_pages
    pdf_source_pages, pdf_source_fetch_ms = _fetch_pdf_source_pages_if_needed(
        protocol,
        website,
        discovered_urls,
        page_map,
        page_concurrency=page_concurrency,
        page_pool=page_pool,
    )
    if pdf_source_pages:
        metrics.fetch_pages_ms += pdf_source_fetch_ms
        _merge_pages_into_map(page_map, pdf_source_pages)
        metrics.fetched_page_count = len(page_map)
        emails, email_sources, email_rule_pages = recover_pdf_asset_emails_if_needed(
            protocol,
            website,
            page_map,
            metrics,
            email_rule_pages,
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


def recover_frontend_asset_emails_if_needed(
    protocol: SiteProtocolClient,
    website: str,
    page_map: dict[str, object],
    metrics: SiteStageMetrics,
    email_rule_pages: list[tuple[str, str]],
    *,
    fetch_text_func=None,
) -> tuple[list[str], dict[str, list[str]], list[tuple[str, str]]]:
    started_fetch = time.monotonic()
    asset_pages = _fetch_frontend_email_asset_pages(
        protocol,
        list(page_map.values()),
        fetch_text_func=fetch_text_func,
    )
    metrics.fetch_pages_ms += int(round((time.monotonic() - started_fetch) * 1000))
    if not asset_pages:
        return [], {}, email_rule_pages
    recovered_pages = [(page.url, page.html) for page in asset_pages]
    started_rule = time.monotonic()
    emails, sources = collect_emails_for_pages(website, recovered_pages)
    metrics.email_rule_ms += int(round((time.monotonic() - started_rule) * 1000))
    if not emails:
        return [], {}, email_rule_pages
    return emails, sources, [*email_rule_pages, *recovered_pages]


def recover_pdf_asset_emails_if_needed(
    protocol: SiteProtocolClient,
    website: str,
    page_map: dict[str, object],
    metrics: SiteStageMetrics,
    email_rule_pages: list[tuple[str, str]],
    *,
    fetch_bytes_func=None,
) -> tuple[list[str], dict[str, list[str]], list[tuple[str, str]]]:
    started_fetch = time.monotonic()
    pdf_pages = _fetch_pdf_email_asset_pages(
        protocol,
        website,
        list(page_map.values()),
        fetch_bytes_func=fetch_bytes_func,
    )
    metrics.fetch_pages_ms += int(round((time.monotonic() - started_fetch) * 1000))
    if not pdf_pages:
        return [], {}, email_rule_pages
    recovered_pages = [(page.url, page.html) for page in pdf_pages]
    started_rule = time.monotonic()
    emails, sources = collect_emails_for_pages(website, recovered_pages)
    metrics.email_rule_ms += int(round((time.monotonic() - started_rule) * 1000))
    if not emails:
        return [], {}, email_rule_pages
    return emails, sources, [*email_rule_pages, *recovered_pages]


def _fetch_frontend_email_asset_pages(
    protocol: SiteProtocolClient,
    source_pages: list[object],
    *,
    fetch_text_func=None,
) -> list[HtmlPage]:
    fetch_text = fetch_text_func or (lambda url: _fetch_frontend_asset_text(protocol, url))
    asset_urls = _select_initial_frontend_asset_urls(source_pages)
    pages: list[HtmlPage] = []
    seen = set(asset_urls)
    lazy_urls: list[str] = []
    for url in asset_urls:
        text = fetch_text(url)
        if not text:
            continue
        pages.append(HtmlPage(url=url, html=text))
        for lazy_url in extract_frontend_lazy_asset_urls(text, url, limit=_FRONTEND_ASSET_TOTAL_LIMIT):
            if lazy_url not in seen:
                seen.add(lazy_url)
                lazy_urls.append(lazy_url)
            if len(seen) >= _FRONTEND_ASSET_TOTAL_LIMIT:
                break
        if len(seen) >= _FRONTEND_ASSET_TOTAL_LIMIT:
            break
    for url in lazy_urls[: max(_FRONTEND_ASSET_TOTAL_LIMIT - len(pages), 0)]:
        text = fetch_text(url)
        if text:
            pages.append(HtmlPage(url=url, html=text))
    return pages


def _fetch_pdf_email_asset_pages(
    protocol: SiteProtocolClient,
    website: str,
    source_pages: list[object],
    *,
    fetch_bytes_func=None,
) -> list[HtmlPage]:
    fetch_bytes = fetch_bytes_func or (lambda url: _fetch_pdf_asset_bytes(protocol, url))
    pages: list[HtmlPage] = []
    for url in _select_pdf_email_asset_urls(website, source_pages):
        data = fetch_bytes(url)
        if not data:
            continue
        text = _decode_pdf_asset_bytes(data)
        if not text:
            continue
        pages.append(HtmlPage(url=url, html=text))
    return pages


def _fetch_pdf_source_pages_if_needed(
    protocol: SiteProtocolClient,
    website: str,
    discovered_urls: list[str],
    page_map: dict[str, object],
    *,
    page_concurrency: int,
    page_pool: PageFetchPool | None,
) -> tuple[list[HtmlPage], int]:
    source_urls = _select_pdf_source_recovery_urls(website, discovered_urls, page_map)
    if not source_urls:
        return [], 0
    started = time.monotonic()
    try:
        pages = protocol.fetch_pages(
            source_urls,
            max_workers=max(min(len(source_urls), page_concurrency, _PDF_SOURCE_PAGE_FETCH_LIMIT), 1),
            page_pool=page_pool,
        )
    except Exception:  # noqa: BLE001
        pages = []
    elapsed_ms = int(round((time.monotonic() - started) * 1000))
    filtered = [page for page in pages if str(getattr(page, "html", "") or "").strip()]
    return filtered, elapsed_ms


def _select_initial_frontend_asset_urls(source_pages: list[object]) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()
    for page in source_pages:
        page_url = str(getattr(page, "url", "") or "")
        html_text = str(getattr(page, "html", "") or "")
        for asset_url in extract_frontend_email_asset_urls(
            html_text,
            page_url,
            limit=_FRONTEND_ASSET_INITIAL_LIMIT,
        ):
            if asset_url in seen:
                continue
            seen.add(asset_url)
            selected.append(asset_url)
            if len(selected) >= _FRONTEND_ASSET_INITIAL_LIMIT:
                return selected
    return selected


def _select_pdf_email_asset_urls(website: str, source_pages: list[object]) -> list[str]:
    candidates: list[tuple[int, int, str]] = []
    seen: set[str] = set()
    for order, page in enumerate(source_pages):
        page_url = str(getattr(page, "url", "") or "")
        html_text = str(getattr(page, "html", "") or "")
        if not page_url or not html_text:
            continue
        source_score = _score_pdf_email_source(page_url, html_text)
        if source_score <= 0 and ".pdf" not in html_text.lower():
            continue
        for raw_url in _PDF_LINK_RE.findall(html_text):
            absolute = _normalize_pdf_asset_url(page_url, raw_url)
            if not absolute or absolute in seen:
                continue
            if not _pdf_asset_matches_website(website, absolute):
                continue
            seen.add(absolute)
            score = source_score + _score_pdf_email_asset_url(absolute)
            candidates.append((score, -order, absolute))
    candidates.sort(reverse=True)
    selected: list[str] = []
    for score, _order, url in candidates:
        if score < _PDF_EMAIL_ASSET_MIN_SCORE:
            continue
        selected.append(url)
        if len(selected) >= _PDF_EMAIL_ASSET_TOTAL_LIMIT:
            break
    return selected[:_PDF_EMAIL_ASSET_INITIAL_LIMIT] if selected else []


def _select_pdf_source_recovery_urls(
    website: str,
    discovered_urls: list[str],
    page_map: dict[str, object],
) -> list[str]:
    fetched_urls = {
        _normalize_pdf_source_page_url(str(getattr(page, "url", "") or ""))
        for page in page_map.values()
    }
    candidates: list[tuple[int, int, str]] = []
    for order, url in enumerate(discovered_urls):
        normalized = _normalize_pdf_source_page_url(url)
        if not normalized or normalized in fetched_urls:
            continue
        if not _pdf_asset_matches_website(website, normalized):
            continue
        score = _score_pdf_email_asset_url(normalized)
        if score < _PDF_EMAIL_ASSET_MIN_SCORE:
            continue
        candidates.append((score, -order, normalized))
    candidates.sort(reverse=True)
    return [url for _score, _order, url in candidates[:_PDF_SOURCE_PAGE_FETCH_LIMIT]]


def _fetch_frontend_asset_text(protocol: SiteProtocolClient, url: str) -> str:
    config = getattr(protocol, "_config", None)
    timeout_seconds = min(float(getattr(config, "timeout_seconds", 10.0) or 10.0), _FRONTEND_ASSET_TIMEOUT_SECONDS)
    default_headers = dict(getattr(config, "default_headers", {}) or {})
    proxy_url = str(getattr(config, "proxy_url", "") or "")
    client_kwargs = build_httpx_client_kwargs(default_headers, proxy_url, timeout_seconds)
    try:
        with httpx.Client(**client_kwargs) as client:
            response = client.get(url, timeout=timeout_seconds)
    except Exception:  # noqa: BLE001
        client_kwargs["verify"] = False
        try:
            with httpx.Client(**client_kwargs) as client:
                response = client.get(url, timeout=timeout_seconds)
        except Exception:  # noqa: BLE001
            return ""
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code != 200:
        return ""
    content_type = str(response.headers.get("Content-Type", "") or "").lower()
    if content_type and not any(token in content_type for token in ("javascript", "ecmascript", "text/plain")):
        return ""
    return str(response.text or "")[:_FRONTEND_ASSET_MAX_CHARS]


def _fetch_pdf_asset_bytes(protocol: SiteProtocolClient, url: str) -> bytes:
    config = getattr(protocol, "_config", None)
    timeout_seconds = min(float(getattr(config, "timeout_seconds", 10.0) or 10.0), _PDF_EMAIL_ASSET_TIMEOUT_SECONDS)
    default_headers = dict(getattr(config, "default_headers", {}) or {})
    default_headers["Range"] = f"bytes=0-{_PDF_EMAIL_ASSET_MAX_BYTES - 1}"
    proxy_url = str(getattr(config, "proxy_url", "") or "")
    client_kwargs = build_httpx_client_kwargs(default_headers, proxy_url, timeout_seconds)
    data = _stream_pdf_asset_bytes(client_kwargs, url, timeout_seconds)
    if data:
        return data
    client_kwargs["verify"] = False
    return _stream_pdf_asset_bytes(client_kwargs, url, timeout_seconds)


def _stream_pdf_asset_bytes(client_kwargs: dict, url: str, timeout_seconds: float) -> bytes:
    try:
        with httpx.Client(**client_kwargs) as client:
            with client.stream("GET", url, timeout=timeout_seconds) as response:
                status_code = int(getattr(response, "status_code", 0) or 0)
                if status_code not in {200, 206}:
                    return b""
                content_type = str(response.headers.get("Content-Type", "") or "").lower()
                if "pdf" not in content_type and ".pdf" not in str(url or "").lower():
                    return b""
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_bytes():
                    if not chunk:
                        continue
                    remaining = _PDF_EMAIL_ASSET_MAX_BYTES - total
                    if remaining <= 0:
                        break
                    piece = chunk[:remaining]
                    chunks.append(piece)
                    total += len(piece)
                    if total >= _PDF_EMAIL_ASSET_MAX_BYTES:
                        break
                return b"".join(chunks)
    except Exception:  # noqa: BLE001
        return b""


def _decode_pdf_asset_bytes(data: bytes) -> str:
    if not data:
        return ""
    parsed_text = _extract_pdf_text_with_parser(data)
    if parsed_text:
        return parsed_text[:_PDF_EMAIL_ASSET_TEXT_LIMIT]
    return data.decode("latin1", errors="ignore")[:_PDF_EMAIL_ASSET_TEXT_LIMIT]


def _extract_pdf_text_with_parser(data: bytes) -> str:
    if not data or PdfReader is None:
        return ""
    if b"%PDF" not in data[:1024]:
        return ""
    stderr_buffer = StringIO()
    try:
        with redirect_stderr(stderr_buffer):
            reader = PdfReader(BytesIO(data), strict=False)
    except Exception:  # noqa: BLE001
        return ""
    parts: list[str] = []
    pages = getattr(reader, "pages", None)
    if pages is None:
        return ""
    for index in range(25):
        try:
            with redirect_stderr(stderr_buffer):
                page = pages[index]
        except IndexError:
            break
        except Exception:  # noqa: BLE001
            return ""
        try:
            with redirect_stderr(stderr_buffer):
                text = str(page.extract_text() or "").strip()
        except Exception:  # noqa: BLE001
            text = ""
        if text:
            parts.append(text)
    return "\n".join(parts)


def _score_pdf_email_source(page_url: str, html_text: str) -> int:
    combined = f"{page_url}\n{html.unescape(html_text)}".lower()
    return _score_text_by_hint_weights(combined, _PDF_SOURCE_HINT_WEIGHTS)


def _score_pdf_email_asset_url(url: str) -> int:
    return _score_text_by_hint_weights(str(url or "").lower(), _PDF_URL_HINT_WEIGHTS)


def _score_text_by_hint_weights(text: str, weights: dict[str, int]) -> int:
    score = 0
    lowered = str(text or "").lower()
    for token, weight in weights.items():
        if token in lowered:
            score += weight
    return score


def _normalize_pdf_asset_url(base_url: str, raw_url: str) -> str:
    value = html.unescape(str(raw_url or "").strip())
    if not value or value.startswith(("data:", "javascript:", "mailto:", "tel:")):
        return ""
    absolute = urljoin(base_url, value)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return parsed._replace(fragment="").geturl()


def _normalize_pdf_source_page_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return parsed._replace(fragment="", query="").geturl()


def _pdf_asset_matches_website(website: str, asset_url: str) -> bool:
    site_domain = extract_registrable_domain(website)
    asset_domain = extract_registrable_domain(asset_url)
    return bool(site_domain and asset_domain and site_domain == asset_domain)


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


def _fetch_initial_open_page_recovery_pages(
    protocol: SiteProtocolClient,
    website: str,
    primary_urls: list[str],
) -> tuple[list, int]:
    probe_urls = _build_initial_open_page_recovery_urls(website, primary_urls)
    if not probe_urls:
        return [], 0
    started = time.monotonic()
    try:
        pages = protocol.fetch_pages(
            probe_urls,
            max_workers=min(len(probe_urls), _COMMON_RECOVERY_BATCH_SIZE),
            page_pool=None,
        )
    except Exception:  # noqa: BLE001
        pages = []
    elapsed_ms = int(round((time.monotonic() - started) * 1000))
    return [page for page in pages if str(getattr(page, "html", "") or "").strip()], elapsed_ms


def _build_initial_open_page_recovery_urls(website: str, primary_urls: list[str]) -> list[str]:
    failed = {_open_recovery_identity(url) for url in primary_urls if str(url or "").strip()}
    selected: list[str] = []
    seen = set(failed)
    for seed_url in [website, *primary_urls]:
        parsed = urlparse(str(seed_url or "").strip())
        if not parsed.scheme or not parsed.netloc:
            continue
        for candidate in _iter_initial_open_page_recovery_candidates(parsed):
            identity = _open_recovery_identity(candidate)
            if not identity or identity in seen:
                continue
            seen.add(identity)
            selected.append(candidate)
            if len(selected) >= _COMMON_RECOVERY_BATCH_SIZE:
                return selected
    return selected


def _alias_open_recovery_pages_to_primary_urls(pages: list, primary_urls: list[str]) -> list:
    if not pages:
        return []
    result = list(pages)
    first_page = pages[0]
    html_text = str(getattr(first_page, "html", "") or "")
    existing = {_open_recovery_identity(str(getattr(page, "url", "") or "")) for page in result}
    for primary_url in primary_urls:
        identity = _open_recovery_identity(primary_url)
        if not identity or identity in existing:
            continue
        existing.add(identity)
        result.append(HtmlPage(url=primary_url, html=html_text))
    return result


def _iter_initial_open_page_recovery_candidates(parsed):
    host = str(parsed.netloc or "").strip().lower()
    if not host:
        return
    hosts = _dedupe_open_recovery_values([host, _alternate_open_recovery_host(host)])
    schemes = _dedupe_open_recovery_values(["https", str(parsed.scheme or "").strip().lower(), "http"])
    path = str(parsed.path or "").strip()
    root_paths = [""] if not path or path == "/" else ["", path]
    for scheme in schemes:
        for origin_host in hosts:
            for candidate_path in root_paths:
                yield parsed._replace(scheme=scheme, netloc=origin_host, path=candidate_path, query="", fragment="").geturl()


def _alternate_open_recovery_host(host: str) -> str:
    clean = str(host or "").strip().lower()
    if not clean:
        return ""
    return clean[4:] if clean.startswith("www.") else f"www.{clean}"


def _dedupe_open_recovery_values(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        clean = str(value or "").strip().lower()
        if clean and clean not in result:
            result.append(clean)
    return result


def _open_recovery_identity(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    host = str(parsed.netloc or "").strip().lower()
    if not parsed.scheme or not host:
        return ""
    path = str(parsed.path or "").strip().rstrip("/")
    return f"{parsed.scheme.lower()}://{host}{path}"


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
