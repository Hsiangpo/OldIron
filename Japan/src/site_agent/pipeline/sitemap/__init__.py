from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import urljoin, urlparse, unquote

from ...constants import Limits
from ...crawler import CrawlerClient
from ...models import LinkItem, PageContent
from ...utils import is_same_domain, is_sitemap_like_url, normalize_url
from ..logging import _humanize_crawl_error, _log
from ..memory import _remember_failed, _update_memory_visited
from ..selection import (
    _COMPANY_ENTRY_KEYWORDS,
    _COMPANY_OVERVIEW_KEYWORDS,
    _CONTACT_KEYWORDS,
    _GREETING_TOKENS,
    _NOISE_PATH_PARTS,
    _PRIVACY_KEYWORDS,
    _REP_PAGE_KEYWORDS,
    _SITEMAP_NOISE_TOKENS,
    _dedupe_urls_keep_order,
    _keyword_hit_score,
    _looks_like_non_html_link,
    _top_matching_urls,
)
from ..crawl import _filter_fetch_urls

_URL_LINE_RE = re.compile(r"https?://[^\\s<>\\\"']+")
_SITEMAP_LINE_RE = re.compile(r"^\\s*sitemap:\\s*(\\S+)\\s*$", re.IGNORECASE)
_SITEMAP_PATHS = (
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/sitemap/sitemap.xml",
)

_REP_ANCHOR_TOKENS = (
    "代表",
    "社長",
    "会長",
    "役員",
    "top-message",
    "topmessage",
    "greeting",
    "president",
    "ceo",
    "chairman",
)


def _parse_sitemap_text(text: str) -> list[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    if "<urlset" in cleaned or "<sitemapindex" in cleaned:
        try:
            root = ET.fromstring(cleaned)
        except Exception:
            return _parse_sitemap_as_text(cleaned)
        urls: list[str] = []
        for elem in root.iter():
            tag = elem.tag.lower()
            if tag.endswith("loc") and elem.text:
                urls.append(elem.text.strip())
        return urls
    return _parse_sitemap_as_text(cleaned)


def _parse_sitemap_as_text(text: str) -> list[str]:
    urls: list[str] = []
    for line in text.splitlines():
        if not line:
            continue
        for match in _URL_LINE_RE.findall(line):
            urls.append(match.strip())
    return urls


def _site_root_url(website: str) -> str | None:
    parsed = urlparse(website or "")
    if not parsed.scheme:
        parsed = urlparse(f"https://{website}")
    if not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def _extract_sitemap_urls_from_robots(text: str, base_url: str) -> list[str]:
    urls: list[str] = []
    if not text:
        return urls
    for line in text.splitlines():
        match = _SITEMAP_LINE_RE.match(line.strip())
        if not match:
            continue
        url = match.group(1).strip()
        if not url:
            continue
        urls.append(urljoin(base_url.rstrip("/") + "/", url))
    return urls


async def _discover_sitemap_urls(website: str, crawler: CrawlerClient) -> list[str]:
    root = _site_root_url(website)
    if not root:
        return []
    robots_url = f"{root}/robots.txt"
    robots_page = await crawler.fetch_page(robots_url)
    if robots_page.success:
        text = robots_page.raw_html or robots_page.markdown or ""
        urls = _extract_sitemap_urls_from_robots(text, root)
        if urls:
            return _dedupe_urls_keep_order(urls)
    return [f"{root}{path}" for path in _SITEMAP_PATHS]


async def _collect_sitemap_links(
    website: str,
    crawler: CrawlerClient,
    memory: dict[str, Any],
    *,
    allow_pdf: bool,
) -> list[str]:
    root = _site_root_url(website)
    if not root:
        return []
    sitemap_urls = await _discover_sitemap_urls(website, crawler)
    if not sitemap_urls:
        return []
    pending = list(_dedupe_urls_keep_order(sitemap_urls))
    seen_sitemaps: set[str] = set()
    urls: list[str] = []
    max_sitemap_files = 8
    while (
        pending
        and len(urls) < Limits.MAX_SITEMAP_URLS
        and len(seen_sitemaps) < max_sitemap_files
    ):
        sitemap_url = pending.pop(0)
        if sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)
        page = await crawler.fetch_page(sitemap_url)
        if not page.success:
            continue
        text = page.raw_html or page.markdown or ""
        extracted = _parse_sitemap_text(text)
        if not extracted:
            continue
        for url in extracted:
            if not isinstance(url, str) or not url.strip():
                continue
            normalized = normalize_url(url) or url.strip()
            if not is_same_domain(root, normalized):
                continue
            if is_sitemap_like_url(normalized):
                if normalized not in seen_sitemaps and normalized not in pending:
                    pending.append(normalized)
                continue
            if _looks_like_non_html_link(normalized, allow_pdf=allow_pdf):
                continue
            if normalized not in urls:
                urls.append(normalized)
            if len(urls) >= Limits.MAX_SITEMAP_URLS:
                break
    urls = _dedupe_urls_keep_order(urls)
    if urls:
        _remember_sitemap_links(memory, urls)
    return urls


def _remember_sitemap_links(memory: dict[str, Any], urls: list[str]) -> None:
    if not urls:
        return
    memory["sitemap_links"] = urls


def _is_company_like_url(url: str) -> bool:
    return (
        _keyword_hit_score(url, "", _COMPANY_OVERVIEW_KEYWORDS + _COMPANY_ENTRY_KEYWORDS)
        > 0
    )


def _is_rep_like_url(url: str) -> bool:
    return _keyword_hit_score(url, "", _REP_PAGE_KEYWORDS) > 0


def _sitemap_should_skip(url: str) -> bool:
    if not url:
        return True
    if _looks_like_non_html_link(url, allow_pdf=False):
        return True
    parsed = urlparse(url)
    path = parsed.path or ""
    if path:
        path = unquote(path).lower()
    if any(token in path for token in _SITEMAP_NOISE_TOKENS):
        return True
    if any(token in path for token in _GREETING_TOKENS) and not any(
        token in path for token in _REP_ANCHOR_TOKENS
    ):
        return True
    if _is_company_like_url(url) or _is_rep_like_url(url):
        return False
    if any(token in path for token in _NOISE_PATH_PARTS):
        return True
    if any(token in path for token in _GREETING_TOKENS):
        return True
    return False


async def _prefetch_key_urls_from_sitemap(
    website: str,
    crawler: CrawlerClient,
    visited: dict[str, PageContent],
    sitemap_urls: list[str],
    memory: dict[str, Any],
    *,
    limit: int = 6,
    max_pages: int,
    allow_pdf: bool,
) -> None:
    if not sitemap_urls:
        return
    filtered = [url for url in sitemap_urls if not _sitemap_should_skip(url)]
    link_items = [LinkItem(url=url, text="sitemap") for url in filtered]
    candidates: list[str] = []
    candidates.extend(_top_matching_urls(link_items, _REP_PAGE_KEYWORDS, limit=3))
    candidates.extend(_top_matching_urls(link_items, _COMPANY_OVERVIEW_KEYWORDS, limit=3))
    candidates.extend(_top_matching_urls(link_items, _COMPANY_ENTRY_KEYWORDS, limit=2))
    candidates.extend(_top_matching_urls(link_items, _CONTACT_KEYWORDS, limit=4))
    candidates.extend(_top_matching_urls(link_items, _PRIVACY_KEYWORDS, limit=3))
    picked = _dedupe_urls_keep_order(candidates)
    picked = [u for u in picked if not _looks_like_non_html_link(u, allow_pdf=allow_pdf)]
    picked = _filter_fetch_urls(picked, visited, memory, max_pages=max_pages)
    picked = picked[: max(0, limit)]
    if not picked:
        return
    _log(website, f"Sitemap 优先解析关键页：{', '.join(picked)}")
    failed = (
        set(memory.get('failed', []))
        if isinstance(memory.get('failed'), list)
        else set()
    )
    for url in picked:
        if url in visited or url in failed:
            continue
        page = await crawler.fetch_page(url)
        if not page.success:
            _log(
                website,
                f"关键页暂时打不开：{url}（{_humanize_crawl_error(page.error)}）",
            )
            _remember_failed(memory, url)
            continue
        visited[page.url] = page
        _log(website, f"已打开关键页：{page.title or page.url}")
        _update_memory_visited(memory, visited)

