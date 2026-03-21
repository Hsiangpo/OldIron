from __future__ import annotations

import re
from dataclasses import replace
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse, unquote

from ...crawler import CrawlerClient
from ...models import LinkItem, PageContent
from ...utils import is_same_domain, normalize_url, url_depth
from ..logging import _humanize_crawl_error, _log
from ..memory import _remember_failed, _update_memory_visited
from ..selection import (
    _COMPANY_ENTRY_KEYWORDS,
    _COMPANY_KEYWORDS,
    _COMPANY_OVERVIEW_KEYWORDS,
    _CONTACT_KEYWORDS,
    _PRIVACY_KEYWORDS,
    _REP_PAGE_KEYWORDS,
    _company_path_boost,
    _dedupe_urls_keep_order,
    _filter_rep_candidate_urls,
    _keyword_hit_score,
    _keyword_present,
    _looks_like_contact_url,
    _looks_like_non_html_link,
    _noise_path_penalty,
    _top_matching_urls,
)


def _url_dedupe_key(url: str | None) -> str:
    normalized = normalize_url(url or "")
    if normalized:
        parsed = urlparse(normalized)
        host = (parsed.hostname or "").lower().strip(".")
        if host.startswith("www."):
            host = host[4:]
        path = (parsed.path or "").rstrip("/")
        query = f"?{parsed.query}" if parsed.query else ""
        return f"{host}{path}{query}"
    raw = (url or "").strip()
    if not raw:
        return ""
    return raw.split("#", 1)[0].rstrip("/")


def _collect_visited_keys(visited: dict[str, PageContent], memory: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in visited.keys():
        key = _url_dedupe_key(value)
        if key:
            keys.add(key)
    cached = memory.get("visited_norm") if isinstance(memory, dict) else None
    if isinstance(cached, list):
        for value in cached:
            if isinstance(value, str):
                key = _url_dedupe_key(value)
                if key:
                    keys.add(key)
    return keys


def _filter_fetch_urls(
    urls: list[str],
    visited: dict[str, PageContent],
    memory: dict[str, Any],
    *,
    max_pages: int,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    visited_keys = _collect_visited_keys(visited, memory)
    failed_raw = memory.get("failed", []) if isinstance(memory.get("failed"), list) else []
    failed_keys = {_url_dedupe_key(value) for value in failed_raw if isinstance(value, str)}
    failed_keys = {value for value in failed_keys if value}
    for url in urls:
        if len(visited) + len(out) >= max_pages:
            break
        if _looks_like_non_html_link(url, allow_pdf=False):
            continue
        key = _url_dedupe_key(url)
        if not key:
            continue
        if key in visited_keys or key in failed_keys or key in seen:
            continue
        seen.add(key)
        out.append(url)
    return out


async def _fetch_pages_batch(
    website: str,
    crawler: CrawlerClient,
    urls: list[str],
    visited: dict[str, PageContent],
    memory: dict[str, Any],
    *,
    max_pages: int,
    label: str,
) -> None:
    urls = _filter_fetch_urls(urls, visited, memory, max_pages=max_pages)
    if not urls:
        return
    preview = urls[:6]
    _log(website, f"{label}（{len(urls)}）：{', '.join(preview)}")
    pages = await crawler.fetch_pages(urls)
    for url, page in zip(urls, pages):
        if not page.success:
            _log(
                website, f"页面暂时打不开：{url}（{_humanize_crawl_error(page.error)}）"
            )
            _remember_failed(memory, url)
            continue
        http_status = _detect_http_error_status(page)
        if http_status in (403, 404):
            _log(website, f"页面返回 {http_status}，跳过：{url}")
            _remember_failed(memory, url)
            continue
        visited[page.url] = page
        visited_keys = _collect_visited_keys(visited, memory)
        requested_key = _url_dedupe_key(url)
        if requested_key:
            visited_keys.add(requested_key)
        resolved_key = _url_dedupe_key(page.url)
        if resolved_key:
            visited_keys.add(resolved_key)
        memory["visited_norm"] = sorted(visited_keys)
        _log(website, f"已打开页面：{page.title or page.url}")
        _update_memory_visited(memory, visited)
        if _should_hint_overview_links(page):
            _hint_overview_neighbor_links(website, page, memory)


class _HomepageLinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._current_href: str | None = None
        self._current_text: list[str] = []
        self.links: list[tuple[str, str]] = []
        self._title_parts: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t == "a":
            href = None
            attrs_dict = {k.lower(): (v or "") for k, v in attrs if k}
            for key, value in attrs:
                if key.lower() == "href":
                    href = value
                    break
            if href:
                self._current_href = href
                self._current_text = []
                label = (
                    attrs_dict.get("aria-label")
                    or attrs_dict.get("title")
                    or attrs_dict.get("data-label")
                    or attrs_dict.get("data-title")
                )
                if label:
                    self._current_text.append(label)
        elif t == "title":
            self._in_title = True
        elif t == "img" and self._current_href is not None:
            attrs_dict = {k.lower(): (v or "") for k, v in attrs if k}
            alt = attrs_dict.get("alt")
            if alt:
                self._current_text.append(alt)

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)
        if self._in_title:
            self._title_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t == "a" and self._current_href is not None:
            text = "".join(self._current_text).strip()
            self.links.append((self._current_href, text))
            self._current_href = None
            self._current_text = []
        elif t == "title":
            self._in_title = False


def _extract_links_from_html(html: str, base_url: str) -> list[LinkItem]:
    if not html:
        return []
    parser = _HomepageLinkExtractor()
    try:
        parser.feed(html)
    except Exception:
        return []
    items: list[LinkItem] = []
    seen: set[str] = set()
    for href, text in parser.links:
        href = (href or "").strip()
        if not href:
            continue
        lower = href.lower()
        if (
            lower.startswith("#")
            or lower.startswith("mailto:")
            or lower.startswith("tel:")
            or lower.startswith("javascript:")
        ):
            continue
        abs_url = urljoin(base_url, href)
        normalized = normalize_url(abs_url) or abs_url
        if not is_same_domain(base_url, normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        if _looks_like_non_html_link(normalized, allow_pdf=True):
            continue
        items.append(LinkItem(url=normalized, text=text.strip() if text else None))
    return items


def _extract_title_from_html(html: str) -> str | None:
    if not html:
        return None
    match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = unescape(match.group(1)).strip()
    return title or None


def _prefer_html_for_short_content(
    website: str, page: PageContent, *, min_len: int = 120
) -> None:
    if not page or not page.success:
        return
    raw_html = page.raw_html
    if not (isinstance(raw_html, str) and raw_html.strip()):
        return
    markdown = page.markdown or ""
    if len(markdown.strip()) >= min_len:
        return
    page.markdown = raw_html
    _log(website, "页面改用 HTML 源码供规则解析")


def _detect_http_error_status(page: PageContent) -> int | None:
    if not page or not page.success:
        return None
    title = (page.title or "").strip()
    snippet = (page.markdown or "")[:800]
    html_snippet = (page.raw_html or "")[:800]
    text = " ".join([title, snippet, html_snippet]).lower()
    if "403" in text and "forbidden" in text:
        return 403
    if "404" in text and ("not found" in text or "notfound" in text or "error" in text):
        return 404
    return None


_PARKED_TITLE_TOKENS = (
    "お名前.com",
    "レンタルサーバー",
    "coming soon",
    "under construction",
    "domain parked",
    "parked domain",
    "domain for sale",
    "website is for sale",
    "default web site page",
    "apache2 ubuntu default page",
    "welcome to nginx",
    "iis windows server",
    "it works!",
)
_PARKED_BODY_TOKENS = (
    "このドメインは",
    "ドメインは現在",
    "ただいま準備中",
    "準備中",
    "工事中",
    "coming soon",
    "under construction",
    "domain parked",
    "parked domain",
    "domain for sale",
    "buy this domain",
    "レンタルサーバー",
    "お名前.com",
)


def _is_parked_page(page: PageContent) -> bool:
    if not page or not page.success:
        return False
    title = (page.title or "").strip().lower()
    if any(token in title for token in _PARKED_TITLE_TOKENS):
        return True
    text = " ".join([(page.markdown or ""), (page.raw_html or "")]).lower()
    if not text:
        return False
    score = 0
    for token in _PARKED_BODY_TOKENS:
        if token in text:
            score += 1
    return score >= 2


def _hint_overview_neighbor_links(
    website: str, page: PageContent, memory: dict[str, Any]
) -> None:
    if not isinstance(memory, dict) or not isinstance(page, PageContent):
        return
    links = page.links
    if not links and isinstance(page.raw_html, str) and page.raw_html.strip():
        links = _extract_links_from_html(page.raw_html, page.url)
    if not links:
        return
    candidates = _top_matching_urls(links, _COMPANY_OVERVIEW_KEYWORDS, limit=6)
    if not candidates:
        return
    hints = memory.get("hints")
    if not isinstance(hints, list):
        hints = []
    new_count = 0
    for url in candidates:
        if url not in hints:
            hints.append(url)
            new_count += 1
    if new_count:
        memory["hints"] = hints[-30:]
        _log(website, f"页面发现会社概要，已标记相邻链接 {new_count} 条")


def _extract_links_from_page(page: PageContent) -> list[LinkItem]:
    if not isinstance(page, PageContent):
        return []
    links = page.links
    if (
        (not links or len(links) == 0)
        and isinstance(page.raw_html, str)
        and page.raw_html.strip()
    ):
        links = _extract_links_from_html(page.raw_html, page.url)
    return links or []


async def _open_company_info_chain(
    website: str,
    crawler: CrawlerClient,
    visited: dict[str, PageContent],
    memory: dict[str, Any],
    *,
    max_pages: int,
    allow_pdf: bool,
    need_rep: bool,
) -> bool:
    if not visited:
        return False
    pages_remaining = max_pages - len(visited)
    if pages_remaining <= 0:
        return False
    homepage = _find_page_by_url(visited, website) or _pick_homepage_candidate(visited)
    if not homepage:
        return False
    home_links = _extract_links_from_page(homepage)
    if not home_links:
        return False
    entry_candidates = _top_matching_urls(home_links, _COMPANY_ENTRY_KEYWORDS, limit=6)
    entry_filtered = [
        u
        for u in entry_candidates
        if _keyword_hit_score(u, "", _PRIVACY_KEYWORDS) == 0
        and _keyword_hit_score(u, "", _CONTACT_KEYWORDS) == 0
    ]
    if entry_filtered:
        entry_candidates = entry_filtered
    entry_candidates = [
        u
        for u in entry_candidates
        if not _looks_like_non_html_link(u, allow_pdf=allow_pdf)
    ]
    entry_candidates = _filter_fetch_urls(
        entry_candidates, visited, memory, max_pages=max_pages
    )
    entry_to_open: list[str] = []
    for url in entry_candidates:
        if len(entry_to_open) >= 1:
            break
        entry_to_open.append(url)
    opened = False
    if entry_to_open:
        _log(website, f"规则优先打开企业情报入口：{', '.join(entry_to_open)}")
        await _fetch_pages_batch(
            website,
            crawler,
            entry_to_open,
            visited,
            memory,
            max_pages=max_pages,
            label="并发打开企业情报页面",
        )
        opened = True
    pages_remaining = max_pages - len(visited)
    if pages_remaining <= 0:
        return opened
    overview_candidates: list[str] = []
    overview_candidates.extend(
        _top_matching_urls(home_links, _COMPANY_OVERVIEW_KEYWORDS, limit=6)
    )
    for url in entry_to_open:
        page = _find_page_by_url(visited, url) or visited.get(url)
        if not page:
            continue
        links = _extract_links_from_page(page)
        overview_candidates.extend(
            _top_matching_urls(links, _COMPANY_OVERVIEW_KEYWORDS, limit=6)
        )
    overview_filtered = [
        u
        for u in overview_candidates
        if _keyword_hit_score(u, "", _PRIVACY_KEYWORDS) == 0
        and _keyword_hit_score(u, "", _CONTACT_KEYWORDS) == 0
    ]
    if overview_filtered:
        overview_candidates = overview_filtered
    overview_candidates = [
        u
        for u in overview_candidates
        if not _looks_like_non_html_link(u, allow_pdf=allow_pdf)
    ]
    overview_candidates = _dedupe_urls_keep_order(overview_candidates)
    overview_candidates = _filter_fetch_urls(
        overview_candidates, visited, memory, max_pages=max_pages
    )
    if not overview_candidates:
        return opened
    overview_to_open = overview_candidates[: max(1, min(2, pages_remaining))]
    _log(website, f"规则优先打开会社概要页面：{', '.join(overview_to_open)}")
    await _fetch_pages_batch(
        website,
        crawler,
        overview_to_open,
        visited,
        memory,
        max_pages=max_pages,
        label="并发打开会社概要页面",
    )
    opened = True
    pages_remaining = max_pages - len(visited)
    if pages_remaining <= 0 or not need_rep:
        return opened
    rep_candidates: list[str] = []
    rep_candidates.extend(_top_matching_urls(home_links, _REP_PAGE_KEYWORDS, limit=6))
    for url in entry_to_open + overview_to_open:
        page = _find_page_by_url(visited, url) or visited.get(url)
        if not page:
            continue
        links = _extract_links_from_page(page)
        rep_candidates.extend(_top_matching_urls(links, _REP_PAGE_KEYWORDS, limit=6))
    rep_filtered = [
        u
        for u in rep_candidates
        if not _keyword_present(u, "", _PRIVACY_KEYWORDS)
        and not _keyword_present(u, "", _CONTACT_KEYWORDS)
    ]
    if rep_filtered:
        rep_candidates = rep_filtered
    rep_candidates = _filter_rep_candidate_urls(rep_candidates)
    rep_candidates = [
        u
        for u in rep_candidates
        if not _looks_like_non_html_link(u, allow_pdf=allow_pdf)
    ]
    rep_candidates = _dedupe_urls_keep_order(rep_candidates)
    rep_candidates = _filter_fetch_urls(rep_candidates, visited, memory, max_pages=max_pages)
    if not rep_candidates:
        return opened
    rep_to_open = rep_candidates[: max(1, min(2, pages_remaining))]
    _log(website, f"规则优先打开代表人页面：{', '.join(rep_to_open)}")
    await _fetch_pages_batch(
        website,
        crawler,
        rep_to_open,
        visited,
        memory,
        max_pages=max_pages,
        label="并发打开代表人页面",
    )
    return True


def _should_hint_overview_links(page: PageContent) -> bool:
    if not isinstance(page, PageContent):
        return False
    title = page.title or ""
    url = page.url or ""
    if _keyword_hit_score(url, title, _COMPANY_KEYWORDS) > 0:
        return True
    for link in page.links or []:
        text = link.text or ""
        if _keyword_hit_score(link.url or "", text, _COMPANY_OVERVIEW_KEYWORDS) > 0:
            return True
        if getattr(link, "is_nav", False) and _keyword_hit_score(
            link.url or "", text, _COMPANY_KEYWORDS
        ):
            return True
    return False


def _pick_homepage_candidate(pages: dict[str, PageContent]) -> PageContent | None:
    if not pages:
        return None
    return sorted(pages.values(), key=lambda p: (url_depth(p.url or ""), len(p.url or "")))[0]


def _compress_html_for_context(html: str) -> str:
    cleaned = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\\1>", " ", html)
    cleaned = _html_to_text(cleaned)
    cleaned = re.sub(r"[ \\t]+", " ", cleaned)
    cleaned = re.sub(r"\\n{3,}", "\\n\\n", cleaned)
    return cleaned.strip()


def _looks_like_html_text(text: str) -> bool:
    snippet = (text or "").lstrip()
    if not snippet.startswith("<"):
        return False
    lower = snippet[:200].lower()
    return any(token in lower for token in ("<html", "<!doctype", "<body", "<div", "<table", "<section"))


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r"(?i)<br\\s*/?>", "\\n", html)
    text = re.sub(r"(?i)</(?:p|div|tr|th|td|li|dt|dd|h\\d)>", "\\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ \\t]+", " ", text)
    text = re.sub(r"\\n{2,}", "\\n", text)
    return text.strip()


def _guess_link_label(url: str) -> str | None:
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip("/")
    if not path:
        return None
    tail = path.split("/")[-1]
    if not tail:
        return None
    tail = re.sub(r"[-_]+", " ", tail)
    tail = unquote(tail).strip()
    return tail or None


def _build_homepage_link_digest(page: PageContent, max_links: int = 24) -> str | None:
    links = _extract_links_from_page(page)
    if not links:
        return None
    keywords = _COMPANY_ENTRY_KEYWORDS + _COMPANY_OVERVIEW_KEYWORDS + _REP_PAGE_KEYWORDS
    scored: list[tuple[int, int, int, LinkItem]] = []
    for item in links:
        url = (item.url or "").strip()
        if not url:
            continue
        if _looks_like_non_html_link(url, allow_pdf=False):
            continue
        text = (item.text or "").strip()
        hit = _keyword_hit_score(url, text, keywords)
        depth = url_depth(url)
        score = hit + (3 if getattr(item, "is_nav", False) else 0) + max(0, 3 - depth)
        score += _company_path_boost(url)
        score += _noise_path_penalty(url)
        scored.append((score, depth, len(url), item))
    if not scored:
        return None
    scored.sort(key=lambda x: (-x[0], x[1], x[2]))
    picked = [item for _, _, _, item in scored[: max(1, max_links)]]
    lines = []
    for item in picked:
        url = (item.url or "").strip()
        if not url:
            continue
        label = (item.text or "").strip()
        if not label:
            label = _guess_link_label(url) or ""
        label = re.sub(r"\\s+", " ", label).strip()
        if label and len(label) > 36:
            label = label[:36] + "…"
        lines.append(f"{label} -> {url}" if label else url)
    return " | ".join(lines) if lines else None


def _build_homepage_context(page: PageContent | None, max_chars: int = 2000) -> str | None:
    if not page:
        return None
    parts: list[str] = []
    remaining = max_chars
    if remaining <= 0:
        return None

    def append_part(label: str, text: str | None) -> None:
        nonlocal remaining
        if not text or remaining <= 0:
            return
        snippet = re.sub(r"\\s+", " ", text).strip()
        if not snippet:
            return
        budget = remaining - len(label)
        if budget <= 0:
            return
        if len(snippet) > budget:
            snippet = snippet[:budget] + "…"
        parts.append(label + snippet)
        remaining = max(0, remaining - len(label) - len(snippet))

    if isinstance(page.fit_markdown, str) and page.fit_markdown.strip():
        append_part("Markdown 摘要：", page.fit_markdown)
    elif isinstance(page.markdown, str) and page.markdown.strip():
        append_part("Markdown 摘要：", page.markdown)
    link_digest = _build_homepage_link_digest(page)
    if link_digest:
        append_part("\\n首页导航链接：", link_digest)
    if isinstance(page.raw_html, str) and page.raw_html.strip() and remaining > 200:
        html_snippet = _compress_html_for_context(page.raw_html)
        append_part("\\nHTML 片段：", html_snippet)
    return "".join(parts) if parts else None


def _get_homepage_context(
    website: str,
    visited: dict[str, PageContent],
    memory: dict[str, Any],
    *,
    max_chars: int = 2000,
) -> str | None:
    cached = memory.get("homepage_context") if isinstance(memory, dict) else None
    if isinstance(cached, str) and cached.strip():
        return cached
    page = _find_page_by_url(visited, website)
    if not page:
        page = _pick_homepage_candidate(visited)
    context = _build_homepage_context(page, max_chars=max_chars)
    if context and isinstance(memory, dict):
        memory["homepage_context"] = context
    return context


def _find_page_by_url(visited: dict[str, PageContent], target_url: str | None) -> PageContent | None:
    if not target_url:
        return None
    target_norm = normalize_url(target_url)
    if not target_norm:
        return None
    for url, page in visited.items():
        if normalize_url(url) == target_norm:
            return page
    return None
