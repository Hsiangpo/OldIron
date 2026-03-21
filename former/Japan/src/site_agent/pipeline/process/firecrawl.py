from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

from ...config import PipelineSettings
from ...crawler import CrawlerClient
from ...llm_client import LLMClient
from ...models import PageContent, SiteInput
from ...utils import normalize_url
from ..crawl import _compress_html_for_context
from ..heuristics import _sanitize_info
from ..logging import _log
from ..selection import (
    _COMPANY_OVERVIEW_KEYWORDS,
    _CONTACT_KEYWORDS,
    _REP_PAGE_KEYWORDS,
    _dedupe_urls_keep_order,
    _keyword_hit_score,
    _looks_like_pdf_link,
    _looks_like_non_html_link,
)

_MISSING_REPRESENTATIVE_TEXT = "未找到代表人"


async def _extract_with_firecrawl(
    site: SiteInput,
    crawler: CrawlerClient,
    visited: dict[str, PageContent],
    settings: PipelineSettings,
    memory: dict[str, Any],
) -> dict[str, Any] | None:
    urls = _pick_firecrawl_urls(
        site, visited, max_urls=settings.firecrawl_extract_max_urls
    )
    if not urls:
        _log(site.website, "Firecrawl 未找到可用页面，跳过代表人/座机抽取")
        return {"error": "firecrawl_no_urls"}
    _log(site.website, f"Firecrawl 提取代表人/座机/注册资金/公司人数（{len(urls)} 页）")
    schema = {
        "type": "object",
        "properties": {
            "representative": {"type": ["string", "null"]},
            "phone": {"type": ["string", "null"]},
            "capital": {"type": ["string", "null"]},
            "employees": {"type": ["string", "null"]},
        },
        "required": ["representative", "phone", "capital", "employees"],
    }
    prompt = (
        "Extract the representative name, phone number, registered capital (資本金/注册资金), "
        "and employee count (従業員数/公司人数) from the official site pages. "
        "If missing, return null. Representative should be a person name (e.g., 代表取締役/社長/CEO). "
        "Do NOT return generic headings such as 挨拶/ご挨拶/メッセージ/トップメッセージ."
    )
    response = await crawler.extract_fields(urls, prompt=prompt, schema=schema)
    if isinstance(response, dict):
        error = response.get("error")
        if isinstance(error, str) and error.strip():
            _log(site.website, f"Firecrawl 提取失败：{error.strip()}")
            return {"error": error.strip()}
    parsed = _parse_firecrawl_extract_response(response)
    if not isinstance(parsed, dict):
        _log(site.website, "Firecrawl 返回结构异常，无法解析")
        return {"error": "firecrawl_extract_failed"}
    info = {
        "representative": parsed.get("representative"),
        "phone": parsed.get("phone"),
        "capital": parsed.get("capital"),
        "employees": parsed.get("employees"),
    }
    evidence: dict[str, Any] = {}
    source_url = urls[0] if urls else site.website
    for key in ("representative", "phone", "capital", "employees"):
        value = info.get(key)
        if isinstance(value, str) and value.strip():
            evidence[key] = {"url": source_url, "quote": value, "source": "firecrawl"}
    if evidence:
        info["evidence"] = evidence
    info = _sanitize_info(info)
    memory["firecrawl_extract_urls"] = urls
    return info


def _parse_firecrawl_extract_response(
    response: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(response, dict):
        return None
    if isinstance(response.get("error"), str):
        return None
    payload = response.get("data") if isinstance(response.get("data"), dict) else response
    if not isinstance(payload, dict):
        return None
    for key in ("data", "extract", "result"):
        candidate = payload.get(key)
        if isinstance(candidate, dict):
            return candidate
    return payload if payload else None


def _pick_firecrawl_urls(
    site: SiteInput, visited: dict[str, PageContent], *, max_urls: int
) -> list[str]:
    max_urls = max(1, int(max_urls))
    candidates: list[tuple[int, str]] = []
    for url, page in visited.items():
        title = page.title or ""
        rep_hit = _keyword_hit_score(url, title, _REP_PAGE_KEYWORDS)
        company_hit = _keyword_hit_score(url, title, _COMPANY_OVERVIEW_KEYWORDS)
        contact_hit = _keyword_hit_score(url, title, _CONTACT_KEYWORDS)
        score = rep_hit * 3 + company_hit * 2 + contact_hit
        candidates.append((score, url))
    candidates.sort(key=lambda item: item[0], reverse=True)
    picked = [url for _score, url in candidates if isinstance(url, str)]
    if site.website not in picked:
        picked.insert(0, site.website)
    guessed = _guess_common_company_paths(site.website, visited, limit=4)
    if guessed:
        picked = [site.website] + guessed + picked
    picked = _dedupe_urls_keep_order(picked)
    return picked[:max_urls]


def _guess_common_company_paths(
    base_url: str, visited: dict[str, PageContent], *, limit: int = 8
) -> list[str]:
    candidates = [
        "/company/profile/index.html",
        "/company/profile/",
        "/company/profile",
        "/company/index.html",
        "/company/",
        "/company",
        "/company/overview/",
        "/company/outline/",
        "/company/aisatsu/",
        "/company/message/",
        "/company/greeting/",
        "/company/president/",
        "/company/top-message/",
        "/company/officer/",
        "/company/officers/",
        "/about",
        "/about-us",
        "/about/index.html",
        "/about/overview/",
        "/about/outline/",
        "/aboutus.html",
        "/corporate",
        "/corporate/overview",
        "/corporate/profile",
        "/corporate/officers/",
        "/profile",
        "/profile.html",
        "/outline.html",
        "/leadership.html",
        "/management.html",
        "/board.html",
        "/message",
        "/message/",
        "/greeting",
        "/greeting/",
        "/president",
        "/president/",
        "/top-message",
        "/top-message/",
        "/gaiyo",
        "/gaiyou",
        "/kaisya",
        "/kaisya/gaiyo",
        "/kaisya/gaiyou",
        "/company-info",
        "/companyinfo",
        "/company_info",
        "/info/outline/",
        "/ext/company/company.html",
    ]
    seen = {normalize_url(u) for u in visited.keys()}
    out: list[str] = []
    base = base_url or ""
    for path in candidates:
        url = normalize_url(urljoin(base, path))
        if not url or url in seen:
            continue
        if _looks_like_non_html_link(url, allow_pdf=False):
            continue
        out.append(url)
        if len(out) >= max(1, int(limit)):
            break
    return out


def _has_basic_company_info(info: dict[str, Any] | None) -> bool:
    if not isinstance(info, dict):
        return False
    company = info.get("company_name")
    rep = info.get("representative")
    return bool(
        isinstance(company, str)
        and company.strip()
        and isinstance(rep, str)
        and rep.strip()
    )


def _has_rep_and_email(info: dict[str, Any] | None) -> bool:
    if not isinstance(info, dict):
        return False
    rep = info.get("representative")
    if not isinstance(rep, str) or not rep.strip():
        return False
    if rep.strip() == _MISSING_REPRESENTATIVE_TEXT:
        return False
    email = info.get("email")
    return isinstance(email, str) and "@" in email


def _should_use_html_for_llm(page: PageContent) -> bool:
    if not isinstance(page, PageContent):
        return False
    url = page.url or ""
    title = page.title or ""
    return (
        _keyword_hit_score(url, title, _COMPANY_OVERVIEW_KEYWORDS + _REP_PAGE_KEYWORDS)
        > 0
    )


def _build_pages_payload(
    pages: list[PageContent],
    max_chars: int,
    *,
    allow_pdf: bool = True,
    prefer_fit: bool = False,
    prefer_raw_html: bool = False,
) -> list[dict[str, Any]]:
    payload = []
    for page in pages:
        if not allow_pdf and _looks_like_pdf_link(page.url or ""):
            continue
        content = page.markdown or ""
        if (
            prefer_fit
            and isinstance(page.fit_markdown, str)
            and page.fit_markdown.strip()
        ):
            content = page.fit_markdown
        if prefer_raw_html and isinstance(page.raw_html, str) and page.raw_html.strip():
            content = _compress_html_for_context(page.raw_html)
        if (
            _should_use_html_for_llm(page)
            and isinstance(page.raw_html, str)
            and page.raw_html.strip()
        ):
            if not prefer_fit or len(content.strip()) < 120:
                content = _compress_html_for_context(page.raw_html)
        elif (
            len(content.strip()) < 80
            and isinstance(page.raw_html, str)
            and page.raw_html.strip()
        ):
            content = _compress_html_for_context(page.raw_html)
        if not content:
            continue
        if max_chars > 0 and len(content) > max_chars:
            content = content[:max_chars]
        payload.append(
            {
                "url": page.url,
                "title": page.title,
                "content": content,
            }
        )
    return payload


def _collect_vision_attachments(
    pages: list[PageContent], max_images: int
) -> list[dict[str, str]]:
    if max_images <= 0:
        return []
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for page in pages:
        for att in page.attachments or []:
            if not isinstance(att, dict):
                continue
            data_url = att.get("data_url")
            if not (isinstance(data_url, str) and data_url.startswith("data:image/")):
                continue
            key = data_url[:64]
            if key in seen:
                continue
            seen.add(key)
            items.append(
                {
                    "kind": str(att.get("kind") or "image"),
                    "url": str(att.get("url") or page.url),
                    "data_url": data_url,
                }
            )
            if len(items) >= max_images:
                return items
    return items


async def _should_skip_by_keyword(
    website: str,
    keyword: str,
    llm: LLMClient,
    visited: dict[str, PageContent],
    settings: PipelineSettings,
    memory: dict[str, Any],
) -> tuple[bool, dict[str, Any] | None]:
    if not (settings.keyword_filter_enabled and keyword):
        return False, None
    pages_payload = _build_pages_payload(list(visited.values()), settings.max_content_chars)
    if not pages_payload:
        return False, None
    decision = await llm.check_site_keyword(website, keyword, pages_payload, memory=memory)
    if not isinstance(decision, dict):
        return False, None
    match = decision.get("match")
    if match is not False:
        return False, decision
    confidence = decision.get("confidence")
    threshold = settings.keyword_min_confidence
    if (
        isinstance(confidence, (int, float))
        and isinstance(threshold, (int, float))
        and threshold > 0
        and confidence < threshold
    ):
        return False, decision
    return True, decision

