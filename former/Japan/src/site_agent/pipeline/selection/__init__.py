from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse, unquote

from ...models import LinkItem, PageContent
from ...utils import url_depth


def _merge_links(
    visited: dict[str, PageContent],
    memory: dict[str, Any] | None = None,
    *,
    allow_pdf: bool = True,
) -> list[LinkItem]:
    seen: set[str] = set()
    merged: list[LinkItem] = []
    for page in visited.values():
        for link in page.links or []:
            url = (link.url or "").strip()
            if not url:
                continue
            if _looks_like_non_html_link(url, allow_pdf=allow_pdf):
                continue
            if url in seen:
                continue
            seen.add(url)
            merged.append(link)
    if memory is not None:
        sitemap_links = memory.get("sitemap_links")
        if isinstance(sitemap_links, list):
            for url in sitemap_links:
                if not isinstance(url, str) or not url.strip():
                    continue
                if _looks_like_non_html_link(url, allow_pdf=allow_pdf):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                merged.append(LinkItem(url=url, text="sitemap"))
    if memory is not None:
        hinted_links = memory.get("hints")
        if isinstance(hinted_links, list):
            for url in hinted_links:
                if not isinstance(url, str) or not url.strip():
                    continue
                if _looks_like_non_html_link(url, allow_pdf=allow_pdf):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                merged.append(LinkItem(url=url, text="hint"))
    if memory is not None:
        memory["link_pool_size"] = len(merged)
    return merged


def _remaining_links(
    visited: dict[str, PageContent],
    links_pool: list[LinkItem],
    memory: dict[str, Any],
) -> list[LinkItem]:
    visited_urls = set(visited.keys())
    failed = (
        set(memory.get("failed", []))
        if isinstance(memory.get("failed"), list)
        else set()
    )
    remaining = [
        link
        for link in links_pool
        if link.url not in visited_urls and link.url not in failed
    ]
    return remaining


def _pick_homepage_candidate(pages: dict[str, PageContent]) -> PageContent | None:
    if not pages:
        return None
    return sorted(
        pages.values(), key=lambda p: (url_depth(p.url or ""), len(p.url or ""))
    )[0]


def _select_pages_for_llm(
    visited: dict[str, PageContent],
    *,
    max_pages: int,
    missing_fields: list[str] | None = None,
) -> list[PageContent]:
    pages = [p for p in visited.values() if isinstance(p, PageContent) and p.success]
    if not pages:
        return []
    homepage = _pick_homepage_candidate(visited)
    focus_keywords = _COMPANY_OVERVIEW_KEYWORDS
    if isinstance(missing_fields, list) and "representative" in missing_fields:
        focus_keywords = _REP_PAGE_KEYWORDS + _COMPANY_OVERVIEW_KEYWORDS

    def score(page: PageContent) -> tuple[int, int, int]:
        url = page.url or ""
        title = page.title or ""
        hit = _keyword_hit_score(url, title, focus_keywords)
        depth = url_depth(url)
        return (hit, max(0, 4 - depth), len(url))

    if isinstance(missing_fields, list) and "representative" in missing_fields:
        pages = [
            p
            for p in pages
            if not _looks_like_contact_url(p.url or "")
            and not _keyword_present(p.url or "", p.title or "", _CONTACT_KEYWORDS)
        ] or pages
    pages.sort(key=score, reverse=True)
    picked: list[PageContent] = []
    seen_urls: set[str] = set()
    if homepage and homepage.url and homepage.url not in seen_urls:
        picked.append(homepage)
        seen_urls.add(homepage.url)
    for page in pages:
        if len(picked) >= max(1, int(max_pages)):
            break
        if page.url and page.url in seen_urls:
            continue
        picked.append(page)
        if page.url:
            seen_urls.add(page.url)
    return picked


def _dedupe_urls_keep_order(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        result.append(url)
    return result


def _collect_pdf_links(visited: dict[str, PageContent]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for page in visited.values():
        for link in page.links or []:
            url = (link.url or "").strip()
            if not url or url in seen:
                continue
            if _looks_like_pdf_link(url):
                seen.add(url)
                urls.append(url)
    return urls


def _looks_like_pdf_link(url: str) -> bool:
    cleaned = (url or "").split("?", 1)[0].split("#", 1)[0].lower()
    return cleaned.endswith(".pdf")


def _looks_like_contact_url(url: str) -> bool:
    value = (url or "").lower()
    return (
        any(kw in value for kw in ("contact", "inquiry", "toiawase", "otoiawase"))
        or ("お問い合わせ" in url)
        or ("問合" in url)
    )


def _filter_rep_candidate_urls(urls: list[str]) -> list[str]:
    if not urls:
        return []
    noise_tokens = (
        "sitemap",
        "archive",
        "link",
        "download",
        "userlist",
        "use",
        "voice",
        "news",
        "blog",
        "column",
        "case",
        "recruit",
        "career",
    )
    filtered: list[str] = []
    for url in urls:
        if not isinstance(url, str) or not url.strip():
            continue
        if _looks_like_time_greeting_url(url):
            continue
        if _looks_like_contact_url(url):
            continue
        lowered = url.lower()
        if any(token in lowered for token in noise_tokens):
            continue
        if _keyword_present(url, "", _CONTACT_KEYWORDS):
            continue
        if _keyword_present(url, "", _PRIVACY_KEYWORDS):
            continue
        filtered.append(url)
    return filtered


def _email_key_pages_exhausted(
    visited: dict[str, PageContent],
    links_pool: list[LinkItem],
    memory: dict[str, Any],
) -> bool:
    key_urls = _top_key_urls_for_email(links_pool)
    if not key_urls:
        return False
    visited_urls = set(visited.keys())
    failed_urls = (
        set(memory.get("failed", []))
        if isinstance(memory.get("failed"), list)
        else set()
    )
    remaining = [
        url for url in key_urls if url not in visited_urls and url not in failed_urls
    ]
    return not remaining


def _top_key_urls_for_email(links_pool: list[LinkItem]) -> list[str]:
    urls: list[str] = []
    urls.extend(_top_matching_urls(links_pool, _CONTACT_KEYWORDS, limit=4))
    urls.extend(_top_matching_urls(links_pool, _COMPANY_KEYWORDS, limit=3))
    urls.extend(_top_matching_urls(links_pool, _PRIVACY_KEYWORDS, limit=3))
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _ensure_key_pages_in_selection(
    selected: list[str],
    remaining: list[LinkItem],
    missing_fields: list[str],
    max_select: int,
    visited: dict[str, PageContent],
    memory: dict[str, Any],
) -> list[str]:
    if max_select <= 0:
        return []
    visited_urls = set(visited.keys())
    failed_urls = (
        set(memory.get("failed", []))
        if isinstance(memory.get("failed"), list)
        else set()
    )

    def pick_best(candidates: list[str]) -> str | None:
        for url in candidates:
            if url in visited_urls or url in failed_urls:
                continue
            if url in selected:
                continue
            return url
        return None

    priority: list[str] = []
    if "representative" in missing_fields:
        already_has_rep = any(
            _keyword_hit_score(url, "", _REP_PAGE_KEYWORDS) > 0 for url in selected
        )
        if not already_has_rep:
            rep_candidates = _top_matching_urls(remaining, _REP_PAGE_KEYWORDS, limit=8)
            rep_candidates = _filter_rep_candidate_urls(rep_candidates)
            rep_url = pick_best(rep_candidates)
            if rep_url:
                priority.append(rep_url)
        already_has_company = any(
            _keyword_hit_score(url, "", _COMPANY_KEYWORDS) > 0 for url in selected
        )
        if not already_has_company:
            company_candidates = _top_matching_urls(
                remaining, _COMPANY_OVERVIEW_KEYWORDS, limit=8
            )
            company_url = pick_best(company_candidates)
            if company_url:
                priority.append(company_url)
    if "email" in missing_fields:
        already_has_contact = any(
            _looks_like_contact_url(url)
            or _keyword_present(url, "", _CONTACT_KEYWORDS)
            for url in selected
        )
        if not already_has_contact:
            contact_candidates = _top_matching_urls(
                remaining, _CONTACT_KEYWORDS, limit=10
            )
            privacy_candidates = _top_matching_urls(
                remaining, _PRIVACY_KEYWORDS, limit=6
            )
            contact_url = pick_best(contact_candidates)
            privacy_url = pick_best(privacy_candidates)
            if contact_url and contact_url not in priority:
                priority.append(contact_url)
            if privacy_url and privacy_url not in priority:
                priority.append(privacy_url)
    if max_select == 1 and priority:
        return priority[:1]
    fixed = list(priority)
    for url in selected:
        if url in fixed:
            continue
        fixed.append(url)
    return fixed[:max_select]


def _top_matching_urls(links: list[LinkItem], keywords: list[str], limit: int) -> list[str]:
    scored: list[tuple[int, int, int, str]] = []
    for item in links:
        url = (item.url or "").strip()
        if not url:
            continue
        if _looks_like_non_html_link(url, allow_pdf=False):
            continue
        text = (item.text or "").strip()
        if not _keyword_present(url, text, keywords):
            continue
        hit = _keyword_hit_score(url, text, keywords)
        depth = url_depth(url)
        nav_boost = 2 if getattr(item, "is_nav", False) else 0
        is_company = _is_company_keyword_list(keywords)
        is_rep = _is_rep_keyword_list(keywords)
        path_boost = _company_path_boost(url) if is_company else 0
        noise_penalty = _noise_path_penalty(url) if (is_company or is_rep) else 0
        text_boost = _company_text_boost(text) if is_company else 0
        scored.append(
            (
                hit + nav_boost + path_boost + noise_penalty + text_boost,
                depth,
                len(url),
                url,
            )
        )
    scored.sort(key=lambda x: (-x[0], x[1], x[2]))
    return [url for _, _, _, url in scored[: max(0, limit)]]


def _prefilter_links_for_llm(
    links: list[LinkItem],
    missing_fields: list[str] | None,
    *,
    limit: int = 80,
) -> list[LinkItem]:
    if not links or len(links) <= limit:
        return links
    link_map = {
        link.url: link for link in links if isinstance(link, LinkItem) and link.url
    }
    picked: list[LinkItem] = []
    seen: set[str] = set()
    keywords: list[str] = []
    if missing_fields:
        if "representative" in missing_fields:
            keywords.extend(_REP_PAGE_KEYWORDS)
        keywords.extend(_COMPANY_OVERVIEW_KEYWORDS)
        keywords.extend(_COMPANY_ENTRY_KEYWORDS)
        if "email" in missing_fields:
            keywords.extend(_CONTACT_KEYWORDS)
    else:
        keywords.extend(_COMPANY_OVERVIEW_KEYWORDS)
        keywords.extend(_COMPANY_ENTRY_KEYWORDS)
    if keywords:
        for url in _top_matching_urls(links, keywords, limit=20):
            if url in seen:
                continue
            item = link_map.get(url)
            if item:
                picked.append(item)
                seen.add(url)
    for url in _top_matching_urls(links, _CONTACT_KEYWORDS, limit=8):
        if url in seen:
            continue
        item = link_map.get(url)
        if item:
            picked.append(item)
            seen.add(url)
    remaining = [link for link in links if link.url and link.url not in seen]
    remaining.sort(key=lambda l: (url_depth(l.url or ""), len(l.url or "")))
    if len(picked) < limit:
        picked.extend(remaining[: max(0, limit - len(picked))])
    return picked


def _keyword_hit_score(url: str, text: str, keywords: list[str]) -> int:
    lower_url = url.lower()
    lower_text = text.lower()
    score = 0
    for kw in keywords:
        if not kw:
            continue
        if kw.isascii():
            needle = kw.lower()
            if needle in lower_url:
                score += 6
            if needle in lower_text:
                score += 3
        else:
            if kw in url:
                score += 6
            if kw in text:
                score += 3
    depth = url_depth(url)
    score += max(0, 4 - depth)
    return score


def _keyword_present(url: str, text: str, keywords: list[str]) -> bool:
    lower_url = url.lower()
    lower_text = text.lower()
    for kw in keywords:
        if not kw:
            continue
        if kw.isascii():
            needle = kw.lower()
            if needle in lower_url or needle in lower_text:
                return True
        else:
            if kw in url or kw in text:
                return True
    return False


def _is_company_keyword_list(keywords: list[str]) -> bool:
    return any(
        token in keywords
        for token in ("会社概要", "会社案内", "企业信息", "company", "corporate", "about")
    )


def _is_rep_keyword_list(keywords: list[str]) -> bool:
    return any(
        token in keywords
        for token in (
            "代表者",
            "代表取締役",
            "代表",
            "社長",
            "会長",
            "役員",
            "top-message",
            "message",
            "greeting",
            "president",
            "ceo",
        )
    )


_COMPANY_TEXT_BOOST_TOKENS = (
    "会社概要",
    "会社案内",
    "会社情報",
    "企业信息",
    "Company Profile",
    "Company Information",
    "About Us",
    "About",
    "Corporate",
)


def _company_text_boost(text: str) -> int:
    if not text:
        return 0
    lowered = text.lower()
    for token in _COMPANY_TEXT_BOOST_TOKENS:
        if token.isascii():
            if token.lower() in lowered:
                return 4
        elif token in text:
            return 5
    return 0


_COMPANY_PATH_PARTS = {
    "company",
    "about",
    "info",
    "profile",
    "outline",
    "overview",
    "corporate",
    "corp",
    "information",
    "company-info",
    "company-information",
    "company-profile",
    "companyprofile",
    "company-outline",
    "companyoverview",
    "corporate-info",
    "corporate-information",
    "corporate-profile",
    "corporate-outline",
    "corporate-overview",
    "corporateinfo",
    "corp-info",
    "corpinfo",
    "company-data",
    "companydata",
    "about-us",
    "aboutus",
    "about-company",
    "aboutcompany",
    "gaiyou",
    "kaisya",
    "kaisha",
}

_NOISE_PATH_PARTS = {
    "news",
    "topics",
    "press",
    "press-release",
    "blog",
    "column",
    "event",
    "events",
    "seminar",
    "search",
    "case",
    "cases",
    "works",
    "work",
    "product",
    "products",
    "service",
    "services",
    "solution",
    "solutions",
    "recruit",
    "recruitment",
    "career",
    "jobs",
    "job",
    "ir",
    "csr",
    "sustainability",
    "faq",
    "support",
    "shop",
    "store",
    "cart",
    "login",
    "signup",
    "お知らせ",
    "ニュース",
    "トピックス",
    "新着",
    "更新",
    "プレス",
    "リリース",
    "ブログ",
    "コラム",
    "イベント",
    "セミナー",
    "採用",
    "求人",
    "リクルート",
    "導入事例",
    "事例",
    "実績",
    "製品",
    "商品",
    "サービス",
    "ソリューション",
    "サポート",
    "qanda",
    "qa",
    "検索",
}

_GREETING_TOKENS = ("挨拶", "メッセージ", "message", "greeting")
_DATE_PATH_RE = re.compile(r"(20\d{2}|19\d{2}|\d{4}年|\d{1,2}月|\d{1,2}日)")
_SITEMAP_NOISE_TOKENS = (
    "年末",
    "年始",
    "年頭",
    "新年",
    "年末年始",
    "冬季休業",
    "夏季休業",
    "休業",
    "休暇",
    "臨時休業",
    "移転",
    "移転のお知らせ",
    "閉店",
    "開店",
    "御礼",
    "お礼",
    "お知らせ",
    "ニュース",
    "press",
    "release",
    "blog",
    "topics",
    "event",
    "seminar",
    "recruit",
    "privacy",
    "policy",
    "terms",
    "cookie",
)


def _path_contains_date(path: str) -> bool:
    if not path:
        return False
    return bool(_DATE_PATH_RE.search(path))


def _looks_like_time_greeting_url(url: str) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").lower().strip("/")
    if path:
        path = unquote(path)
    if not path:
        return False
    if not any(token in path for token in _GREETING_TOKENS):
        return False
    if _path_contains_date(path):
        return True
    for token in _SITEMAP_NOISE_TOKENS:
        if token in path:
            return True
    return False


def _greeting_path_penalty(path: str) -> int:
    if not path:
        return 0
    if not any(token in path for token in _GREETING_TOKENS):
        return 0
    if _path_contains_date(path):
        return -8
    for token in _SITEMAP_NOISE_TOKENS:
        if token in path:
            return -6
    return -2


def _noise_path_penalty(url: str) -> int:
    parsed = urlparse(url or "")
    path = (parsed.path or "").lower().strip("/")
    if path:
        path = unquote(path)
    if not path:
        return 0
    parts = [p for p in path.split("/") if p]
    if not parts:
        return 0
    penalty = _greeting_path_penalty("/".join(parts))
    for part in parts:
        for token in _NOISE_PATH_PARTS:
            if token in part:
                penalty = min(penalty, -6)
                break
    return penalty


def _company_path_boost(url: str) -> int:
    parsed = urlparse(url or "")
    path = (parsed.path or "").lower().strip("/")
    if not path:
        return 0
    parts = [p for p in path.split("/") if p]
    if not parts:
        return 0
    if len(parts) <= 3 and parts[-1] in _COMPANY_PATH_PARTS:
        return 4
    return 0


_NON_HTML_EXTENSIONS = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".bmp",
    ".svg",
    ".mp4",
    ".mov",
    ".avi",
    ".wmv",
    ".mp3",
    ".wav",
    ".ogg",
    ".zip",
    ".rar",
    ".7z",
    ".gz",
    ".tar",
    ".tgz",
    ".bz2",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".csv",
    ".tsv",
    ".json",
    ".xml",
    ".rss",
    ".atom",
    ".js",
    ".css",
    ".map",
    ".ico",
    ".eot",
    ".ttf",
    ".woff",
    ".woff2",
    ".otf",
)


def _looks_like_non_html_link(url: str, *, allow_pdf: bool) -> bool:
    lower = (url or "").strip().lower()
    if not lower:
        return False
    if lower.startswith(("mailto:", "tel:", "javascript:", "data:")):
        return True
    cleaned = lower.split("?", 1)[0].split("#", 1)[0]
    parsed = urlparse(cleaned)
    path = unquote(parsed.path or "")
    if path:
        stripped = path.strip("/ ")
        if stripped and all(ch in "!._-" for ch in stripped):
            return True
    if any(token in cleaned for token in ("/wp-json", "/xmlrpc.php", "/oembed/")):
        return True
    if cleaned.endswith(("/feed", "/comments/feed")) or "/feed/" in cleaned:
        return True
    if not allow_pdf and cleaned.endswith(".pdf"):
        return True
    return cleaned.endswith(_NON_HTML_EXTENSIONS)


_CONTACT_KEYWORDS = [
    "contact",
    "contactus",
    "inquiry",
    "toiawase",
    "otoiawase",
    "mail",
    "email",
    "お問い合わせ",
    "問合せ",
    "問合",
    "問い合わせ",
    "連絡",
    "ご意見",
    "ご要望",
]

_COMPANY_KEYWORDS = [
    "company",
    "company-information",
    "company-info",
    "company-profile",
    "companyprofile",
    "company-outline",
    "companyoverview",
    "corporate",
    "corporate-information",
    "corporate-info",
    "corporate-profile",
    "corporate-outline",
    "corporate-overview",
    "profile",
    "outline",
    "overview",
    "about",
    "about-us",
    "aboutus",
    "info",
    "information",
    "会社概要",
    "会社案内",
    "会社情報",
    "会社紹介",
    "会社データ",
    "会社プロフィール",
    "会社沿革",
    "企業情報",
    "企業概要",
    "企業案内",
    "企業紹介",
    "企業データ",
    "企業プロフィール",
    "企业信息",
    "企业情报",
    "沿革",
    "アクセス",
    "所在地",
]

_REP_PAGE_KEYWORDS = [
    "代表者",
    "代表取締役",
    "代表",
    "社長",
    "会長",
    "取締役",
    "役員",
    "役員紹介",
    "役員一覧",
    "経営陣",
    "代表挨拶",
    "代表者挨拶",
    "代表者あいさつ",
    "代表メッセージ",
    "社長挨拶",
    "社長あいさつ",
    "社長メッセージ",
    "トップメッセージ",
    "ご挨拶",
    "ごあいさつ",
    "挨拶",
    "あいさつ",
    "メッセージ",
    "message",
    "greeting",
    "top-message",
    "topmessage",
    "leadership",
    "management",
    "executive",
    "officer",
    "board",
    "president",
    "ceo",
    "chairman",
]

_COMPANY_ENTRY_KEYWORDS = [
    "企業情報",
    "公司信息",
    "公司情报",
    "会社情報",
    "会社案内",
    "会社紹介",
    "会社データ",
    "会社プロフィール",
    "企業案内",
    "企業紹介",
    "企業データ",
    "企業プロフィール",
    "about",
    "about-us",
    "aboutus",
    "company",
    "company-information",
    "company-info",
    "companyprofile",
    "company-profile",
    "company-outline",
    "companyoverview",
    "corporate",
    "corporate-information",
    "corporate-info",
    "corporate-profile",
    "corporate-outline",
    "corporate-overview",
    "profile",
    "outline",
    "overview",
    "info",
    "information",
    "gaiyou",
    "kaisya",
    "kaisha",
]

_COMPANY_OVERVIEW_KEYWORDS = [
    "会社概要",
    "会社案内",
    "会社情報",
    "会社紹介",
    "会社データ",
    "会社プロフィール",
    "会社沿革",
    "企業情報",
    "企業概要",
    "企業案内",
    "企業紹介",
    "企業データ",
    "企業プロフィール",
    "公司信息",
    "公司情报",
    "company",
    "company-information",
    "company-info",
    "company-profile",
    "companyprofile",
    "company-outline",
    "companyoverview",
    "corporate",
    "corporate-information",
    "corporate-info",
    "corporate-profile",
    "corporate-outline",
    "corporate-overview",
    "profile",
    "overview",
    "about",
    "outline",
    "about-us",
    "aboutus",
    "information",
    "company profile",
    "company outline",
    "代表挨拶",
    "代表メッセージ",
    "社長挨拶",
    "社長メッセージ",
    "トップメッセージ",
    "役員紹介",
    "役員一覧",
    "経営陣",
]

_PRIVACY_KEYWORDS = [
    "privacy",
    "policy",
    "プライバシ",
    "個人情報",
    "個人情報保護",
    "情報セキュリティ",
]
