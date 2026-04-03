"""OneCareer HTML 解析器。"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from lxml import html


_CATEGORY_RE = re.compile(r"/companies/business_categories/(\d+)")
_COMPANY_RE = re.compile(r"^/companies/(\d+)$")
_PAGE_RE = re.compile(r"[?&]page=(\d+)")
def parse_business_categories(page_html: str) -> list[dict[str, str]]:
    """解析行业分类入口。"""
    categories: list[dict[str, str]] = []
    seen: set[str] = set()
    for category_id in re.findall(_CATEGORY_RE, str(page_html or "")):
        category_id = str(category_id or "").strip()
        if not category_id or category_id in seen:
            continue
        seen.add(category_id)
        categories.append({"category_id": category_id, "category_name": category_id})
    return categories


def parse_total_pages(page_html: str) -> int:
    """解析分类页总页数。"""
    tree = html.fromstring(page_html)
    max_page = 1
    for link in tree.cssselect('a[href*="page="]'):
        href = str(link.get("href", "") or "")
        matched = _PAGE_RE.search(href)
        if matched is not None:
            max_page = max(max_page, int(matched.group(1)))
    return max_page


def parse_company_cards(page_html: str) -> list[dict[str, str]]:
    """解析分类页的公司卡片。"""
    tree = html.fromstring(page_html)
    cards: list[dict[str, str]] = []
    for item in tree.cssselect("li.v2-companies__item"):
        title_links = item.cssselect('a.v2-companies__title[href^="/companies/"]')
        if not title_links:
            continue
        href = str(title_links[0].get("href", "") or "").strip()
        company_id = _extract_company_id(href)
        company_name = _clean_text(title_links[0].text_content())
        if not company_id or not company_name:
            continue
        business = item.cssselect("div.v2-companies__business-field span")
        industry = " / ".join(
            text
            for text in (_clean_text(node.text_content()) for node in business[:2])
            if text
        )
        cards.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "industry": industry,
                "detail_url": href,
            }
        )
    return cards


def parse_company_detail(page_html: str) -> dict[str, str]:
    """解析公司详情页。"""
    tree = html.fromstring(page_html)
    info: dict[str, str] = {}
    for row in tree.cssselect("table tr"):
        headers = row.cssselect("th")
        cells = row.cssselect("td")
        if not headers or not cells:
            continue
        key = _clean_text(headers[0].text_content())
        value = _clean_text(cells[0].text_content())
        if key:
            info[key] = value
    return {
        "company_name": info.get("会社名", ""),
        "representative": info.get("代表者名", ""),
        "website": _normalize_website(info.get("ホームページURL", "")),
        "address": info.get("所在地", ""),
        "industry": "",
    }


def _extract_company_id(detail_url: str) -> str:
    matched = _COMPANY_RE.match(str(detail_url or "").strip())
    return str(matched.group(1) or "").strip() if matched is not None else ""


def _normalize_website(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    if not parsed.scheme:
        text = f"https://{text}"
    return text.rstrip("/")


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()
