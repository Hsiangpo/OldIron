"""OpenWork HTML 解析器。"""

from __future__ import annotations

import math
import re
from urllib.parse import urlparse

from lxml import html


_TOTAL_RE = re.compile(r"([\d,]+)\s*件中")
_PAGE_RE = re.compile(r"next_page=(\d+)")


def parse_total_results(page_html: str) -> int:
    """解析总公司数。"""
    matched = _TOTAL_RE.search(page_html or "")
    if matched is None:
        return 0
    return int(str(matched.group(1) or "0").replace(",", ""))


def parse_total_pages(page_html: str, per_page: int = 50) -> int:
    """解析总页数。"""
    tree = html.fromstring(page_html)
    max_page = 1
    for link in tree.cssselect('a[href*="next_page="]'):
        href = str(link.get("href", "") or "")
        matched = _PAGE_RE.search(href)
        if matched is not None:
            max_page = max(max_page, int(matched.group(1)))
    if max_page > 1:
        return max_page
    total_results = parse_total_results(page_html)
    if total_results <= 0:
        return 1
    return math.ceil(total_results / max(int(per_page or 1), 1))


def parse_company_cards(page_html: str) -> list[dict[str, str]]:
    """解析列表页中的公司卡片。"""
    tree = html.fromstring(page_html)
    cards: list[dict[str, str]] = []
    for item in tree.cssselect("ul.testCompanyList > li"):
        company_link = item.cssselect("div.searchCompanyName h3 a")
        if not company_link:
            continue
        href = str(company_link[0].get("href", "") or "").strip()
        company_id = _extract_company_id(href)
        company_name = _clean_text(company_link[0].text_content())
        industry_nodes = item.cssselect("div.f-l.w-295 p.gray")
        industry = _clean_text(industry_nodes[0].text_content()) if industry_nodes else ""
        if not company_id or not company_name:
            continue
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
    info_map = _extract_company_info_map(tree)
    company_name = _parse_company_name(tree)
    return {
        "company_name": company_name or info_map.get("社名", ""),
        "website": _normalize_website(info_map.get("URL", "")),
        "address": info_map.get("所在地", ""),
        "representative": info_map.get("代表者", ""),
        "industry": info_map.get("業界", ""),
    }


def _extract_company_info_map(tree) -> dict[str, str]:
    info: dict[str, str] = {}
    for row in tree.cssselect("table.definitionList-wiki tr"):
        headers = row.cssselect("th")
        cells = row.cssselect("td")
        if not headers or not cells:
            continue
        key = _clean_text(headers[0].text_content())
        value = _clean_text(cells[0].text_content())
        if key:
            info[key] = value
    return info


def _parse_company_name(tree) -> str:
    for selector in ("a.noLink.v-m", "h1", "title"):
        nodes = tree.cssselect(selector)
        if not nodes:
            continue
        text = _clean_text(nodes[0].text_content())
        if text:
            return text.replace(" 「社員クチコミ」 就職・転職の採用企業リサーチ OpenWork", "")
    return ""


def _extract_company_id(detail_url: str) -> str:
    matched = re.search(r"m_id=([^&]+)", str(detail_url or ""))
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
