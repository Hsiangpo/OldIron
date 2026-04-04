"""Mynavi HTML 解析器。"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from lxml import html


_GROUP_RE = re.compile(r"/company/list/(n[a-z]+)/")
_COMPANY_RE = re.compile(r"/company/(\d+)/")
_PAGE_RE = re.compile(r"/pg(\d+)/")
_TOTAL_RE = re.compile(r"（(\d+)社）")


def parse_kana_groups(page_html: str) -> list[dict[str, str]]:
    """解析五十音分组。"""
    groups: list[dict[str, str]] = []
    seen: set[str] = set()
    for group_code in re.findall(_GROUP_RE, str(page_html or "")):
        if group_code in seen:
            continue
        seen.add(group_code)
        groups.append({"group_code": group_code, "group_name": group_code})
    return groups


def parse_total_pages(page_html: str) -> int:
    """解析列表页总页数。"""
    max_page = 1
    for page_text in re.findall(_PAGE_RE, str(page_html or "")):
        max_page = max(max_page, int(page_text))
    return max_page


def parse_total_results(page_html: str) -> int:
    matched = _TOTAL_RE.search(str(page_html or ""))
    if matched is None:
        return 0
    return int(str(matched.group(1) or "0"))


def parse_company_cards(page_html: str) -> list[dict[str, str]]:
    tree = html.fromstring(page_html)
    cards: list[dict[str, str]] = []
    items = tree.xpath(
        "//li[contains(@class, 'companySearchList__content')]"
    )
    for item in items:
        links = item.cssselect('a[href*="/company/"]')
        if not links:
            continue
        href = str(links[0].get("href", "") or "").strip()
        company_id = _extract_company_id(href)
        company_name_nodes = item.cssselect("h2.companySearchList__company-name")
        info_nodes = item.cssselect("h3.companySearchList__icon-parent")
        company_name = _clean_text(company_name_nodes[0].text_content()) if company_name_nodes else ""
        address = _clean_text(info_nodes[0].text_content()) if len(info_nodes) >= 1 else ""
        industry = _clean_text(info_nodes[1].text_content()) if len(info_nodes) >= 2 else ""
        if not company_id or not company_name:
            continue
        cards.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "address": address,
                "industry": industry,
                "detail_url": href,
            }
        )
    return cards


def parse_company_detail(page_html: str) -> dict[str, str]:
    tree = html.fromstring(page_html)
    title = _clean_text(tree.xpath("string(//h1[contains(@class,'headingBlock')])") or "")
    representative = _extract_labeled_value(tree, "代表者")
    address = _extract_labeled_value(tree, "本社所在地")
    website = _normalize_website(_extract_first_external_link(tree))
    return {
        "company_name": title.replace("の会社概要", ""),
        "representative": representative,
        "website": website,
        "address": address,
    }


def _extract_company_id(detail_url: str) -> str:
    matched = _COMPANY_RE.search(str(detail_url or ""))
    return str(matched.group(1) or "").strip() if matched is not None else ""


def _extract_labeled_value(tree, label: str) -> str:
    rows = tree.xpath(f'//th[contains(normalize-space(.), "{label}")]/following-sibling::td[1]')
    if not rows:
        return ""
    return _clean_text(rows[0].text_content())


def _extract_first_external_link(tree) -> str:
    for href in tree.xpath("//table//a/@href"):
        parsed = urlparse(str(href or "").strip())
        host = (parsed.netloc or "").lower()
        if host and "tenshoku.mynavi.jp" not in host and "mynavi.jp" in host:
            return str(href).strip()
        if host and "mynavi.jp" not in host:
            return str(href).strip()
    return ""


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

