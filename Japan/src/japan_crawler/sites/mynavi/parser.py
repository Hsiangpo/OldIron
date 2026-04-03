"""mynavi HTML 解析器。"""

from __future__ import annotations

import json
import re
from urllib.parse import urlparse

from lxml import html


_PAGE_RE = re.compile(r"/pg(\d+)/")
_MAIL_RE = re.compile(r"mailto:([^\"'>\s]+)", re.IGNORECASE)


def parse_job_cards(page_html: str) -> list[dict[str, str]]:
    """解析列表页中的职位卡片。"""
    tree = html.fromstring(page_html)
    cards: list[dict[str, str]] = []
    for section in tree.cssselect("section.recruit"):
        title_links = [
            link
            for link in section.cssselect("a.entry_click.entry3")
            if "/jobinfo-" in str(link.get("href", "") or "")
        ]
        if not title_links:
            continue
        detail_url = _normalize_detail_url(str(title_links[0].get("href", "") or "").strip())
        job_title = _clean_text(title_links[0].text_content())
        company_name = _clean_company_name(section)
        address = _extract_table_value(section, "勤務地")
        company_data = _clean_text(" ".join(section.xpath('.//p[contains(@class, "company_data")]//text()')))
        if not company_name or not detail_url:
            continue
        cards.append(
            {
                "company_name": company_name,
                "job_title": job_title,
                "detail_url": detail_url,
                "address": address,
                "company_data": company_data,
            }
        )
    return cards


def parse_has_next(page_html: str) -> bool:
    """解析是否存在下一页。"""
    tree = html.fromstring(page_html)
    return bool(tree.xpath('//link[@rel="next"]/@href'))


def parse_detail_page(page_html: str) -> dict[str, str]:
    """解析职位详情页。"""
    tree = html.fromstring(page_html)
    company_info = _extract_company_info_map(tree)
    ldjson = _extract_job_posting_json(page_html)
    return {
        "company_name": _extract_detail_company_name(tree) or company_info.get("会社名", ""),
        "website": _extract_website(company_info, ldjson),
        "representative": company_info.get("代表者", ""),
        "address": company_info.get("本社所在地", "") or company_info.get("所在地", ""),
        "emails": _extract_emails(page_html),
        "phone": _extract_phone(page_html),
        "source_job_url": _extract_canonical_url(tree),
    }


def _clean_company_name(section) -> str:
    nodes = section.cssselect("p.main_title")
    if not nodes:
        return ""
    text = _clean_text(nodes[0].text_content())
    return text.split("|", 1)[0].strip()


def _extract_table_value(section, label: str) -> str:
    for row in section.cssselect("table.detaile_table tr"):
        headers = row.cssselect("th")
        cells = row.cssselect("td")
        if not headers or not cells:
            continue
        if _clean_text(headers[0].text_content()) != label:
            continue
        return _clean_text(cells[0].text_content())
    return ""


def _extract_company_info_map(tree) -> dict[str, str]:
    info: dict[str, str] = {}
    for row in tree.cssselect("section.company table tr"):
        headers = row.cssselect("th")
        cells = row.cssselect("td")
        if not headers or not cells:
            continue
        key = _clean_text(headers[0].text_content())
        value = _clean_text(cells[0].text_content())
        if key:
            info[key] = value
    return info


def _extract_job_posting_json(page_html: str) -> dict[str, object]:
    for matched in re.findall(r'<script type="application/ld\+json">(.*?)</script>', page_html, re.S):
        try:
            payload = json.loads(matched)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(payload, dict) and payload.get("@type") == "JobPosting":
            return payload
    return {}


def _extract_detail_company_name(tree) -> str:
    for link in tree.xpath('//a[contains(@href, "/company/")]'):
        text = _clean_text(link.text_content())
        if text and "求人情報" not in text:
            return text
    return ""


def _extract_website(company_info: dict[str, str], ldjson: dict[str, object]) -> str:
    site = company_info.get("企業ホームページ", "")
    if site and "tenshoku.mynavi.jp/url-forwarder/" not in site:
        return _normalize_url(site)
    organization = ldjson.get("hiringOrganization") if isinstance(ldjson, dict) else None
    if isinstance(organization, dict):
        return _normalize_url(str(organization.get("sameAs", "") or ""))
    return ""


def _extract_emails(page_html: str) -> str:
    emails: list[str] = []
    for matched in _MAIL_RE.findall(page_html or ""):
        email = str(matched or "").strip().lower()
        if email and email not in emails:
            emails.append(email)
    return "; ".join(emails)


def _extract_phone(page_html: str) -> str:
    text = _clean_text(page_html)
    matched = re.search(r"\b0\d{1,4}-\d{1,4}-\d{3,4}\b", text)
    return str(matched.group(0) or "").strip() if matched is not None else ""


def _extract_canonical_url(tree) -> str:
    urls = tree.xpath('//link[@rel="canonical"]/@href')
    return _normalize_url(urls[0]) if urls else ""


def _normalize_detail_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"/(msg|adv\d+)/.*$", "/", text)
    return text


def _normalize_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    if not parsed.scheme:
        text = f"https://{text}"
    return text.rstrip("/")


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()
