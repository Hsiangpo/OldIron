"""PasonaCareer HTML 解析器。"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from lxml import html


_TOTAL_RE = re.compile(r"検索結果一覧\s*([\d,]+)\s*件")


def parse_total_results(page_html: str) -> int:
    matched = _TOTAL_RE.search(str(page_html or ""))
    if matched is None:
        return 0
    return int(str(matched.group(1) or "0").replace(",", ""))


def parse_total_pages(page_html: str, per_page: int = 51) -> int:
    total = parse_total_results(page_html)
    if total <= 0:
        return 1
    return (total + per_page - 1) // per_page


def parse_job_cards(page_html: str) -> list[dict[str, str]]:
    tree = html.fromstring(page_html)
    cards: list[dict[str, str]] = []
    for item in tree.cssselect("article.job-info"):
        detail_links = item.cssselect('a.link-job-detail[href^="/job/"]')
        if not detail_links:
            continue
        detail_url = str(detail_links[0].get("href", "") or "").strip()
        title_nodes = item.cssselect("h3.job-info__title div.title")
        company_nodes = item.cssselect("h3.job-info__title div.company p")
        location_nodes = item.cssselect("dt.location + dd")
        company_name = _clean_text(company_nodes[0].text_content()) if company_nodes else ""
        if not detail_url or not company_name:
            continue
        cards.append(
            {
                "detail_url": detail_url,
                "job_title": _clean_text(title_nodes[0].text_content()) if title_nodes else "",
                "company_name": company_name,
                "job_location": _clean_text(location_nodes[0].text_content()) if location_nodes else "",
            }
        )
    return cards


def parse_job_detail(page_html: str) -> dict[str, str]:
    tree = html.fromstring(page_html)
    title = _clean_text(tree.xpath("string(//h1)") or "")
    company_link = tree.cssselect('a[href^="/company/"]')
    company_name = _clean_text(company_link[0].text_content()) if company_link else _extract_company_name_from_title(title)
    address = _extract_labeled_value(tree, "本社所在地")
    website = _normalize_website(_extract_website(tree))
    return {
        "company_name": company_name,
        "representative": "",
        "website": website,
        "address": address,
    }


def _extract_labeled_value(tree, label: str) -> str:
    rows = tree.xpath(f'//th[h3[contains(normalize-space(.), "{label}")]]/following-sibling::td[1]')
    if not rows:
        return ""
    return _clean_text(rows[0].text_content())


def _extract_website(tree) -> str:
    links = tree.xpath('//th[h3[contains(normalize-space(.), "企業URL")]]/following-sibling::td[1]//a/@href')
    return str(links[0] or "").strip() if links else ""


def _extract_company_name_from_title(title: str) -> str:
    matched = re.match(r"(.+?)\s+の【", title)
    return _clean_text(matched.group(1)) if matched is not None else title


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

