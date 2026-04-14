"""DubaiBizDirectory Pipeline 1。"""

from __future__ import annotations

import html
import logging
import re
import time
from pathlib import Path

from bs4 import BeautifulSoup

from ..common.enrich import normalize_person_name
from ..common.enrich import normalize_website_url
from ..common.store import UaeCompanyStore
from .client import DubaiBizDirectoryClient


LOGGER = logging.getLogger("uae.dubaibizdirectory.pipeline")


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str = "",
    max_pages: int = 0,
    concurrency: int = 1,
) -> dict[str, int]:
    """抓取 DubaiBizDirectory 列表与详情。"""
    _ = concurrency
    output_dir.mkdir(parents=True, exist_ok=True)
    store = UaeCompanyStore(output_dir / "companies.db")
    checkpoint = store.get_checkpoint("list") or {}
    start_page = max(int(checkpoint.get("last_page", 0) or 0) + 1, 1)
    client = DubaiBizDirectoryClient(output_dir, proxy)
    page = start_page
    processed_pages = 0
    new_companies = 0
    try:
        while True:
            list_url = _list_url(page)
            html_text = client.fetch_list_html(list_url)
            items = _parse_list_items(html_text)
            if not items:
                store.update_checkpoint("list", page - 1, "done")
                break
            companies = []
            for index, item in enumerate(items, 1):
                try:
                    detail_html = client.fetch_detail_html(item["detail_url"])
                    companies.append(_parse_detail(detail_html, item["detail_url"], item["summary"]))
                except Exception as exc:  # noqa: BLE001
                    _log_detail_skip(page, index, len(items), item["detail_url"], exc)
                if index == 1 or index == len(items) or index % 5 == 0:
                    LOGGER.info("DubaiBizDirectory 页 %d：详情进度 %d/%d", page, index, len(items))
                time.sleep(0.2)
            new_companies += store.upsert_companies(companies)
            processed_pages += 1
            store.update_checkpoint("list", page, "running")
            LOGGER.info("DubaiBizDirectory 页 %d：解析 %d 家", page, len(companies))
            if max_pages > 0 and processed_pages >= max_pages:
                break
            if not _has_next_page(html_text, page):
                store.update_checkpoint("list", page, "done")
                break
            page += 1
            time.sleep(max(request_delay, 0.0))
    finally:
        client.close()
    return {
        "pages": processed_pages,
        "new_companies": new_companies,
        "total_companies": store.get_company_count(),
    }


def _list_url(page: int) -> str:
    return f"https://dubaibizdirectory.com/organisations/search/page:{page}"


def _parse_list_items(html_text: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_text, "html.parser")
    results: list[dict[str, str]] = []
    for block in soup.select("div#results div#result"):
        anchor = block.find("a", href=True)
        summary_tag = block.find("p")
        if anchor is None:
            continue
        detail_url = f"https://dubaibizdirectory.com{anchor.get('href')}"
        results.append(
            {
                "detail_url": detail_url,
                "summary": _clean_text(summary_tag.get_text(" ", strip=True) if summary_tag else ""),
            }
        )
    return results


def _has_next_page(html_text: str, page: int) -> bool:
    return f"/organisations/search/page:{page + 1}" in html_text


def _parse_detail(html_text: str, detail_url: str, summary: str) -> dict[str, str]:
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find("table", class_="table")
    rows = _table_rows(table)
    company_name = _heading_text(soup, "h1")
    contact_name = _extract_contact_name(rows.get("Contact", ""), company_name)
    email = _extract_email(table)
    website = normalize_website_url(_extract_website(rows.get("Website", "")))
    address = _clean_text(rows.get("Location", ""))
    phone = _clean_text(rows.get("Telephone", ""))
    summary_text = summary or _first_paragraph(soup)
    return {
        "company_name": company_name,
        "representative_p1": contact_name,
        "representative_final": contact_name,
        "website": website,
        "address": address,
        "phone": phone,
        "emails": email,
        "detail_url": detail_url,
        "summary": summary_text,
        "evidence_url": detail_url,
    }


def _log_detail_skip(page: int, index: int, total: int, detail_url: str, error: Exception) -> None:
    message = str(error)
    if "404" in message:
        LOGGER.info(
            "DubaiBizDirectory 详情页不存在，已跳过：page=%d item=%d/%d url=%s",
            page,
            index,
            total,
            detail_url,
        )
        return
    LOGGER.warning(
        "DubaiBizDirectory 详情抓取失败，已跳过：page=%d item=%d/%d url=%s error=%s",
        page,
        index,
        total,
        detail_url,
        error,
    )


def _table_rows(table) -> dict[str, str]:
    if table is None:
        return {}
    results: dict[str, str] = {}
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        key = _clean_text(cells[0].get_text(" ", strip=True))
        value = _clean_text(cells[1].get_text(" ", strip=True))
        if key:
            results[key] = value
    return results


def _extract_email(table) -> str:
    if table is None:
        return ""
    coded = table.find("span", class_="__cf_email__")
    if coded is not None and coded.get("data-cfemail"):
        return _decode_cf_email(str(coded.get("data-cfemail") or ""))
    anchor = table.find("a", href=re.compile(r"^mailto:", flags=re.I))
    if anchor is None:
        return ""
    return _clean_text(str(anchor.get("href") or "").replace("mailto:", ""))


def _extract_website(value: str) -> str:
    matched = re.search(r"https?://\S+", str(value or ""))
    return _clean_text(matched.group(0)) if matched is not None else ""


def _heading_text(soup: BeautifulSoup, tag_name: str) -> str:
    tag = soup.find(tag_name)
    return _clean_text(tag.get_text(" ", strip=True) if tag else "")


def _first_paragraph(soup: BeautifulSoup) -> str:
    tag = soup.find("p")
    return _clean_text(tag.get_text(" ", strip=True) if tag else "")


def _extract_contact_name(raw_value: str, company_name: str) -> str:
    clean_text = _clean_text(raw_value)
    direct = normalize_person_name(clean_text, company_name)
    if direct:
        return direct
    matched = re.match(
        r"([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,4})",
        clean_text,
    )
    if matched is None:
        return ""
    return normalize_person_name(matched.group(1), company_name)


def _decode_cf_email(encoded: str) -> str:
    raw = str(encoded or "").strip()
    if len(raw) < 4:
        return ""
    key = int(raw[:2], 16)
    chars: list[str] = []
    for index in range(2, len(raw), 2):
        chars.append(chr(int(raw[index:index + 2], 16) ^ key))
    return _clean_text("".join(chars))


def _clean_text(value: str) -> str:
    text = html.unescape(str(value or "")).replace("\xa0", " ").strip(" \t\r\n|")
    return re.sub(r"\s+", " ", text)
