"""Day Of Dubai Pipeline 1。"""

from __future__ import annotations

import html
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

from ..common.enrich import normalize_website_url
from ..common.store import UaeCompanyStore


LOGGER = logging.getLogger("uae.dayofdubai.pipeline")
LIST_URL = "https://dayofdubai.com/directory/?page={page}"
UAE_MARKERS = (
    "united arab emirates",
    "uae",
    "dubai",
    "abu dhabi",
    "sharjah",
    "ajman",
    "ras al khaimah",
    "fujairah",
    "umm al quwain",
    "umm al quwwain",
    "al ain",
)


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str = "",
    max_pages: int = 0,
    concurrency: int = 8,
) -> dict[str, int]:
    """抓取 Day Of Dubai 列表与详情。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    store = UaeCompanyStore(output_dir / "companies.db")
    checkpoint = store.get_checkpoint("list") or {}
    start_page = max(int(checkpoint.get("last_page", 0) or 0) + 1, 1)
    session = _build_session(proxy)
    page = start_page
    total_pages = 0
    processed_pages = 0
    new_companies = 0
    try:
        while True:
            html_text = _fetch_text(session, LIST_URL.format(page=page))
            detail_urls = _parse_detail_urls(html_text)
            if not detail_urls:
                store.update_checkpoint("list", page - 1, "done")
                break
            total_pages = max(total_pages, _parse_total_pages(html_text))
            companies = _hydrate_details(detail_urls, proxy, concurrency)
            new_companies += store.upsert_companies(companies)
            processed_pages += 1
            store.update_checkpoint("list", page, "running")
            LOGGER.info("Day Of Dubai 页 %d/%s：解析 %d 家", page, total_pages or "?", len(companies))
            if max_pages > 0 and processed_pages >= max_pages:
                break
            if total_pages and page >= total_pages:
                store.update_checkpoint("list", page, "done")
                break
            page += 1
            time.sleep(max(request_delay, 0.0))
    finally:
        session.close()
    return {
        "pages": processed_pages,
        "new_companies": new_companies,
        "total_companies": store.get_company_count(),
    }


def _build_session(proxy: str) -> cffi_requests.Session:
    proxies = {}
    if proxy:
        proxies = {"http": proxy, "https": proxy}
    session = cffi_requests.Session(impersonate="chrome136", proxies=proxies)
    session.trust_env = False
    session.headers.update({"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
    return session


def _fetch_text(session: cffi_requests.Session, url: str) -> str:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return str(response.text or "")


def _parse_detail_urls(html_text: str) -> list[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    results: list[str] = []
    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href.startswith("/directory/"):
            continue
        if href in {"/directory/", "/directory"} or href.endswith("#contact"):
            continue
        full_url = f"https://dayofdubai.com{href}"
        if full_url not in results:
            results.append(full_url)
    return results


def _parse_total_pages(html_text: str) -> int:
    matched = re.search(r"Page\s+1\s+of\s+(\d+)", html_text, flags=re.I)
    return int(matched.group(1)) if matched is not None else 0


def _hydrate_details(detail_urls: list[str], proxy: str, concurrency: int) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(int(concurrency or 1), 1)) as executor:
        futures = {executor.submit(_fetch_detail_record, proxy, url): url for url in detail_urls}
        for future in as_completed(futures):
            detail_url = futures[future]
            try:
                record = future.result()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Day Of Dubai 详情抓取失败，已跳过：url=%s error=%s", detail_url, exc)
                continue
            if record["company_name"]:
                results.append(record)
    return results


def _fetch_detail_record(proxy: str, detail_url: str) -> dict[str, str]:
    session = _build_session(proxy)
    try:
        html_text = _fetch_text(session, detail_url)
    finally:
        session.close()
    soup = BeautifulSoup(html_text, "html.parser")
    company_name = _meta_content(soup, "og:title")
    summary = _meta_content(soup, "og:description")
    phone = _extract_labeled_value(soup, "Contact Number")
    website = _extract_labeled_href(soup, "Website")
    address = _extract_labeled_value(soup, "Address")
    email = _extract_email(soup)
    return {
        "company_name": company_name,
        "representative_p1": "",
        "representative_final": "",
        "website": normalize_website_url(website),
        "address": address,
        "phone": phone,
        "emails": email,
        "detail_url": detail_url,
        "summary": summary,
        "evidence_url": detail_url,
    } if _looks_like_uae_record(phone, address, summary, website) else _empty_record(detail_url)


def _meta_content(soup: BeautifulSoup, name: str) -> str:
    tag = soup.find("meta", attrs={"property": name})
    if tag is None:
        return ""
    return _clean_text(str(tag.get("content") or ""))


def _extract_labeled_value(soup: BeautifulSoup, label: str) -> str:
    for span in soup.find_all("span"):
        text = _clean_text(span.get_text(" ", strip=True))
        if not text.startswith(label):
            continue
        row = span.find_parent("div", class_="row")
        if row is None:
            continue
        value_box = row.find("div", class_="col-md-8")
        if value_box is None:
            continue
        return _clean_text(value_box.get_text(" ", strip=True))
    return ""


def _extract_labeled_href(soup: BeautifulSoup, label: str) -> str:
    for span in soup.find_all("span"):
        text = _clean_text(span.get_text(" ", strip=True))
        if not text.startswith(label):
            continue
        row = span.find_parent("div", class_="row")
        if row is None:
            continue
        value_box = row.find("div", class_="col-md-8")
        if value_box is None:
            continue
        anchor = value_box.find("a", href=True)
        if anchor is None:
            return ""
        return _clean_text(str(anchor.get("href") or ""))
    return ""


def _extract_email(soup: BeautifulSoup) -> str:
    coded = soup.find("span", class_="__cf_email__")
    if coded is not None and coded.get("data-cfemail"):
        return _decode_cf_email(str(coded.get("data-cfemail") or ""))
    anchor = soup.find("a", href=re.compile(r"^mailto:", flags=re.I))
    if anchor is None:
        return ""
    return _clean_text(str(anchor.get("href") or "").replace("mailto:", ""))


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


def _looks_like_uae_record(phone: str, address: str, summary: str, website: str) -> bool:
    normalized_phone = re.sub(r"\D+", "", str(phone or ""))
    website_text = str(website or "").lower()
    if normalized_phone.startswith("971") or normalized_phone.startswith("00971"):
        return True
    if normalized_phone.startswith("0") and len(normalized_phone) >= 7:
        return True
    if normalized_phone and ".ae" not in website_text:
        return False
    if ".ae" in website_text:
        return True
    address_text = str(address or "").lower()
    if any(marker in address_text for marker in UAE_MARKERS):
        return True
    summary_text = str(summary or "").lower()
    if not normalized_phone and any(marker in summary_text for marker in UAE_MARKERS):
        return True
    return False


def _empty_record(detail_url: str) -> dict[str, str]:
    return {
        "company_name": "",
        "representative_p1": "",
        "representative_final": "",
        "website": "",
        "address": "",
        "phone": "",
        "emails": "",
        "detail_url": detail_url,
        "summary": "",
        "evidence_url": detail_url,
    }
