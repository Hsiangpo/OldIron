"""HiDubai Pipeline 1。"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

from ..common.enrich import normalize_website_url
from ..common.store import UaeCompanyStore


LOGGER = logging.getLogger("uae.hidubai.pipeline")
SEARCH_URL = (
    "https://api.hidubai.com/local-businesses/search"
    "?lat=25.197965&lon=55.273985&page={page}&place=All+of+Dubai&q=Company&size=80"
)
DETAIL_URL = "https://www.hidubai.com/businesses/{slug}"
PAGE_FETCH_RETRIES = 4


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str = "",
    max_pages: int = 0,
    concurrency: int = 8,
) -> dict[str, int]:
    """抓取 HiDubai 列表与详情。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    store = UaeCompanyStore(output_dir / "companies.db")
    checkpoint = store.get_checkpoint("list") or {}
    start_page = int(checkpoint.get("last_page", -1) or -1) + 1
    session = _build_session(proxy)
    page = max(start_page, 0)
    processed_pages = 0
    new_companies = 0
    try:
        while True:
            items, session = _fetch_page_items_with_retry(session, proxy, page)
            if not items:
                store.update_checkpoint("list", max(page - 1, 0), "done")
                break
            companies = _hydrate_page_items(items, proxy, concurrency)
            new_companies += store.upsert_companies(companies)
            processed_pages += 1
            store.update_checkpoint("list", page, "running")
            LOGGER.info("HiDubai 页 %d：解析 %d 家", page, len(companies))
            if max_pages > 0 and processed_pages >= max_pages:
                break
            if len(items) < 80:
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
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.hidubai.com",
            "Referer": "https://www.hidubai.com/",
            "wl-channel": "pp",
            "wl-guestuserid": os.getenv("HIDUBAI_GUEST_USER_ID", "cc60d8b7-4d35-4980-93ed-4dbb9b4c04fa"),
        }
    )
    return session


def _fetch_page_items(session: cffi_requests.Session, page: int) -> list[dict]:
    response = session.get(SEARCH_URL.format(page=page), timeout=30)
    if response.status_code == 204:
        LOGGER.info("HiDubai 列表页已到末尾：page=%d status=204", page)
        return []
    response.raise_for_status()
    if not str(response.text or "").strip():
        raise ValueError(f"HiDubai 返回空正文：page={page} status={response.status_code}")
    payload = response.json()
    return list(payload.get("_embedded", {}).get("localBusinesses", []))


def _fetch_page_items_with_retry(
    session: cffi_requests.Session,
    proxy: str,
    page: int,
) -> tuple[list[dict], cffi_requests.Session]:
    current_session = session
    for attempt in range(1, PAGE_FETCH_RETRIES + 1):
        try:
            return _fetch_page_items(current_session, page), current_session
        except Exception as exc:  # noqa: BLE001
            if attempt >= PAGE_FETCH_RETRIES:
                raise
            LOGGER.warning(
                "HiDubai 列表页抓取失败，重建 session 后重试：page=%d attempt=%d/%d error=%s",
                page,
                attempt,
                PAGE_FETCH_RETRIES,
                exc,
            )
            try:
                current_session.close()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(min(attempt * 2, 6))
            current_session = _build_session(proxy)
    return [], current_session


def _hydrate_page_items(items: list[dict], proxy: str, concurrency: int) -> list[dict[str, str]]:
    thread_local = threading.local()

    def _get_session() -> cffi_requests.Session:
        session = getattr(thread_local, "session", None)
        if session is not None:
            return session
        thread_local.session = _build_detail_session(proxy)
        return thread_local.session

    def _worker(item: dict) -> dict[str, str]:
        detail_url = DETAIL_URL.format(slug=item.get("friendlyUrlName", ""))
        detail = _fetch_detail(_get_session(), detail_url)
        company_name = str(item.get("businessName", {}).get("en", "")).strip()
        address = _join_address(
            str(item.get("address", {}).get("en", "")),
            str(item.get("neighborhood", {}).get("name", {}).get("en", "")),
            str(item.get("neighborhood", {}).get("districtName", {}).get("en", "")),
        )
        phone = str(item.get("contactPhone") or item.get("mobilePhone") or "").strip()
        website = normalize_website_url(str(item.get("website") or detail["website"] or "").strip())
        summary = ", ".join(item.get("businessKeywords", {}).get("en", [])[:5])
        return {
            "company_name": company_name,
            "representative_p1": "",
            "representative_final": "",
            "website": website,
            "address": address,
            "phone": phone or detail["phone"],
            "emails": detail["email"],
            "detail_url": detail_url,
            "summary": summary,
            "evidence_url": detail_url,
        }

    results: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(int(concurrency or 1), 1)) as executor:
        futures = {executor.submit(_worker, item): item for item in items}
        for future in as_completed(futures):
            item = futures[future]
            try:
                record = future.result()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "HiDubai 详情抓取失败，已降级为列表字段：company=%s url=%s error=%s",
                    str(item.get("businessName", {}).get("en", "")).strip(),
                    DETAIL_URL.format(slug=item.get("friendlyUrlName", "")),
                    exc,
                )
                record = _build_fallback_record(item)
            results.append(record)
    return results


def _build_detail_session(proxy: str) -> cffi_requests.Session:
    proxies = {}
    if proxy:
        proxies = {"http": proxy, "https": proxy}
    session = cffi_requests.Session(impersonate="chrome136", proxies=proxies)
    session.trust_env = False
    session.headers.update({"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
    return session


def _fetch_detail(session: cffi_requests.Session, detail_url: str) -> dict[str, str]:
    response = session.get(detail_url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(str(response.text or ""), "html.parser")
    return {
        "email": _find_meta_value(soup, "email"),
        "phone": _find_meta_value(soup, "telephone"),
        "website": _find_website_href(soup),
    }


def _find_meta_value(soup: BeautifulSoup, name: str) -> str:
    tag = soup.find("meta", attrs={"itemprop": name})
    return str(tag.get("content") or "").strip() if tag else ""


def _find_website_href(soup: BeautifulSoup) -> str:
    meta_tag = soup.find("meta", attrs={"itemprop": "url"})
    if meta_tag is not None:
        candidate = normalize_website_url(str(meta_tag.get("content") or ""))
        if candidate:
            return candidate
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        candidate = normalize_website_url(href)
        if not candidate:
            continue
        anchor_text = anchor.get_text(" ", strip=True).lower()
        parent_text = anchor.find_parent().get_text(" ", strip=True).lower() if anchor.find_parent() else ""
        if "website" not in anchor_text and "website" not in parent_text and "visit" not in anchor_text:
            continue
        return candidate
    return ""


def _join_address(*parts: str) -> str:
    result: list[str] = []
    for part in parts:
        clean = str(part or "").strip()
        if clean and clean not in result:
            result.append(clean)
    return ", ".join(result)


def _build_fallback_record(item: dict) -> dict[str, str]:
    company_name = str(item.get("businessName", {}).get("en", "")).strip()
    address = _join_address(
        str(item.get("address", {}).get("en", "")),
        str(item.get("neighborhood", {}).get("name", {}).get("en", "")),
        str(item.get("neighborhood", {}).get("districtName", {}).get("en", "")),
    )
    phone = str(item.get("contactPhone") or item.get("mobilePhone") or "").strip()
    website_raw = str(item.get("website") or "").strip()
    if website_raw and "://" not in website_raw:
        website_raw = f"https://{website_raw}"
    website = normalize_website_url(website_raw)
    summary = ", ".join(item.get("businessKeywords", {}).get("en", [])[:5])
    detail_url = DETAIL_URL.format(slug=item.get("friendlyUrlName", ""))
    return {
        "company_name": company_name,
        "representative_p1": "",
        "representative_final": "",
        "website": website,
        "address": address,
        "phone": phone,
        "emails": "",
        "detail_url": detail_url,
        "summary": summary,
        "evidence_url": detail_url,
    }
