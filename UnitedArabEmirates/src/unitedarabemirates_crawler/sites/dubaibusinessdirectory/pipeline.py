"""Dubai Business Directory Pipeline 1。"""

from __future__ import annotations

import html
import logging
import re
import time
from pathlib import Path

from curl_cffi import requests as cffi_requests

from ..common.enrich import normalize_person_name
from ..common.enrich import normalize_website_url
from ..common.store import UaeCompanyStore


LOGGER = logging.getLogger("uae.dubaibusinessdirectory.pipeline")
LIST_URL = (
    "https://dubai-businessdirectory.com/findadealer.php"
    "?city=&main_search=1&m_name=&reg=All&sector=All&nr_display=5&pageno={page}"
)


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.5,
    proxy: str = "",
    max_pages: int = 0,
    concurrency: int = 1,
) -> dict[str, int]:
    """抓取 Dubai Business Directory 列表。"""
    _ = concurrency
    output_dir.mkdir(parents=True, exist_ok=True)
    store = UaeCompanyStore(output_dir / "companies.db")
    checkpoint = store.get_checkpoint("list") or {}
    start_page = max(int(checkpoint.get("last_page", 0) or 0) + 1, 1)
    session = _build_session(proxy)
    page = start_page
    total_pages = 0
    new_companies = 0
    processed_pages = 0
    try:
        while True:
            html_text = _fetch_page(session, page)
            companies = _parse_companies(html_text, page)
            if not companies:
                store.update_checkpoint("list", page - 1, "done")
                break
            new_companies += store.upsert_companies(companies)
            processed_pages += 1
            total_pages = max(total_pages, _parse_total_pages(html_text))
            store.update_checkpoint("list", page, "running")
            LOGGER.info("Dubai Business Directory 页 %d/%s：解析 %d 家", page, total_pages or "?", len(companies))
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
    session.headers.update(
        {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def _fetch_page(session: cffi_requests.Session, page: int) -> str:
    response = session.get(LIST_URL.format(page=page), timeout=30)
    response.raise_for_status()
    return str(response.text or "")


def _parse_companies(html_text: str, page: int) -> list[dict[str, str]]:
    chunks = re.split(r"<tr><td><hr size=\"2\" color=\"#784b04\"></td></tr>", html_text, flags=re.I)
    results: list[dict[str, str]] = []
    for chunk in chunks:
        if "Company Name:" not in chunk:
            continue
        company_name = _extract_field(chunk, r"Company Name:\s*([^<]+)")
        if not company_name:
            continue
        contact_name = _extract_field(chunk, r"Contact name:\s*<b>(.*?)</b>")
        address = _extract_field(chunk, r"Address:\s*([^<]+)")
        phone = _extract_field(chunk, r"Contact Tel:\s*([^<]+)")
        email = _extract_field(chunk, r"mailto:([^\"'>]+)")
        website = _extract_field(chunk, r"Web Address:\s*([^<\s]+)")
        summary = _extract_field(chunk, r"Description of services:\s*(.*?)</td>")
        clean_company = _clean_text(company_name)
        clean_summary = _clean_html_text(summary)
        clean_contact = normalize_person_name(contact_name, clean_company)
        if not clean_contact:
            clean_contact = _extract_contact_from_summary(clean_summary, clean_company)
        results.append(
            {
                "company_name": clean_company,
                "representative_p1": clean_contact,
                "representative_final": clean_contact,
                "website": normalize_website_url(website),
                "address": _clean_text(address),
                "phone": _clean_text(phone),
                "emails": _clean_text(email),
                "detail_url": LIST_URL.format(page=page),
                "summary": clean_summary,
                "evidence_url": LIST_URL.format(page=page),
            }
        )
    return results


def _parse_total_pages(html_text: str) -> int:
    pages = [int(value) for value in re.findall(r"pageno=(\d+)", html_text)]
    return max(pages, default=0)


def _extract_field(chunk: str, pattern: str) -> str:
    matched = re.search(pattern, chunk, flags=re.I | re.S)
    if matched is None:
        return ""
    return str(matched.group(1) or "")


def _clean_text(value: str) -> str:
    text = html.unescape(str(value or "")).replace("\xa0", " ").strip(" \t\r\n|")
    return re.sub(r"\s+", " ", text)


def _clean_html_text(value: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", str(value or ""), flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return _clean_text(text)


def _extract_contact_from_summary(summary: str, company_name: str) -> str:
    text = _clean_text(summary)
    if not text:
        return ""
    patterns = (
        r"Contact Person\s*[:-]\s*([^|,;/]+(?:\s+[^|,;/]+){0,4})",
        r"Business Name\s*/\s*Contact Person\s*[:-]\s*[^/]+/\s*([^|,;]+)",
    )
    for pattern in patterns:
        matched = re.search(pattern, text, flags=re.I)
        if matched is None:
            continue
        raw_candidate = re.split(
            r"\b(?:Country/Region|Street Address|City|State|Postal Code|Phone No|Email|Web)\b\s*[:-]",
            matched.group(1),
            maxsplit=1,
            flags=re.I,
        )[0]
        candidate = normalize_person_name(raw_candidate, company_name)
        if candidate:
            return candidate
    return ""
