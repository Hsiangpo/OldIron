"""CTOS 公共目录页面解析器。"""

from __future__ import annotations

import re
from urllib.parse import urljoin
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from malaysia_crawler.ctos_directory.models import CTOSCompanyDetail
from malaysia_crawler.ctos_directory.models import CTOSCompanyItem
from malaysia_crawler.ctos_directory.models import CTOSDirectoryPage

DEFAULT_BASE_URL = "https://businessreport.ctoscredit.com.my"


def _clean_text(value: str) -> str:
    return " ".join(value.split()).strip()


def _safe_text(node: object) -> str:
    if node is None:
        return ""
    getter = getattr(node, "get_text", None)
    if callable(getter):
        return _clean_text(getter(" ", strip=True))
    return ""


def _extract_listing_from_url(response_url: str) -> tuple[str, int]:
    parsed = urlparse(response_url)
    chunks = [part for part in parsed.path.split("/") if part]
    if len(chunks) < 3:
        return "", 1
    if chunks[-3] != "malaysia-company-listing":
        return "", 1
    prefix = chunks[-2].strip().lower()
    page = chunks[-1].strip()
    if not page.isdigit():
        return prefix, 1
    return prefix, int(page)


def _extract_company_number(detail_path: str) -> str:
    matched = re.search(r"/single-report/malaysia-company/([^/]+)/", detail_path)
    if not matched:
        return ""
    return matched.group(1).strip().upper()


def _fallback_name_from_path(detail_path: str) -> str:
    slug = detail_path.rstrip("/").rsplit("/", 1)[-1]
    return _clean_text(slug.replace("-", " "))


def parse_directory_page(
    html: str,
    *,
    response_url: str = "",
    base_url: str = DEFAULT_BASE_URL,
) -> CTOSDirectoryPage:
    soup = BeautifulSoup(html, "lxml")
    prefix, current_page = _extract_listing_from_url(response_url)

    companies: list[CTOSCompanyItem] = []
    seen_paths: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        if "/single-report/malaysia-company/" not in href:
            continue
        detail_url = urljoin(base_url, href)
        detail_path = urlparse(detail_url).path
        if detail_path in seen_paths:
            continue
        seen_paths.add(detail_path)
        company_name = _safe_text(anchor) or _fallback_name_from_path(detail_path)
        companies.append(
            CTOSCompanyItem(
                company_name=company_name,
                registration_no=_extract_company_number(detail_path),
                detail_path=detail_path,
                detail_url=detail_url,
            )
        )

    next_page = current_page + 1 if companies else None
    return CTOSDirectoryPage(
        prefix=prefix,
        current_page=current_page,
        next_page=next_page,
        companies=companies,
    )


def _extract_table_fields(soup: BeautifulSoup) -> dict[str, str]:
    fields: dict[str, str] = {}
    for head in soup.select("th.tabledetails"):
        key = _safe_text(head)
        if not key:
            continue
        row = getattr(head, "parent", None)
        if row is None:
            continue
        cell = row.find("td")
        if cell is None:
            continue
        fields[key] = _safe_text(cell)
    return fields


def _split_registration(value: str, fallback: str) -> tuple[str, str]:
    clean = _clean_text(value)
    if not clean:
        return fallback, ""
    parts = [part.strip().upper() for part in clean.split("/") if part.strip()]
    if not parts:
        return fallback, ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def parse_company_detail_page(
    html: str,
    detail_url: str,
) -> CTOSCompanyDetail:
    soup = BeautifulSoup(html, "lxml")
    name = _safe_text(soup.select_one("h1")) or _fallback_name_from_path(urlparse(detail_url).path)
    fallback_number = _extract_company_number(urlparse(detail_url).path)
    fields = _extract_table_fields(soup)
    registration = fields.get("Company Registration No.", "")
    company_registration_no, new_registration_no = _split_registration(
        registration,
        fallback=fallback_number,
    )
    nature = fields.get("Nature of Business", "")
    date_of_registration = fields.get("Date of Registration", "")
    state = fields.get("State", "")
    return CTOSCompanyDetail(
        detail_url=detail_url,
        company_name=name,
        company_registration_no=company_registration_no,
        new_registration_no=new_registration_no,
        nature_of_business=nature,
        date_of_registration=date_of_registration,
        state=state,
    )

