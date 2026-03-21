from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.core.config import DEFAULT_BASE_URL
from src.utils.cf_email import decode_cfemail


HEADER_ROW = ["CIN", "Name", "Status", "Paid Up Capital", "Address"]


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def decode_cf_emails(soup: BeautifulSoup) -> None:
    for tag in soup.find_all(attrs={"data-cfemail": True}):
        decoded = decode_cfemail(tag.get("data-cfemail", ""))
        if decoded:
            tag.replace_with(decoded)


def extract_total_pages(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    match = re.search(r"Page\s+\d+\s+of\s+([\d,]+)", text, re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_list_page(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    target_table = None
    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        first_cells = rows[0].find_all(["td", "th"])
        if first_cells and clean_text(first_cells[0].get_text(" ", strip=True)) == "CIN":
            target_table = table
            break
    if not target_table:
        return []
    rows = target_table.find_all("tr")
    companies: List[Dict[str, str]] = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        values = [clean_text(c.get_text(" ", strip=True)) for c in cells[:5]]
        if values == HEADER_ROW:
            continue
        cin = values[0]
        name = values[1]
        status = values[2]
        paid_up_capital = values[3]
        address = values[4]
        link = cells[0].find("a") or cells[1].find("a")
        detail_url = link.get("href") if link else ""
        if detail_url:
            detail_url = urljoin(DEFAULT_BASE_URL, detail_url)
        companies.append(
            {
                "cin": cin,
                "name": name,
                "status": status,
                "paid_up_capital": paid_up_capital,
                "address": address,
                "detail_url": detail_url,
            }
        )
    return companies


def parse_detail_page(html: str) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    decode_cf_emails(soup)
    basic_info = _extract_basic_information(soup)
    contact_details = _extract_contact_details(soup)
    current_director = _extract_current_director(soup)
    return basic_info, contact_details, current_director


def _extract_basic_information(soup: BeautifulSoup) -> Dict[str, str]:
    header = _find_h3_by_text(soup, "Basic Information")
    if not header:
        return {}
    table = header.find_next("table")
    if not table:
        return {}
    info: Dict[str, str] = {}
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        key = clean_text(cells[0].get_text(" ", strip=True))
        value = clean_text(cells[1].get_text(" ", strip=True))
        if key:
            info[key] = value
    return info


def _extract_contact_details(soup: BeautifulSoup) -> Dict[str, str]:
    header = _find_h3_startswith(soup, "Contact Details")
    if not header:
        return {}
    container = header.find_next(id="contact-details-content")
    if not container:
        container = header.find_next("div")
    if not container:
        return {}

    text = container.get_text("\n", strip=True)
    lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]
    details: Dict[str, str] = {}
    current_key: Optional[str] = None
    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            key = clean_text(key)
            value = clean_text(value)
            details[key] = value
            current_key = key if value == "" else None
        else:
            if current_key:
                details[current_key] = clean_text(f"{details.get(current_key, '')} {line}")
    return details


def _extract_current_director(soup: BeautifulSoup) -> Dict[str, str]:
    header_tokens = {"din", "director name", "designation", "appointment date"}
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        header_row_index = None
        for idx, row in enumerate(rows):
            cells = row.find_all(["td", "th"])
            if len(cells) < 4:
                continue
            values = [clean_text(c.get_text(" ", strip=True)).lower() for c in cells]
            if "cessation" in values:
                header_row_index = None
                break
            if header_tokens.issubset(set(values)):
                header_row_index = idx
                break
        if header_row_index is None:
            continue
        for row in rows[header_row_index + 1 :]:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            values = [clean_text(c.get_text(" ", strip=True)) for c in cells[:4]]
            if not any(values):
                continue
            return {
                "DIN": values[0],
                "Director Name": values[1],
                "Designation": values[2],
                "Appointment Date": values[3],
            }
    return {}


def _find_h3_by_text(soup: BeautifulSoup, text: str):
    for h3 in soup.find_all("h3"):
        if clean_text(h3.get_text(" ", strip=True)) == text:
            return h3
    return None


def _find_h3_startswith(soup: BeautifulSoup, prefix: str):
    prefix = prefix.lower()
    for h3 in soup.find_all("h3"):
        value = clean_text(h3.get_text(" ", strip=True)).lower()
        if value.startswith(prefix):
            return h3
    return None
