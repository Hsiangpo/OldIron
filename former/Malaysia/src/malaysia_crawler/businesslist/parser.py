"""BusinessList 页面解析器。"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from malaysia_crawler.businesslist.models import BusinessListCompany

EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def _clean_text(value: str) -> str:
    return " ".join(value.split()).strip()


def _safe_text(node: object) -> str:
    if node is None:
        return ""
    getter = getattr(node, "get_text", None)
    if callable(getter):
        return _clean_text(getter(" ", strip=True))
    return ""


def _extract_company_id(response_url: str) -> int:
    matched = re.search(r"/company/(\d+)", response_url)
    if not matched:
        return 0
    return int(matched.group(1))


def _is_not_found_page(soup: BeautifulSoup) -> bool:
    title = _safe_text(soup.select_one("title")).lower()
    return "404" in title and "not found" in title


def _looks_like_not_found_text(value: str) -> bool:
    lower = _clean_text(value).lower()
    if not lower:
        return False
    # 中文注释：部分 404 页会伪装成带 #company_name 的详情页，需在解析层硬过滤。
    if lower.startswith("404"):
        return True
    if "page not found" in lower and "error" in lower:
        return True
    return False


def _strip_label_prefix(full_text: str, label_text: str) -> str:
    left = full_text.strip()
    right = label_text.strip()
    if not left or not right:
        return left
    if left.lower().startswith(right.lower()):
        return left[len(right) :].strip(" :-")
    return left


def _extract_info_map(soup: BeautifulSoup) -> dict[str, tuple[str, object]]:
    fields: dict[str, tuple[str, object]] = {}
    for info in soup.select("div.info"):
        label_node = info.select_one("div.label")
        if label_node is None:
            continue
        label_text = _safe_text(label_node)
        if not label_text:
            continue
        value_text = _strip_label_prefix(_safe_text(info), label_text)
        fields[label_text.lower()] = (value_text, info)
    return fields


def _extract_contact_numbers(value_text: str, info_node: object) -> list[str]:
    numbers: list[str] = []
    selector = getattr(info_node, "select", None)
    if callable(selector):
        tel_nodes = info_node.select("a[href^='tel:']")
        for tel in tel_nodes:
            text = _safe_text(tel)
            if text:
                numbers.append(text)
    if not numbers:
        numbers = re.findall(r"\+?\d[\d\s\-]{5,}\d", value_text)
    # 中文注释：去重并保持顺序。
    return list(dict.fromkeys(_clean_text(item) for item in numbers if item.strip()))


def _extract_website_href(info_node: object) -> str:
    selector = getattr(info_node, "select_one", None)
    if not callable(selector):
        return ""
    anchor = info_node.select_one("a[href]")
    if anchor is None:
        return ""
    href = str(anchor.get("href", "")).strip()
    return href


def _extract_contact_email(*candidates: str) -> str:
    for value in candidates:
        matched = EMAIL_PATTERN.search(value)
        if matched:
            return matched.group(0).strip()
    return ""


def _normalize_employee_role(value: str) -> str:
    return _clean_text(value).upper().replace(" ", "")


def _extract_employees(soup: BeautifulSoup) -> list[dict[str, str]]:
    employees: list[dict[str, str]] = []
    for node in soup.select("div.product.employee"):
        name = _safe_text(node.select_one(".product_name"))
        lines = [
            _clean_text(line)
            for line in node.get_text("\n", strip=True).splitlines()
            if _clean_text(line)
        ]
        if name and lines and lines[0] == name:
            lines = lines[1:]
        role = lines[0] if lines else ""
        phone = ""
        for line in lines[1:]:
            if re.search(r"\d{6,}", line):
                phone = line
                break
        if _normalize_employee_role(role) != "DIRECTOR":
            continue
        employees.append({"name": name, "role": role, "phone": phone})
    return employees


def parse_company_page(
    html: str,
    *,
    response_url: str = "",
) -> BusinessListCompany | None:
    soup = BeautifulSoup(html, "lxml")
    if _is_not_found_page(soup):
        return None

    company_id = _extract_company_id(response_url)
    if company_id <= 0:
        return None

    info_map = _extract_info_map(soup)
    company_name = _safe_text(soup.select_one("#company_name"))
    if not company_name:
        company_name = _safe_text(soup.select_one("h1")).split(" - ", 1)[0]
    if _looks_like_not_found_text(company_name):
        return None
    if _looks_like_not_found_text(_safe_text(soup.select_one("h1"))):
        return None

    registration_code = info_map.get("registration code", ("", None))[0]
    address = info_map.get("address", ("", None))[0]
    contact_text, contact_node = info_map.get("contact number", ("", None))
    website_text, website_node = info_map.get("website address", ("", None))
    contact_person = info_map.get("contact person", ("", None))[0]
    company_manager = info_map.get("company manager", ("", None))[0]
    email_action_text = info_map.get("e-mail address", ("", None))[0]
    if not email_action_text:
        email_action_text = info_map.get("email address", ("", None))[0]
    contact_email = _extract_contact_email(contact_person, email_action_text)

    contact_numbers = _extract_contact_numbers(contact_text, contact_node)
    website_href = _extract_website_href(website_node)
    if not website_href and website_text:
        website_href = website_text
    employees = _extract_employees(soup)
    if not company_manager:
        for member in employees:
            name = str(member.get("name", "")).strip()
            if name:
                company_manager = name
                break

    parsed = urlparse(response_url)
    company_url = parsed._replace(query="", fragment="").geturl()
    return BusinessListCompany(
        company_id=company_id,
        company_url=company_url,
        company_name=company_name,
        registration_code=registration_code,
        address=address,
        contact_numbers=contact_numbers,
        website_href=website_href,
        contact_email=contact_email,
        company_manager=company_manager,
        employees=employees,
    )


def parse_redir_target(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    refresh = soup.select_one("meta[http-equiv='refresh']")
    if refresh is not None:
        content = str(refresh.get("content", "")).strip()
        matched = re.search(r"url\s*=\s*([^;]+)$", content, flags=re.I)
        if matched:
            return matched.group(1).strip()

    matched = re.search(r"window\.location(?:\.href)?\s*=\s*['\"]([^'\"]+)", html, flags=re.I)
    if matched:
        return matched.group(1).strip()
    return ""
