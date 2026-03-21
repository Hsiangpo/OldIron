"""indonesiayp 页面解析逻辑。"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from indonesia_crawler.models import CompanyRecord

COMPANY_LINK_PATTERN = re.compile(r"/company/(\d+)/[^?#\"']+")
EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
GENERIC_ANCHOR_TEXTS = {"view profile", "send enquiry", "map", "e-mail", "website", "detail"}


def _normalize_text(raw: str) -> str:
    """标准化文本空白。"""
    return " ".join(raw.split()).strip()


def _is_company_anchor_text(text: str) -> bool:
    """判断锚文本是否可作为公司名。"""
    if not text:
        return False
    lower = text.lower().strip()
    if lower in GENERIC_ANCHOR_TEXTS:
        return False
    if lower.startswith("http://") or lower.startswith("https://"):
        return False
    return True


def extract_total_pages(html: str) -> int:
    """从分页区域提取总页数。"""
    soup = BeautifulSoup(html, "html.parser")
    max_page = 1
    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "")
        match = re.search(r"/category/general_business/(\d+)$", href)
        if match:
            max_page = max(max_page, int(match.group(1)))
        text = _normalize_text(anchor.get_text(" ", strip=True))
        if text.isdigit():
            max_page = max(max_page, int(text))
    return max_page


def parse_list_page(html: str) -> list[CompanyRecord]:
    """解析列表页，提取公司ID、公司名和详情路径。"""
    soup = BeautifulSoup(html, "html.parser")
    records: dict[str, CompanyRecord] = {}

    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "")
        match = COMPANY_LINK_PATTERN.search(href)
        if not match:
            continue

        company_id = match.group(1)
        detail_path = match.group(0)
        text = _normalize_text(anchor.get_text(" ", strip=True))

        if company_id not in records:
            records[company_id] = CompanyRecord(
                comp_id=f"IYP_{company_id}",
                company_name="",
                detail_path=detail_path,
            )

        # 只在识别到真实公司名时更新公司名，避免被 “View Profile” 覆盖。
        if _is_company_anchor_text(text):
            records[company_id].company_name = text
            records[company_id].detail_path = detail_path

    return [record for record in records.values() if record.company_name and record.detail_path]


def normalize_homepage(raw: str) -> str:
    """标准化官网 URL。"""
    value = raw.strip()
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("www."):
        return f"https://{value}"
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if "." in value and " " not in value:
        return f"https://{value}"
    return ""


def _extract_value_after_label(soup: BeautifulSoup, label: str) -> str:
    """根据标签文本提取其后一个值。"""
    target = soup.find(string=lambda text: isinstance(text, str) and _normalize_text(text).lower() == label.lower())
    if target is None:
        return ""

    node = target.parent
    # 某些详情块中，标签和数值在同一个 info 容器内。
    info_block = node.find_parent("div", class_="info")
    if info_block is not None:
        block_text = _normalize_text(info_block.get_text(" ", strip=True))
        if block_text.lower().startswith(label.lower()):
            value = block_text[len(label):].strip(" :")
            if value:
                return value

    # 兜底：标签和数值直接在同一节点中。
    container_text = _normalize_text(node.get_text(" ", strip=True))
    if container_text.lower().startswith(label.lower()):
        value = container_text[len(label):].strip(" :")
        if value:
            return value

    for _ in range(5):
        if node is None:
            break
        node = node.find_next_sibling()
        if node is None:
            break
        text = _normalize_text(node.get_text(" ", strip=True))
        if text:
            return text
    return ""


def _extract_homepage(soup: BeautifulSoup) -> str:
    """提取官网地址，兼容 redir 参数。"""
    label_node = soup.find(
        string=lambda text: isinstance(text, str) and _normalize_text(text).lower() == "website address"
    )
    if label_node is None:
        return ""

    anchor = label_node.parent.find_next("a", href=True)
    if anchor is None:
        return ""

    href = anchor.get("href", "").strip()
    text_value = _normalize_text(anchor.get_text(" ", strip=True))
    if "/redir/" in href and "u=" in href:
        parsed = urlparse(href)
        query = parse_qs(parsed.query)
        value = query.get("u", [""])[0]
        return normalize_homepage(value or text_value)
    return normalize_homepage(href or text_value)


def _extract_emails(soup: BeautifulSoup) -> list[str]:
    """提取页面中的邮箱地址并去重。"""
    found: list[str] = []
    for anchor in soup.select("a[href^='mailto:']"):
        email = anchor.get("href", "").replace("mailto:", "", 1).strip().lower()
        if email:
            found.append(email)

    all_text = soup.get_text(" ", strip=True)
    found.extend(email.lower() for email in EMAIL_PATTERN.findall(all_text))

    unique: list[str] = []
    seen: set[str] = set()
    for email in found:
        if email in seen:
            continue
        seen.add(email)
        unique.append(email)
    return unique


def parse_detail_page(html: str, detail_path: str = "") -> CompanyRecord:
    """解析详情页，提取公司名、负责人、官网和邮箱。"""
    soup = BeautifulSoup(html, "html.parser")
    heading = _normalize_text(soup.find("h1").get_text(" ", strip=True)) if soup.find("h1") else ""
    company_name = heading.split(" - ", 1)[0].strip() if heading else ""

    record = CompanyRecord(
        company_name=company_name,
        ceo=_extract_value_after_label(soup, "company manager"),
        homepage=_extract_homepage(soup),
        emails=_extract_emails(soup),
        detail_path=detail_path,
    )
    return record
