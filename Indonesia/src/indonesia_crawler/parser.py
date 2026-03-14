"""HTML 解析器 — 从 GAPENSI 页面 HTML 提取公司信息。"""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from .models import CompanyRecord

logger = logging.getLogger(__name__)


def _extract_modal_field(modal_soup: BeautifulSoup, label: str) -> str:
    """从 modal 弹窗中提取指定标签的值。"""
    for li in modal_soup.select("li"):
        divs = li.find_all("div")
        if len(divs) >= 3:
            field_label = divs[0].get_text(strip=True)
            if field_label == label:
                return divs[2].get_text(strip=True)
    return ""


def _extract_emails_from_text(text: str) -> list[str]:
    """用正则从文本中提取所有邮箱地址。"""
    pattern = r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
    found = re.findall(pattern, text)
    # 去重并保持顺序
    seen: set[str] = set()
    result: list[str] = []
    for email in found:
        lower = email.lower().strip()
        if lower not in seen:
            seen.add(lower)
            result.append(lower)
    return result


def parse_page(html: str) -> list[CompanyRecord]:
    """
    解析一页 HTML，返回公司记录列表。

    数据来源：页面中的 modal 弹窗（id=modal_companyN），每个 modal 包含：
    - Nama Badan Usaha (公司名)
    - Nama Pimpinan (法人)
    - Email (邮箱)
    - Alamat (地址)
    - Kabupaten Kota (市/县)
    - Propinsi (省份)
    - Nomor Sertifikat (注册号)
    - Kualifikasi (资质等级)
    """
    soup = BeautifulSoup(html, "html.parser")
    records: list[CompanyRecord] = []

    # 提取总数信息（用于日志）
    total_text = soup.find(string=re.compile(r"Ditemukan \d+ Anggota"))
    if total_text:
        match = re.search(r"Ditemukan (\d+) Anggota", total_text)
        if match:
            logger.debug("页面总数: %s", match.group(1))

    # 遍历所有公司 modal
    modal_idx = 1
    while True:
        modal = soup.find(id=f"modal_company{modal_idx}")
        if modal is None:
            break

        company_name = _extract_modal_field(modal, "Nama Badan Usaha")
        ceo = _extract_modal_field(modal, "Nama Pimpinan")
        email_raw = _extract_modal_field(modal, "Email")
        address = _extract_modal_field(modal, "Alamat")
        city = _extract_modal_field(modal, "Kabupaten Kota")
        province = _extract_modal_field(modal, "Propinsi")
        reg_no = _extract_modal_field(modal, "Nomor Sertifikat")
        qualification = _extract_modal_field(modal, "Kualifikasi")

        emails = _extract_emails_from_text(email_raw) if email_raw else []

        if company_name:
            record = CompanyRecord(
                company_name=company_name,
                ceo=ceo,
                emails=emails,
                address=address,
                province=province,
                city=city,
                registration_no=reg_no,
                qualification=qualification,
            )
            records.append(record)

        modal_idx += 1

    return records


def extract_total_count(html: str) -> int:
    """从页面提取总记录数。"""
    match = re.search(r"Ditemukan (\d+) Anggota", html)
    if match:
        return int(match.group(1))
    return 0
