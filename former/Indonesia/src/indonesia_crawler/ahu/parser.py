"""AHU 页面与 JSON 响应解析。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from bs4 import BeautifulSoup


@dataclass(slots=True)
class AhuSearchResult:
    """AHU 搜索结果。"""

    nama_korporasi: str
    alamat: str
    detail_id: str


@dataclass(slots=True)
class AhuDetail:
    """AHU 详情结果。"""

    nama_korporasi: str = ""
    pemilik_manfaat: list[str] = field(default_factory=list)


def _normalize_text(raw: str) -> str:
    """压缩空白字符。"""
    return " ".join(raw.split()).strip()


def extract_form_token(html: str) -> str:
    """提取搜索页隐藏 token `mxyplyzyk`。"""
    soup = BeautifulSoup(html, "html.parser")
    token_input = soup.select_one("input[name='mxyplyzyk']")
    if token_input is None:
        return ""
    return str(token_input.get("value", "")).strip()


def parse_search_results(html: str) -> list[AhuSearchResult]:
    """解析搜索结果列表。"""
    soup = BeautifulSoup(html, "html.parser")
    results: list[AhuSearchResult] = []
    for row in soup.select("#hasil_cari .cl0"):
        corp_name = _normalize_text(row.select_one(".judul").get_text(" ", strip=True)) if row.select_one(".judul") else ""
        address = _normalize_text(row.select_one(".alamat").get_text(" ", strip=True)) if row.select_one(".alamat") else ""
        detail_node = row.select_one(".detail_pemilik_manfaat")
        detail_id = str(detail_node.get("data-id", "")).strip() if detail_node else ""
        if corp_name and detail_id:
            results.append(AhuSearchResult(nama_korporasi=corp_name, alamat=address, detail_id=detail_id))
    return results


def parse_detail_payload(payload_text: str) -> AhuDetail:
    """解析详情接口 JSON。"""
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return AhuDetail()

    values = payload.get("value", [])
    if not isinstance(values, list) or not values:
        return AhuDetail()

    first = values[0] if isinstance(values[0], dict) else {}
    corp_name = str(first.get("nama_korporasi", "")).strip()
    owners = []
    for item in first.get("data_pemilik_manfaat", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("nama_lengkap", "")).strip()
        if name:
            owners.append(name)
    return AhuDetail(nama_korporasi=corp_name, pemilik_manfaat=owners)

