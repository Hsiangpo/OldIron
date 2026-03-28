"""hellowork HTML 解析器。

解析两类页面：
  1. 搜索结果列表页 — 提取详情 URL、总件数、分页信息
  2. 详情页 — 提取企业全量字段
"""

from __future__ import annotations

import re
import urllib.parse
from typing import Any

from lxml import html as lxml_html

BASE_URL = "https://www.hellowork.mhlw.go.jp/kensaku"

# 不公开企业的标识文本
HIDDEN_MARKER = "事業所の意向により公開していません"


# ══════════════════════════════════════
# 搜索结果页解析
# ══════════════════════════════════════

def parse_total_count(page_html: str) -> int:
    """从搜索结果页提取总求人件数。"""
    m = re.search(r'name="kyujinkensu"\s+value="(\d+)"', page_html)
    return int(m.group(1)) if m else 0


def parse_detail_urls(page_html: str) -> list[str]:
    """从搜索结果页提取所有详情页 URL（去重）。"""
    pattern = (
        r'action=dispDetailBtn&amp;'
        r'kJNo=([^&]+)&amp;'
        r'kJKbn=(\d)&amp;'
        r'jGSHNo=([^&]+)&amp;'
        r'fullPart=(\d)'
    )
    seen = set()
    urls = []
    for m in re.finditer(pattern, page_html):
        kj_no, kj_kbn, jgsh_no_raw, full_part = m.groups()
        # jGSHNo 在 HTML 中被 HTML entity 编码，先还原
        jgsh_no = jgsh_no_raw.replace("&amp;", "&")
        # 用事業所番号(jGSHNo)去重 — 同一企业多个职位只取一个详情
        if jgsh_no in seen:
            continue
        seen.add(jgsh_no)
        url = (
            f"{BASE_URL}/GECA110010.do?screenId=GECA110010"
            f"&action=dispDetailBtn"
            f"&kJNo={kj_no}"
            f"&kJKbn={kj_kbn}"
            f"&jGSHNo={urllib.parse.quote(urllib.parse.unquote(jgsh_no), safe='')}"
            f"&fullPart={full_part}"
            f"&tatZngy=&shogaiKbn=0"
        )
        urls.append(url)
    return urls


def parse_total_pages(total_count: int, per_page: int = 50) -> int:
    """根据总件数计算总页数。"""
    if total_count <= 0:
        return 0
    return (total_count + per_page - 1) // per_page


# ══════════════════════════════════════
# 详情页解析
# ══════════════════════════════════════

def parse_detail_page(page_html: str) -> dict[str, str] | None:
    """解析详情页，提取企业关键字段。

    若企业选择不公开信息，返回 None。
    """
    if HIDDEN_MARKER in page_html:
        # 检查是否是事業所名不公开（整个企业都隐藏）
        m = re.search(r'事業所名.*?' + re.escape(HIDDEN_MARKER), page_html, re.DOTALL)
        if m:
            return None

    fields: dict[str, str] = {}

    # 使用 lxml 解析
    try:
        tree = lxml_html.fromstring(page_html)
    except Exception:
        return None

    # 事業所名：优先从 div[name="jgshMei"] 提取（避免把读音混入）
    jgsh_mei = tree.cssselect('div[name="jgshMei"], div[id="ID_jgshMei"]')
    if jgsh_mei:
        raw_name = _clean_text(jgsh_mei[0].text_content())
        if raw_name and HIDDEN_MARKER not in raw_name:
            fields["company_name"] = raw_name

    rows = tree.cssselect("tr")
    for row in rows:
        ths = row.cssselect("th")
        tds = row.cssselect("td")
        if not ths or not tds:
            continue
        key = _clean_text(ths[0].text_content())
        val = _clean_text(tds[0].text_content())
        if key and val:
            _extract_field(fields, key, val, tds[0])

    # 提取 homepage link（可能在 <a> 标签里）
    if not fields.get("website"):
        for row in rows:
            th = row.cssselect("th")
            if th and "ホームページ" in th[0].text_content():
                td = row.cssselect("td")
                if td:
                    links = td[0].cssselect("a[href]")
                    for a in links:
                        href = a.get("href", "")
                        if href.startswith("http") and "hellowork" not in href:
                            fields["website"] = href
                            break

    # 提取详情页 URL 本身（用于记录来源）
    # 由调用方负责传入

    if not fields.get("company_name"):
        return None

    return fields


def _extract_field(fields: dict[str, str], key: str, val: str, td_elem: Any) -> None:
    """根据 th 文本匹配提取对应字段。"""
    if "事業所名" in key and "company_name" not in fields:
        # 后备逻辑（div[name="jgshMei"] 未命中时使用）
        fields["company_name"] = val.replace("画像あり", "").strip()

    elif "代表者名" in key:
        # 格式: "役職代表取締役 代表者名大津 進"
        m = re.search(r"代表者名\s*(.+)", val)
        fields["representative"] = m.group(1).strip() if m else val.strip()

    elif "ホームページ" in key:
        # 从文本提取 URL
        m = re.search(r"(https?://\S+)", val)
        if m:
            fields["website"] = m.group(1).strip()

    elif key == "所在地":
        fields["address"] = val.strip()

    elif "産業分類" in key:
        fields["industry"] = val.strip()

    elif "法人番号" in key:
        m = re.search(r"(\d{13})", val)
        if m:
            fields["corp_number"] = m.group(1)

    elif "従業員数" in key:
        # "企業全体704人" → "704人"
        m = re.search(r"企業全体\s*(\S+)", val)
        fields["employees"] = m.group(1) if m else val.strip()

    elif "資本金" in key and "資本金" not in key[:3]:
        # 避免匹配到"基本給"等
        pass
    elif key == "資本金":
        fields["capital"] = val.strip()

    elif "設立年" in key:
        fields["founded_year"] = val.strip()

    elif "電話番号" in key and "担当者" not in key:
        # 事業所电话（非担当者电话）
        if "phone" not in fields:
            fields["phone"] = val.strip()

    elif "担当者" in key:
        # 从担当者信息中提取电话
        m = re.search(r"電話番号\s*([\d\-]+)", val)
        if m and "phone" not in fields:
            fields["phone"] = m.group(1)


def _clean_text(text: str) -> str:
    """清理文本：去掉多余空白、JS 代码等。"""
    # 移除 JS 函数体
    text = re.sub(r"function\s+\w+\([^)]*\)\s*\{[^}]*\}", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_katakana_only(text: str) -> bool:
    """判断文本是否全为片假名（含空格和中点）。"""
    return bool(re.match(r"^[\u30A0-\u30FF\u3000\s・　]+$", text))
