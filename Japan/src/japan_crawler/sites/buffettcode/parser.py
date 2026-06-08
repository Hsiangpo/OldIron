# -*- coding: utf-8 -*-
"""buffettcode 页面解析：行业总索引 / 行业列表页 / 公司详情页。"""
from __future__ import annotations

import re
from lxml import html as lxml_html


# 老板指定不采的 10 个日本標準産業分類顶层行业（ID 已实测核对）
EXCLUDED_INDUSTRY_IDS = {
    "1000062",  # 銀行業
    "1000064",  # 貸金業，クレジットカード業等非預金信用機関
    "1000071",  # 学術・開発研究機関
    "1000078",  # 洗濯・理容・美容・浴場業
    "1000081",  # 学校教育
    "1000085",  # 社会保険・社会福祉・介護事業
    "1000087",  # 協同組合（他に分類されないもの）
    "1000093",  # 政治・経済・文化団体
    "1000094",  # 宗教
    "1000098",  # 地方公務
}

_INDUSTRY_HREF = re.compile(r"/industries/(1000\d+)")
_COMPANY_HREF = re.compile(r'href="(/company/[^"#?]+)"')
_PAGE_PARAM = re.compile(r"[?&]page=(\d+)")
_COUNT_IN_NAME = re.compile(r"\(([\d,]+)\)")


def parse_industry_index(html_text: str) -> list[tuple[str, str, int]]:
    """从 /industries 解析顶层行业（1000xxx），返回 (id, name, count)，已排除 10 个不采分类。"""
    doc = lxml_html.fromstring(html_text)
    seen: dict[str, tuple[str, int]] = {}
    for a in doc.xpath('//a[contains(@href, "/industries/1000")]'):
        m = _INDUSTRY_HREF.search(a.get("href", ""))
        if not m:
            continue
        iid = m.group(1)
        if iid in EXCLUDED_INDUSTRY_IDS or iid in seen:
            continue
        text = re.sub(r"\s+", " ", a.text_content()).strip()
        cm = _COUNT_IN_NAME.search(text)
        cnt = int(cm.group(1).replace(",", "")) if cm else 0
        name = _COUNT_IN_NAME.sub("", text).strip()
        seen[iid] = (name, cnt)
    return [(iid, nm, ct) for iid, (nm, ct) in seen.items()]


def parse_list_page(html_text: str) -> tuple[list[str], int]:
    """解析行业列表/排名页：返回 (公司详情链接列表, 该行业总页数)。"""
    paths = []
    seen = set()
    for m in _COMPANY_HREF.finditer(html_text):
        p = m.group(1)
        if not p.endswith("/"):
            p += "/"
        if p not in seen:
            seen.add(p)
            paths.append(p)
    pages = [int(x) for x in _PAGE_PARAM.findall(html_text)]
    total_pages = max(pages) if pages else 1
    return paths, total_pages


def _clean(text: str | None) -> str:
    if not text:
        return ""
    v = re.sub(r"\s+", " ", text).strip().lstrip(":").strip()
    # 站点用 N/A 等表示缺失，统一归一为空，避免被当成有值
    return "" if v.upper() in ("N/A", "NA", "-", "—", "ー", "−") else v


def parse_detail(html_text: str) -> dict:
    """解析公司详情页，取 公司名 / 代表者 / 官网 / 住所 / 資本金。"""
    doc = lxml_html.fromstring(html_text)
    fields: dict[str, str] = {}
    for tr in doc.xpath("//tr[th and td]"):
        th = re.sub(r"\s+", "", tr.xpath("string(th)"))
        if not th or th in fields:
            continue
        # 官网优先取 td 内 <a> 的 href，取不到再用文本
        if th == "会社HP":
            hrefs = tr.xpath(".//td//a/@href")
            fields[th] = hrefs[0].strip() if hrefs else _clean(tr.xpath("string(td)"))
        else:
            fields[th] = _clean(tr.xpath("string(td)"))
    name = _clean(doc.xpath("string(//h1)"))
    if not name:
        og = doc.xpath('//meta[@property="og:title"]/@content')
        if og:
            name = re.sub(r"\s*の企業情報.*$", "", og[0]).strip()
    return {
        "company_name": name,
        "representative": fields.get("代表者", ""),
        "website": fields.get("会社HP", ""),
        "address": fields.get("住所", ""),
        "capital": fields.get("資本金", ""),
    }
