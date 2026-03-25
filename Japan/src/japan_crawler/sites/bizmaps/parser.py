"""bizmaps HTML 解析器。

基于实际抓包分析的 biz-maps.com 列表页 HTML 结构：
  - 公司名: a[href*="/item/"] 链接
  - 详情: 每家公司一个 7 行 table（業種/住所/設立年度/代表者名/資本金等/電話番号/AIスコア）
  - 分页: page=N 参数
"""

from __future__ import annotations

import re

from lxml import html


# table 行标题到字段名的映射
_FIELD_MAP = {
    "業種": "industry",
    "住所": "address",
    "設立年度": "founded_year",
    "代表者名": "representative",
    "資本金等": "capital",
    "電話番号": "phone",
}


def parse_company_list(page_html: str) -> list[dict[str, str]]:
    """解析列表页 HTML，提取公司信息列表。"""
    tree = html.fromstring(page_html)
    results: list[dict[str, str]] = []

    # 找到 results 容器
    wrapper = tree.cssselect("div.results, div.resultsWrapper")
    if not wrapper:
        wrapper = [tree]  # 回退到整个文档

    root = wrapper[0]

    # 公司名在 a[href*="/item/"] 链接中
    company_links = root.cssselect('a[href*="/item/"]')
    # 每家公司对应一个 7 行 table（業種/住所/設立/代表/資本/電話/AI）
    # 注意还有标签 table（1 行，key=オリジナルタグ），需要跳过
    info_tables = []
    for tbl in root.cssselect("table"):
        rows = tbl.cssselect("tr")
        # 信息 table 有 7 行（或至少 5 行以上）
        if len(rows) >= 5:
            info_tables.append(tbl)

    # 公司链接可能比 info_tables 多（链接重复），
    # 取不重复的链接
    seen_hrefs: set[str] = set()
    unique_links: list = []
    for link in company_links:
        href = link.get("href", "")
        if href and href not in seen_hrefs:
            seen_hrefs.add(href)
            unique_links.append(link)

    # 按顺序配对
    count = min(len(unique_links), len(info_tables))
    for i in range(count):
        link = unique_links[i]
        tbl = info_tables[i]
        company = _parse_company_from_pair(link, tbl)
        if company.get("company_name"):
            results.append(company)

    return results


def _parse_company_from_pair(link_el, table_el) -> dict[str, str]:
    """从链接 + 信息表格解析一家公司。"""
    company: dict[str, str] = {}

    # 公司名
    name_text = link_el.text_content().strip()
    # 去掉链接中可能混入的"キーマン人数"等噪音
    name_clean = name_text.split("\n")[0].strip()
    company["company_name"] = _clean_text(name_clean)

    # 详情页链接
    href = link_el.get("href", "")
    if href and not href.startswith("http"):
        href = f"https://biz-maps.com{href}"
    company["detail_url"] = href

    # 从 table 逐行提取
    for row in table_el.cssselect("tr"):
        th_el = row.cssselect("th")
        td_el = row.cssselect("td")
        if not th_el or not td_el:
            continue
        header = _clean_text(th_el[0].text_content())
        value = _clean_text(td_el[0].text_content())

        field_name = _FIELD_MAP.get(header)
        if field_name:
            # 地址清理：去掉邮编前缀
            if field_name == "address" and value:
                value = re.sub(r"^〒\s*\d{3}-?\d{4}\s*", "", value).strip()
            company[field_name] = value

    # 确保所有字段都存在
    for field in ("representative", "address", "industry", "phone", "website",
                  "founded_year", "capital", "detail_url"):
        company.setdefault(field, "")

    return company


def parse_total_results(page_html: str) -> int:
    """解析列表页中的总件数（如 "334778件"）。取最大值避免匹配到 "0件"。"""
    matches = re.findall(r"([\d,]+)\s*件", page_html)
    if not matches:
        return 0
    return max(int(m.replace(",", "")) for m in matches)


def parse_total_pages(page_html: str, per_page: int = 20) -> int:
    """从列表页解析总页数。

    优先从分页导航提取，回退到用总件数计算。
    """
    tree = html.fromstring(page_html)

    # 方法1：找分页链接中最大的数字
    max_page = 1
    for link in tree.cssselect('a[href*="page="]'):
        href = link.get("href", "")
        m = re.search(r"page=(\d+)", href)
        if m:
            page_num = int(m.group(1))
            max_page = max(max_page, page_num)

    # 方法2：用总件数计算
    total = parse_total_results(page_html)
    if total > 0:
        calc_pages = (total + per_page - 1) // per_page
        max_page = max(max_page, calc_pages)

    return max_page


def _clean_text(text: str) -> str:
    """清理文本：去除多余空白和换行。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()
