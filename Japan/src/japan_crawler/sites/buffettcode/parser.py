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


# 代表者职衔词（前/后缀剥离用，按长度降序整体优先匹配）
_REP_TITLES = (
    "代表取締役会長兼社長", "代表取締役社長執行役員", "代表取締役社長", "代表取締役会長",
    "代表取締役副社長", "代表取締役専務", "代表取締役CEO", "代表取締役", "代表執行役員社長",
    "代表執行役社長", "代表執行役員", "代表執行役", "執行役員社長", "取締役執行役員",
    "取締役社長", "取締役会長", "取締役副社長", "取締役", "執行役員", "執行役",
    "代表社員", "業務執行社員", "社員", "代表理事", "専務理事", "常務理事", "理事長",
    "副理事長", "理事", "副会長", "副社長", "専務", "常務", "社長", "会長", "総裁",
    "頭取", "監査役", "副院長", "院長", "校長", "園長", "所長", "支店長", "局長",
    "統括", "代表者", "代表", "創業者", "オーナー", "CEO", "COO", "CFO", "CTO",
    "President", "Founder",
)
_REP_TITLE_LEAD = re.compile(r"^(?:" + "|".join(sorted(map(re.escape, _REP_TITLES), key=len, reverse=True)) + r")")
_REP_TITLE_TAIL = re.compile(r"(?:" + "|".join(sorted(map(re.escape, _REP_TITLES), key=len, reverse=True)) + r")$")
_REP_SEP_RE = re.compile(r"^[\s　・/／·:：，,&兼\-]+")
_REP_BRACKET = re.compile(r"[\[\(（【［][^\]\)）】］]*[\]\)）】］]")
_REP_TITLE_WORDS = re.compile(r"代表|取締役|執行役|監査役|社長|会長|理事|社員|頭取|院長|校長|園長|所長|支店長|局長|オーナー|創業|総裁|専務|常務|CEO|COO|CFO|CTO|President|Founder|最高|責任者|税理士|会計士|弁護士|共同")
_REP_KANA = re.compile(r"^[぀-ゟ\s　]+$")
# 被错配进代表者单元格的"字段标签"，本身不是人名
_REP_LABELS = {
    "設立", "設立年月日", "従業員数", "従業員", "グループ従業員数", "電話番号", "業種",
    "決算", "決算期", "売上高", "上場", "法人番号", "ホームページ", "資本金", "住所",
    "所在地", "本社所在地", "事業内容", "URL",
}


def _drop_anno_bracket(m) -> str:
    """括号注释组：含职衔/角色/公司/纯读音(平假名)的整组删除，普通括号原样保留——绝不吃括号外的人名。"""
    inner = m.group(0).strip("[]()（）【】［］ 　")
    if not inner or _REP_TITLE_WORDS.search(inner) or _REP_KANA.match(inner):
        return " "
    return m.group(0)


def clean_representative(rep: str | None, strip_title: bool = False) -> str:
    """规整代表者：去标点前缀、清纯标签污染；strip_title=True 再剥离前/后职衔与职衔括注。

    DB 落库用 strip_title=False（保留职衔，忠实原文）；导出 CSV 用 True（要纯人名）。
    只删职衔，绝不吃人名；多代表人字段尽量保留各人名。
    """
    s = (rep or "").strip()
    if not s or s in _REP_LABELS:
        return ""
    s = _REP_SEP_RE.sub("", s).strip()
    if strip_title:
        s = _REP_BRACKET.sub(_drop_anno_bracket, s)          # 删职衔/读音括注，保名字
        s = re.sub(r"\s+", " ", s).strip()
        for _ in range(5):                                    # 反复剥开头职衔（可叠加）
            s = _REP_SEP_RE.sub("", s)
            m = _REP_TITLE_LEAD.match(s)
            if not m:
                break
            s = s[m.end():]
        for _ in range(3):                                    # 剥结尾粘连职衔
            s = s.strip(" 　・/／:：")
            m = _REP_TITLE_TAIL.search(s)
            if not m:
                break
            s = s[:m.start()]
        s = _REP_SEP_RE.sub("", s).strip(" 　・/／:：")
    return "" if (not s or s in _REP_LABELS) else s


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
        "representative": clean_representative(fields.get("代表者", "")),
        "website": fields.get("会社HP", ""),
        "address": fields.get("住所", ""),
        "capital": fields.get("資本金", ""),
    }
