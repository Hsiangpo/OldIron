"""交付数据清洗 & 三项齐全门禁。

所有国家的 delivery.py 在组装最终记录时，统一调用:
    from oldiron_core.delivery.sanitize import sanitize_record
"""

from __future__ import annotations

import html as _html_mod
import re

# ---------- 公司法人后缀正则，用于检测代表人字段误填公司名 ----------
_CORP_SUFFIX_RE = re.compile(
    r"\b("
    r"ApS|A/S|I/S|K/S|P/S|IVS|AMBA|FMBA|SMB[Aa]"
    r"|GmbH|AG|OHG|KG|UG|e\.?V\.?"
    r"|Ltd\.?|LLC|Inc\.?|PLC|LP|LLP"
    r"|AB|HB|KB"
    r"|SA|SL|SAS|SARL|BV|NV|Oy|AS"
    r"|Sp\.?\s*z\.?\s*o\.?\s*o\.?"
    r")\b",
    re.IGNORECASE,
)

# ---------- 零宽 / 不可见字符正则 ----------
_INVISIBLE_RE = re.compile(
    r"[\u200b\u200c\u200d\u200e\u200f\ufeff\u00ad\u2060]"
)


def sanitize_record(
    entry: dict[str, str | list[str]],
    emails_list: list[str],
) -> dict[str, str] | None:
    """清洗单条交付记录。

    返回清洗后的 dict（含 ``emails`` 字段），
    或 ``None`` 表示不满足三项齐全门禁、应丢弃。
    """

    # --- 1. HTML 实体解码（代表人、公司名）---
    for field in ("company_name", "representative"):
        val = str(entry.get(field, "")).strip()
        if "&" in val:
            val = _html_mod.unescape(val)
        entry[field] = val

    # --- 2. 公司名基本检查 ---
    company_name = str(entry.get("company_name", "")).strip()
    if len(company_name) < 2:
        return None  # 过短的脏数据（如 "1"）
    if len(company_name) > 150:
        company_name = company_name[:150].rsplit(" ", 1)[0]
        entry["company_name"] = company_name

    # --- 3. 代表人质量检查 ---
    rep = str(entry.get("representative", "")).strip()
    # 含公司后缀 → 视为无效代表人
    if rep and _CORP_SUFFIX_RE.search(rep):
        rep = ""
        entry["representative"] = ""

    # --- 4. 邮箱清洗 ---
    cleaned_emails: list[str] = []
    for em in emails_list:
        em = _INVISIBLE_RE.sub("", em).strip()  # 去零宽字符
        if not em or "@" not in em:
            continue
        user_part = em.split("@")[0]
        if "/" in user_part:
            continue  # 丢弃 user 部分含斜杠的
        cleaned_emails.append(em)
    entry["emails"] = "; ".join(cleaned_emails)

    # --- 5. 电话清洗 ---
    phone = str(entry.get("phone", "")).strip()
    if phone:
        phone = phone.replace("☎", "").replace("?", "").strip()
        phone = _INVISIBLE_RE.sub("", phone).strip()
        # 只保留数字、空格、+、-、()
        phone = re.sub(r"[^\d\s+\-()]", "", phone).strip()
        entry["phone"] = phone

    # --- 6. 三项齐全门禁：公司名 + 代表人 + 邮箱 ---
    if not entry.get("company_name", "").strip():
        return None
    if not entry.get("representative", "").strip():
        return None
    if not entry.get("emails", "").strip():
        return None

    return entry
