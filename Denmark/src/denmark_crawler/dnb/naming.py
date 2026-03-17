"""丹麦 DNB 公司名决策。"""

from __future__ import annotations

import re


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def has_korean_company_name(value: str) -> bool:
    return bool(_normalize_name(value))


def resolve_company_name(
    *,
    company_name_en_dnb: str,
    company_name_local_gmap: str = "",
    company_name_local_site: str = "",
) -> str:
    return _normalize_name(company_name_en_dnb)

