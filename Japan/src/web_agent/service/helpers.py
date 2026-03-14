from __future__ import annotations

import time
from typing import Any


def _status_from_fields_simple(
    company_name: Any,
    representative: Any,
    email: Any,
    required_fields: list[str],
) -> str:
    has_company = isinstance(company_name, str) and company_name.strip()
    rep_text = representative if isinstance(representative, str) else ""
    if isinstance(rep_text, str) and rep_text.strip() == "未找到代表人":
        rep_text = ""
    has_rep = isinstance(rep_text, str) and rep_text.strip()
    has_email = isinstance(email, str) and email.strip()
    present = {
        "company_name": bool(has_company),
        "representative": bool(has_rep),
        "email": bool(has_email),
    }
    if all(present.get(field, False) for field in required_fields):
        return "ok"
    if has_company or has_rep or has_email:
        return "partial"
    return "failed"


def _safe_count_company_websites(company_to_website: dict[str, str]) -> int:
    if not isinstance(company_to_website, dict) or not company_to_website:
        return 0
    values: list[Any] = []
    deadline = time.time() + 0.2
    while True:
        try:
            values = list(company_to_website.values())
            break
        except RuntimeError:
            if time.time() >= deadline:
                break
            time.sleep(0)
    if not values:
        try:
            values = list(company_to_website.values())
        except RuntimeError:
            return 0
    return sum(1 for v in values if isinstance(v, str) and v.strip())

