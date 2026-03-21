from __future__ import annotations

from typing import Any


def _normalize_required_fields(required_fields: list[str] | None) -> list[str]:
    allowed_order = ["company_name", "email", "representative", "phone"]
    default_fields = ["company_name", "email", "representative"]
    if not (isinstance(required_fields, list) and required_fields):
        return default_fields
    cleaned = {f.strip() for f in required_fields if isinstance(f, str) and f.strip()}
    picked = [f for f in allowed_order if f in cleaned]
    return picked or default_fields


def _missing_fields(
    info: dict[str, Any] | None, *, required_fields: list[str] | None = None
) -> list[str]:
    required = _normalize_required_fields(required_fields)
    if not info:
        return required
    missing: list[str] = []
    for key in required:
        value = info.get(key)
        if not isinstance(value, str) or not value.strip():
            missing.append(key)
    return missing


def _status_from_fields(
    company_name: Any,
    representative: Any,
    email: Any,
    phone: Any,
    *,
    required_fields: list[str] | None = None,
) -> str:
    has_company = isinstance(company_name, str) and company_name.strip()
    has_rep = isinstance(representative, str) and representative.strip()
    has_email = isinstance(email, str) and email.strip()
    has_phone = isinstance(phone, str) and phone.strip()
    required = _normalize_required_fields(required_fields)
    if required:
        field_map = {
            "company_name": has_company,
            "representative": has_rep,
            "email": has_email,
            "phone": has_phone,
        }
        if all(field_map.get(field, False) for field in required):
            return "ok"
    if has_company or has_rep or has_email or has_phone:
        return "partial"
    return "failed"
