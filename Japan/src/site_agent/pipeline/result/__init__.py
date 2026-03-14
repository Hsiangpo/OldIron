from __future__ import annotations

from typing import Any

from ...models import ExtractionResult, PageContent, SiteInput
from ...utils import url_depth, utc_now_iso
from ..fields import _status_from_fields
from ..heuristics import _sanitize_info
from ..memory import _update_memory_found
from ..selection import _looks_like_contact_url
from ..logging import _resolve_input_name


def _build_result(
    site: SiteInput,
    visited: dict[str, PageContent],
    info: dict[str, Any] | None,
    *,
    required_fields: list[str] | None = None,
    memory: dict[str, Any] | None = None,
) -> ExtractionResult:
    if isinstance(info, dict):
        info = _sanitize_info(info)
        if isinstance(memory, dict):
            _update_memory_found(memory, info)
    evidence = info.get("evidence", {}) if isinstance(info, dict) else {}
    company_evidence = evidence.get("company_name", {}) if isinstance(evidence, dict) else {}
    rep_evidence = evidence.get("representative", {}) if isinstance(evidence, dict) else {}
    capital_evidence = evidence.get("capital", {}) if isinstance(evidence, dict) else {}
    employees_evidence = evidence.get("employees", {}) if isinstance(evidence, dict) else {}
    email_evidence = evidence.get("email", {}) if isinstance(evidence, dict) else {}
    phone_evidence = evidence.get("phone", {}) if isinstance(evidence, dict) else {}
    input_name = _resolve_input_name(site)
    company_name = input_name if isinstance(input_name, str) and input_name.strip() else None
    representative = info.get("representative") if isinstance(info, dict) else None
    capital = info.get("capital") if isinstance(info, dict) else None
    employees = info.get("employees") if isinstance(info, dict) else None
    email = info.get("email") if isinstance(info, dict) else None
    phone = info.get("phone") if isinstance(info, dict) else None
    emails = info.get("emails") if isinstance(info, dict) else None
    if isinstance(emails, list):
        emails = [e for e in emails if isinstance(e, str) and e.strip()]
    else:
        emails = None
    email_count = info.get("email_count") if isinstance(info, dict) else None
    if not isinstance(email_count, int):
        email_count = len(emails) if emails else 0
    notes = None
    if isinstance(info, dict):
        raw_notes = info.get("notes")
        if isinstance(raw_notes, str) and "contact_form_url=" in raw_notes:
            notes = raw_notes.strip()
    if not (isinstance(notes, str) and notes.strip()):
        notes = _infer_notes_from_contact_form(visited, email, emails, info)
    status = _status_from_fields(
        company_name,
        representative,
        email,
        phone,
        required_fields=required_fields,
    )
    error = None
    if status == "failed":
        if isinstance(info, dict):
            firecrawl_error = info.get("error")
            if isinstance(firecrawl_error, str) and firecrawl_error.strip():
                error = firecrawl_error
            else:
                error = "no_content"
        else:
            error = "no_content"
    return ExtractionResult(
        website=site.website,
        input_name=input_name,
        company_name=company_name if isinstance(company_name, str) else None,
        representative=representative if isinstance(representative, str) else None,
        capital=capital if isinstance(capital, str) else None,
        employees=employees if isinstance(employees, str) else None,
        email=email if isinstance(email, str) else None,
        emails=emails,
        email_count=email_count,
        phone=phone if isinstance(phone, str) else None,
        company_name_source_url=_evidence_url(company_evidence),
        representative_source_url=_evidence_url(rep_evidence),
        capital_source_url=_evidence_url(capital_evidence),
        employees_source_url=_evidence_url(employees_evidence),
        email_source_url=_evidence_url(email_evidence),
        phone_source_url=_evidence_url(phone_evidence),
        notes=notes if isinstance(notes, str) else None,
        source_urls=sorted(visited.keys()),
        status=status,
        error=error,
        extracted_at=utc_now_iso(),
        raw_llm=info if isinstance(info, dict) else None,
    )


def _infer_notes_from_contact_form(
    visited: dict[str, PageContent],
    email: Any,
    emails: Any,
    info: dict[str, Any] | None,
) -> str | None:
    if isinstance(email, str) and email.strip():
        return None
    if isinstance(emails, list) and any(isinstance(e, str) and e.strip() for e in emails):
        return None
    if not visited:
        return None
    contact_urls = [url for url in visited.keys() if _looks_like_contact_url(url)]
    if not contact_urls:
        return None
    best = sorted(contact_urls, key=lambda u: (url_depth(u), len(u)))[0]
    if isinstance(info, dict):
        raw_notes = info.get("notes")
        if isinstance(raw_notes, str) and "contact_form_url=" in raw_notes:
            return raw_notes.strip()
    return f"contact_form_url={best}"


def _evidence_url(evidence: dict[str, Any]) -> str | None:
    url = evidence.get("url") if isinstance(evidence, dict) else None
    return url if isinstance(url, str) and url.strip() else None

