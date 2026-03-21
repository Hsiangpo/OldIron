"""DNB 快照导出。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from korea_crawler.dnb.domain_quality import assess_company_domain
from korea_crawler.dnb.domain_quality import normalize_website_url
from korea_crawler.snov.client import extract_domain


def _parse_json_list(raw: str) -> list[str]:
    try:
        value = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _record_score(record: dict[str, object]) -> tuple[int, int, int, int]:
    emails = record.get("emails", [])
    email_list = emails if isinstance(emails, list) else []
    has_email = 1 if email_list else 0
    has_ceo = 1 if str(record.get("ceo", "")).strip() else 0
    has_name = 1 if str(record.get("company_name", "")).strip() else 0
    return has_email, len(email_list), has_ceo, has_name


def _snapshot_from_company_row(row: dict[str, object]) -> dict[str, object]:
    emails = _parse_json_list(str(row.get("emails_json", "[]")))
    homepage = normalize_website_url(str(row.get("website", "") or row.get("dnb_website", "")).strip())
    return {
        "comp_id": str(row.get("duns", "")),
        "duns": str(row.get("duns", "")),
        "company_name": str(row.get("company_name_resolved", "") or row.get("company_name_en_dnb", "")).strip(),
        "company_name_en_dnb": str(row.get("company_name_en_dnb", "")).strip(),
        "ceo": str(row.get("key_principal", "")).strip(),
        "homepage": homepage,
        "dnb_website": str(row.get("dnb_website", "")).strip(),
        "emails": emails,
        "website_source": str(row.get("website_source", "")).strip(),
    }


def _snapshot_row_allowed(row: dict[str, object]) -> bool:
    homepage = normalize_website_url(str(row.get("homepage", "")).strip())
    company_name_en_dnb = str(row.get("company_name_en_dnb", row.get("company_name", ""))).strip()
    if not homepage:
        return False
    assessment = assess_company_domain(
        company_name_en_dnb,
        homepage,
        source=str(row.get("website_source", "")).strip() or "dnb",
    )
    if assessment.blocked:
        return False
    return bool(str(row.get("company_name", "")).strip() and str(row.get("ceo", "")).strip() and row.get("emails"))


def _write_json_line(fp, row: dict[str, object]) -> None:
    fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _replace_tmp(tmp_path: Path, target_path: Path) -> None:
    tmp_path.replace(target_path)


def export_jsonl_snapshots(db_path: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    companies_tmp = output_dir / "companies.jsonl.tmp"
    enriched_tmp = output_dir / "companies_enriched.jsonl.tmp"
    with_emails_tmp = output_dir / "companies_with_emails.jsonl.tmp"
    final_tmp = output_dir / "final_companies.jsonl.tmp"

    domain_best: dict[str, dict[str, object]] = {}
    domain_order: list[str] = []
    no_domain_rows: list[dict[str, object]] = []

    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000;")
    try:
        with (
            companies_tmp.open("w", encoding="utf-8") as companies_fp,
            enriched_tmp.open("w", encoding="utf-8") as enriched_fp,
            with_emails_tmp.open("w", encoding="utf-8") as with_emails_fp,
        ):
            last_rowid = 0
            while True:
                rows = conn.execute(
                    """
                    SELECT rowid, *
                    FROM companies
                    WHERE rowid > ?
                    ORDER BY rowid ASC
                    LIMIT 500
                    """,
                    (last_rowid,),
                ).fetchall()
                if not rows:
                    break
                for row in rows:
                    last_rowid = int(row["rowid"])
                    record = _snapshot_from_company_row(dict(row))
                    _write_json_line(
                        companies_fp,
                        {
                            "comp_id": record["comp_id"],
                            "company_name": record["company_name_en_dnb"],
                            "ceo": record["ceo"],
                            "homepage": record["dnb_website"],
                            "emails": [],
                        },
                    )
                    _write_json_line(
                        enriched_fp,
                        {
                            "comp_id": record["comp_id"],
                            "company_name": record["company_name"],
                            "ceo": record["ceo"],
                            "homepage": record["homepage"],
                            "emails": [],
                        },
                    )
                    if not _snapshot_row_allowed(record):
                        continue
                    _write_json_line(with_emails_fp, record)
                    final_row = {
                        "comp_id": record["comp_id"],
                        "duns": record["duns"],
                        "company_name": record["company_name"],
                        "ceo": record["ceo"],
                        "homepage": record["homepage"],
                        "emails": record["emails"],
                    }
                    domain = extract_domain(str(record.get("homepage", "")))
                    if not domain:
                        no_domain_rows.append(final_row)
                        continue
                    if domain not in domain_best:
                        domain_best[domain] = final_row
                        domain_order.append(domain)
                        continue
                    if _record_score(final_row) > _record_score(domain_best[domain]):
                        domain_best[domain] = final_row

        with final_tmp.open("w", encoding="utf-8") as final_fp:
            for domain in domain_order:
                _write_json_line(final_fp, domain_best[domain])
            for row in no_domain_rows:
                _write_json_line(final_fp, row)

        _replace_tmp(companies_tmp, output_dir / "companies.jsonl")
        _replace_tmp(enriched_tmp, output_dir / "companies_enriched.jsonl")
        _replace_tmp(with_emails_tmp, output_dir / "companies_with_emails.jsonl")
        _replace_tmp(final_tmp, output_dir / "final_companies.jsonl")
    finally:
        conn.close()
