"""DNB 快照导出。"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from pathlib import Path

from denmark_crawler.dnb.domain_quality import assess_company_domain
from denmark_crawler.dnb.domain_quality import normalize_website_url
from denmark_crawler.snov.client import extract_domain


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


def _json_safe(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, str):
        return value.encode("utf-8", errors="replace").decode("utf-8")
    return value


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
    domain = str(row.get("domain", "")).strip() or extract_domain(homepage)
    return {
        "comp_id": str(row.get("duns", "")),
        "duns": str(row.get("duns", "")),
        "company_name": str(row.get("company_name_resolved", "") or row.get("company_name_en_dnb", "")).strip(),
        "company_name_en_dnb": str(row.get("company_name_en_dnb", "")).strip(),
        "ceo": str(row.get("key_principal", "")).strip(),
        "homepage": homepage,
        "domain": domain,
        "phone": str(row.get("phone", "")).strip(),
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
    payload = json.dumps(_json_safe(row), ensure_ascii=False) + "\n"
    fp.write(payload.encode("utf-8", errors="replace"))


def _replace_tmp(tmp_path: Path, target_path: Path) -> None:
    os.replace(tmp_path, target_path)


def _tmp_output_path(output_dir: Path, target_name: str) -> Path:
    return output_dir / f"{target_name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"


def export_jsonl_snapshots(db_path: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    last_error: OSError | None = None
    for attempt in range(3):
        companies_tmp = _tmp_output_path(output_dir, "companies.jsonl")
        enriched_tmp = _tmp_output_path(output_dir, "companies_enriched.jsonl")
        with_emails_tmp = _tmp_output_path(output_dir, "companies_with_emails.jsonl")
        final_tmp = _tmp_output_path(output_dir, "final_companies.jsonl")

        domain_best: dict[str, dict[str, object]] = {}
        domain_order: list[str] = []
        no_domain_rows: list[dict[str, object]] = []

        conn = sqlite3.connect(db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000;")
        try:
            with (
                companies_tmp.open("wb") as companies_fp,
                enriched_tmp.open("wb") as enriched_fp,
                with_emails_tmp.open("wb") as with_emails_fp,
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
                            "domain": record["domain"],
                            "phone": record["phone"],
                            "emails": record["emails"],
                        }
                        domain = str(record.get("domain", "")).strip() or extract_domain(str(record.get("homepage", "")))
                        if not domain:
                            no_domain_rows.append(final_row)
                            continue
                        if domain not in domain_best:
                            domain_best[domain] = final_row
                            domain_order.append(domain)
                            continue
                        if _record_score(final_row) > _record_score(domain_best[domain]):
                            domain_best[domain] = final_row

            with final_tmp.open("wb") as final_fp:
                for domain in domain_order:
                    _write_json_line(final_fp, domain_best[domain])
                for row in no_domain_rows:
                    _write_json_line(final_fp, row)

            _replace_tmp(companies_tmp, output_dir / "companies.jsonl")
            _replace_tmp(enriched_tmp, output_dir / "companies_enriched.jsonl")
            _replace_tmp(with_emails_tmp, output_dir / "companies_with_emails.jsonl")
            _replace_tmp(final_tmp, output_dir / "final_companies.jsonl")
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.2 * (attempt + 1))
        finally:
            conn.close()
            for tmp_path in (companies_tmp, enriched_tmp, with_emails_tmp, final_tmp):
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except OSError:
                        pass
    if last_error is not None:
        raise last_error

