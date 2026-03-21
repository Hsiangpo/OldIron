"""Companies House 站点快照导出。"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from pathlib import Path

from england_crawler.companies_house.client import normalize_company_name
from england_crawler.google_maps.pipeline import clean_homepage
from england_crawler.snov.client import extract_domain


def _parse_json_list(raw: str) -> list[str]:
    try:
        value = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


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


def _tmp_output_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    last_error: OSError | None = None
    for attempt in range(3):
        tmp_path = _tmp_output_path(path)
        try:
            with tmp_path.open("wb") as fp:
                for row in rows:
                    payload = json.dumps(_json_safe(row), ensure_ascii=False) + "\n"
                    fp.write(payload.encode("utf-8", errors="replace"))
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp_path, path)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.2 * (attempt + 1))
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
    if last_error is not None:
        raise last_error


def _record_score(row: dict[str, object]) -> tuple[int, int]:
    emails = row.get("emails", [])
    email_count = len(emails) if isinstance(emails, list) else 0
    has_domain = 1 if str(row.get("domain", "")).strip() else 0
    return has_domain, email_count


def export_jsonl_snapshots(db_path: Path, output_dir: Path) -> None:
    """导出标准 JSONL 产物。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    companies_rows: list[dict[str, object]] = []
    enriched_rows: list[dict[str, object]] = []
    email_rows: list[dict[str, object]] = []
    final_by_key: dict[str, dict[str, object]] = {}
    final_order: list[str] = []

    connection = sqlite3.connect(db_path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT comp_id, company_name, ceo, company_number, company_status,
                   homepage, domain, phone, emails_json
            FROM companies
            ORDER BY rowid ASC
            """
        ).fetchall()
    finally:
        connection.close()

    for row in rows:
        emails = _parse_json_list(str(row["emails_json"]))
        homepage = clean_homepage(str(row["homepage"]).strip())
        domain = str(row["domain"]).strip() or extract_domain(homepage)
        base_row = {
            "comp_id": str(row["comp_id"]),
            "company_name": str(row["company_name"]).strip(),
            "ceo": str(row["ceo"]).strip(),
            "homepage": homepage,
            "domain": domain,
            "phone": str(row["phone"]).strip(),
            "emails": emails,
            "company_number": str(row["company_number"]).strip(),
            "company_status": str(row["company_status"]).strip(),
        }
        companies_rows.append(
            {
                "comp_id": base_row["comp_id"],
                "company_name": base_row["company_name"],
                "ceo": base_row["ceo"],
                "homepage": "",
                "emails": [],
            }
        )
        enriched_rows.append(
            {
                "comp_id": base_row["comp_id"],
                "company_name": base_row["company_name"],
                "ceo": base_row["ceo"],
                "homepage": base_row["homepage"],
                "domain": base_row["domain"],
                "phone": base_row["phone"],
                "emails": [],
            }
        )
        if not (base_row["company_name"] and base_row["ceo"] and base_row["homepage"] and emails):
            continue
        email_rows.append(base_row)
        final_key = str(base_row["domain"]).strip() or normalize_company_name(
            str(base_row["company_name"])
        )
        if final_key not in final_by_key:
            final_by_key[final_key] = base_row
            final_order.append(final_key)
            continue
        if _record_score(base_row) > _record_score(final_by_key[final_key]):
            final_by_key[final_key] = base_row

    final_rows = [final_by_key[key] for key in final_order]
    _write_jsonl(output_dir / "companies.jsonl", companies_rows)
    _write_jsonl(output_dir / "companies_enriched.jsonl", enriched_rows)
    _write_jsonl(output_dir / "companies_with_emails.jsonl", email_rows)
    _write_jsonl(output_dir / "final_companies.jsonl", final_rows)
