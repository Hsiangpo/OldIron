"""站点级集中合并。"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from pathlib import Path

from england_crawler.companies_house.client import normalize_company_name


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        value = json.loads(text)
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _sqlite_tables(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    finally:
        conn.close()
    return {str(row[0]).strip() for row in rows}


def _refresh_run_snapshot(run_dir: Path) -> None:
    db_path = run_dir / "store.db"
    if not db_path.exists():
        return
    tables = _sqlite_tables(db_path)
    if "dnb_segments" in tables:
        from england_crawler.dnb.runtime.snapshot_export import export_jsonl_snapshots

        export_jsonl_snapshots(db_path, run_dir)
        return
    if "ch_queue" in tables:
        from england_crawler.companies_house.snapshot_export import export_jsonl_snapshots

        export_jsonl_snapshots(db_path, run_dir)


def _load_run_records(run_dir: Path) -> list[dict[str, object]]:
    _refresh_run_snapshot(run_dir)
    primary_path = run_dir / "companies_with_emails.jsonl"
    fallback_path = run_dir / "final_companies.jsonl"
    if primary_path.exists():
        return _read_jsonl(primary_path)
    if fallback_path.exists():
        return _read_jsonl(fallback_path)
    return []


def _record_key(record: dict[str, object]) -> str:
    company_name = normalize_company_name(str(record.get("company_name", "")).strip())
    if company_name:
        return f"name|{company_name}"
    domain = str(record.get("domain", "")).strip().lower()
    return f"domain|{domain}" if domain else ""


def _record_score(record: dict[str, object]) -> tuple[int, int, int, int]:
    emails = record.get("emails", [])
    email_count = len(emails) if isinstance(emails, list) else 0
    return (
        1 if email_count > 0 else 0,
        email_count,
        1 if str(record.get("homepage", "")).strip() else 0,
        1 if str(record.get("ceo", "")).strip() else 0,
    )


def _tmp_path(target: Path) -> Path:
    return target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")


def _write_jsonl_atomic(path: Path, rows: list[dict[str, object]]) -> None:
    tmp_path = _tmp_path(path)
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    tmp_path.write_text(payload + ("\n" if payload else ""), encoding="utf-8")
    os.replace(tmp_path, path)


def merge_site_runs(run_dirs: list[str | Path], output_dir: str | Path) -> dict[str, object]:
    """合并多个 run 目录到标准站点输出目录。"""
    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    best_by_key: dict[str, dict[str, object]] = {}
    order: list[str] = []
    input_records = 0
    for raw_dir in run_dirs:
        run_dir = Path(raw_dir).resolve()
        for record in _load_run_records(run_dir):
            input_records += 1
            key = _record_key(record)
            if not key:
                continue
            if key not in best_by_key:
                best_by_key[key] = record
                order.append(key)
                continue
            if _record_score(record) > _record_score(best_by_key[key]):
                best_by_key[key] = record

    final_rows = [best_by_key[key] for key in order]
    companies_rows = [
        {
            "company_name": str(row.get("company_name", "")).strip(),
            "ceo": str(row.get("ceo", "")).strip(),
            "homepage": "",
            "emails": [],
        }
        for row in final_rows
    ]
    enriched_rows = [
        {
            "company_name": str(row.get("company_name", "")).strip(),
            "ceo": str(row.get("ceo", "")).strip(),
            "homepage": str(row.get("homepage", "")).strip(),
            "emails": [],
        }
        for row in final_rows
    ]

    _write_jsonl_atomic(target_dir / "companies.jsonl", companies_rows)
    _write_jsonl_atomic(target_dir / "companies_enriched.jsonl", enriched_rows)
    _write_jsonl_atomic(target_dir / "companies_with_emails.jsonl", final_rows)
    _write_jsonl_atomic(target_dir / "final_companies.jsonl", final_rows)

    return {
        "output_dir": str(target_dir),
        "input_records": input_records,
        "merged_companies": len(final_rows),
    }
