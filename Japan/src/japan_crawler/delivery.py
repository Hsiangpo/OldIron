"""Japan 国家级交付。"""

from __future__ import annotations

import csv
import json
import shutil
import sqlite3
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import validate_day_sequence
from oldiron_core.delivery.sanitize import sanitize_record


ALLOWED_EMAIL_DOMAINS = {
    "gmail.com",
    "icloud.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
}

_CSV_FIELDS = [
    "company_name",
    "representative",
    "emails",
    "website",
    "phone",
    "evidence_url",
]


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Japan 国家级日交付包。"""
    day, _latest = validate_day_sequence(Path(delivery_root), "Japan", day_label)
    delivery_dir = Path(delivery_root) / f"Japan_day{day:03d}"
    if delivery_dir.exists():
        shutil.rmtree(delivery_dir)
    delivery_dir.mkdir(parents=True, exist_ok=True)

    current_records = _load_and_merge_records(Path(data_root))
    baseline_keys = _load_baseline_keys(Path(delivery_root), day - 1)
    delta_records = [row for row in current_records if _record_key(row) not in baseline_keys]

    csv_path = delivery_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(delta_records)

    keys_path = delivery_dir / "keys.txt"
    keys_path.write_text(
        "\n".join(_record_key(row) for row in current_records),
        encoding="utf-8",
    )

    summary = {
        "country": "Japan",
        "day": day,
        "baseline_day": max(day - 1, 0),
        "delta_companies": len(delta_records),
        "total_current_companies": len(current_records),
    }
    (delivery_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


def _load_and_merge_records(data_root: Path) -> list[dict[str, str]]:
    grouped: dict[str, dict[str, str | list[str]]] = {}
    if not data_root.exists():
        return []

    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir() or site_dir.name == "delivery":
            continue
        for record in _load_site_records(site_dir.name, site_dir):
            _merge_record(grouped, record)

    merged: list[dict[str, str]] = []
    for entry in grouped.values():
        emails = _split_emails(str(entry.pop("_emails", "")))
        cleaned = sanitize_record(entry, emails)
        if cleaned is not None:
            merged.append(cleaned)
    return merged


def _load_site_records(site_name: str, site_dir: Path) -> list[dict[str, str]]:
    if site_name == "bizmaps":
        return _load_bizmaps_data(site_dir)
    if site_name == "xlsximport":
        return _load_xlsximport_data(site_dir)
    if site_name == "hellowork":
        return _load_hellowork_data(site_dir)
    return []


def _load_xlsximport_data(site_dir: Path) -> list[dict[str, str]]:
    db_path = site_dir / "xlsximport_store.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT website, email, company_name, representative
        FROM companies
        WHERE company_name != '' AND company_name IS NOT NULL
        ORDER BY id
        """
    ).fetchall()
    conn.close()

    return [
        {
            "company_name": str(row["company_name"] or "").strip(),
            "representative": str(row["representative"] or "").strip(),
            "website": str(row["website"] or "").strip(),
            "emails": _filter_emails(str(row["email"] or "").strip()),
            "phone": "",
            "evidence_url": str(row["website"] or "").strip(),
        }
        for row in rows
    ]


def _load_hellowork_data(site_dir: Path) -> list[dict[str, str]]:
    db_path = site_dir / "hellowork_store.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT company_name, representative, website, phone, detail_url, emails
        FROM companies
        WHERE company_name != '' AND company_name IS NOT NULL
        ORDER BY id
        """
    ).fetchall()
    conn.close()

    return [
        {
            "company_name": str(row["company_name"] or "").strip(),
            "representative": str(row["representative"] or "").strip(),
            "website": str(row["website"] or "").strip(),
            "emails": _filter_emails(str(row["emails"] or "").strip()),
            "phone": str(row["phone"] or "").strip(),
            "evidence_url": str(row["detail_url"] or "").strip(),
        }
        for row in rows
    ]


def _load_bizmaps_data(site_dir: Path) -> list[dict[str, str]]:
    db_path = site_dir / "bizmaps_store.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    col_info = conn.execute("PRAGMA table_info(companies)").fetchall()
    existing_cols = {col["name"] for col in col_info}
    optional_cols = [col for col in ("phone", "detail_url", "emails") if col in existing_cols]
    select_cols = ["company_name", "representative", "website"] + optional_cols
    rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM companies WHERE company_name != '' ORDER BY id"
    ).fetchall()
    conn.close()

    records: list[dict[str, str]] = []
    for row in rows:
        records.append(
            {
                "company_name": str(row["company_name"] or "").strip(),
                "representative": str(row["representative"] or "").strip(),
                "website": str(row["website"] or "").strip(),
                "emails": _filter_emails(str(row["emails"] or "").strip()),
                "phone": str(row["phone"] or "").strip() if "phone" in existing_cols else "",
                "evidence_url": str(row["detail_url"] or "").strip() if "detail_url" in existing_cols else "",
            }
        )
    return records


def _merge_record(grouped: dict[str, dict[str, str | list[str]]], record: dict[str, str]) -> None:
    company_name = str(record.get("company_name", "")).strip()
    if not company_name:
        return
    key = company_name.lower()
    current = grouped.get(key)
    emails = _split_emails(str(record.get("emails", "")))
    if current is None:
        grouped[key] = {
            "company_name": company_name,
            "representative": str(record.get("representative", "")).strip(),
            "website": str(record.get("website", "")).strip(),
            "phone": str(record.get("phone", "")).strip(),
            "evidence_url": str(record.get("evidence_url", "")).strip(),
            "_emails": "; ".join(emails),
        }
        return

    for field in ("representative", "website", "phone", "evidence_url"):
        if not str(current.get(field, "")).strip():
            value = str(record.get(field, "")).strip()
            if value:
                current[field] = value

    merged_emails = _split_emails(str(current.get("_emails", "")))
    for email in emails:
        if email not in merged_emails:
            merged_emails.append(email)
    current["_emails"] = "; ".join(merged_emails)


def _split_emails(emails_str: str) -> list[str]:
    if not emails_str:
        return []
    values: list[str] = []
    for raw in emails_str.replace(",", ";").split(";"):
        email = str(raw or "").strip().lower()
        if email and email not in values:
            values.append(email)
    return values


def _filter_emails(emails_str: str) -> str:
    if not emails_str:
        return ""
    filtered: list[str] = []
    for email in _split_emails(emails_str):
        if "@" not in email:
            continue
        if email.split("@", 1)[1] in ALLOWED_EMAIL_DOMAINS:
            filtered.append(email)
    return "; ".join(filtered)


def _record_key(record: dict[str, str]) -> str:
    return str(record.get("company_name", "")).strip().lower()


def _load_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    day_dir = Path(delivery_root) / f"Japan_day{baseline_day:03d}"
    keys_path = day_dir / "keys.txt"
    if keys_path.exists():
        return {
            line.strip()
            for line in keys_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    csv_path = day_dir / "companies.csv"
    if not csv_path.exists():
        return set()
    with csv_path.open(encoding="utf-8-sig", newline="") as fp:
        return {
            _record_key(row)
            for row in csv.DictReader(fp)
            if str(row.get("company_name", "")).strip()
        }
