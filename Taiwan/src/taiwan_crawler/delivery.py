"""Taiwan 交付包装。"""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import prepare_delivery_dir
from oldiron_core.delivery.engine import validate_day_sequence
from oldiron_core.delivery.sanitize import sanitize_record


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Taiwan 日交付包。"""
    day, _latest = validate_day_sequence(Path(delivery_root), "Taiwan", day_label)
    delivery_dir = Path(delivery_root) / f"Taiwan_day{day:03d}"
    prepare_delivery_dir(delivery_dir)

    current_records = _load_records(Path(data_root))
    baseline_keys = _load_baseline_keys(Path(delivery_root), day - 1)
    delta_records = [r for r in current_records if _record_key(r) not in baseline_keys]

    csv_path = delivery_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=["company_name", "representative", "emails", "website", "phone", "evidence_url"],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(delta_records)

    (delivery_dir / "keys.txt").write_text(
        "\n".join(_record_key(r) for r in current_records),
        encoding="utf-8",
    )
    summary = {
        "country": "Taiwan",
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


def _load_records(data_root: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    site_dir = data_root / "ieatpe"
    db_path = site_dir / "ieatpe_store.db"
    if not db_path.exists():
        return records

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT company_name, representative, website, phone, address, emails, detail_url
        FROM companies
        WHERE company_name != ''
        ORDER BY member_id
        """
    ).fetchall()
    conn.close()

    for row in rows:
        entry = {
            "company_name": str(row["company_name"] or "").strip(),
            "representative": str(row["representative"] or "").strip(),
            "website": str(row["website"] or "").strip(),
            "phone": str(row["phone"] or "").strip(),
            "address": str(row["address"] or "").strip(),
            "evidence_url": str(row["detail_url"] or "").strip(),
        }
        emails = [
            item.strip().lower()
            for item in str(row["emails"] or "").replace(",", ";").split(";")
            if item.strip()
        ]
        cleaned = sanitize_record(entry, emails)
        if cleaned is not None:
            records.append(cleaned)
    return records


def _load_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    keys_path = Path(delivery_root) / f"Taiwan_day{baseline_day:03d}" / "keys.txt"
    if not keys_path.exists():
        return set()
    return {line.strip() for line in keys_path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _record_key(record: dict[str, str]) -> str:
    return str(record.get("company_name", "")).strip().lower()
