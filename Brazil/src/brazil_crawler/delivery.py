"""Brazil 国家级交付。"""

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


_CSV_FIELDS = [
    "company_name",
    "representative",
    "emails",
    "website",
    "phone",
    "address",
    "evidence_url",
]


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Brazil 国家级日交付包。"""
    day, _latest = validate_day_sequence(Path(delivery_root), "Brazil", day_label)
    baseline_day = max(day - 1, 0)
    delivery_dir = Path(delivery_root) / f"Brazil_day{day:03d}"

    if delivery_dir.exists():
        shutil.rmtree(delivery_dir)
    delivery_dir.mkdir(parents=True, exist_ok=True)

    current_records = _load_records(Path(data_root))
    baseline_keys = _load_baseline_keys(Path(delivery_root), baseline_day)
    delta_records = [record for record in current_records if _record_key(record) not in baseline_keys]

    csv_path = delivery_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(delta_records)

    (delivery_dir / "keys.txt").write_text(
        "\n".join(_record_key(record) for record in current_records),
        encoding="utf-8",
    )

    summary = {
        "country": "Brazil",
        "day": day,
        "baseline_day": baseline_day,
        "delta_companies": len(delta_records),
        "total_current_companies": len(current_records),
    }
    (delivery_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        f"  dnb: DB 总计 {len(current_records)} → 当前合格 {len(current_records)} 家公司 → 当日新增 {len(delta_records)} 家公司"
    )
    return summary


def _load_records(data_root: Path) -> list[dict[str, str]]:
    db_path = Path(data_root) / "dnb" / "dnb_store.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT company_name, representative, emails, website, phone, address, evidence_url
        FROM final_companies
        ORDER BY company_name
        """
    ).fetchall()
    conn.close()

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = _record_key(
            {
                "company_name": str(row["company_name"] or "").strip(),
                "representative": str(row["representative"] or "").strip(),
                "website": str(row["website"] or "").strip(),
            }
        )
        if not key.strip(" |"):
            continue
        grouped.setdefault(key, []).append(row)

    records: list[dict[str, str]] = []
    for group in grouped.values():
        group.sort(key=_row_score, reverse=True)
        best = group[0]
        emails = _merge_group_emails(group)
        if not (
            str(best["company_name"] or "").strip()
            and str(best["representative"] or "").strip()
            and emails
        ):
            continue
        records.append(
            {
                "company_name": str(best["company_name"] or "").strip(),
                "representative": str(best["representative"] or "").strip(),
                "emails": "; ".join(emails),
                "website": str(best["website"] or "").strip(),
                "phone": str(best["phone"] or "").strip(),
                "address": str(best["address"] or "").strip(),
                "evidence_url": str(best["evidence_url"] or "").strip(),
            }
        )
    return records


def _row_score(row: sqlite3.Row) -> int:
    score = 0
    for field in ("representative", "website", "phone", "address", "evidence_url"):
        if str(row[field] or "").strip():
            score += 1
    score += len([item for item in str(row["emails"] or "").split(";") if item.strip()])
    return score


def _merge_group_emails(group: list[sqlite3.Row]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for row in group:
        for raw in str(row["emails"] or "").split(";"):
            email = raw.strip().lower()
            if email and email not in seen:
                seen.add(email)
                merged.append(email)
    return merged


def _record_key(record: dict[str, str]) -> str:
    parts = (
        str(record.get("company_name", "") or "").strip().lower(),
        str(record.get("representative", "") or "").strip().lower(),
        str(record.get("website", "") or "").strip().lower(),
    )
    return " | ".join(parts)


def _load_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    baseline_dir = Path(delivery_root) / f"Brazil_day{baseline_day:03d}"

    keys_path = baseline_dir / "keys.txt"
    if keys_path.exists():
        return {
            line.strip()
            for line in keys_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }

    legacy_keys_path = baseline_dir / "dnb.keys.txt"
    if legacy_keys_path.exists():
        return {
            line.strip()
            for line in legacy_keys_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }

    csv_path = baseline_dir / "companies.csv"
    if csv_path.exists():
        with csv_path.open(encoding="utf-8-sig", newline="") as fp:
            return {
                _record_key(row)
                for row in csv.DictReader(fp)
                if str(row.get("company_name", "")).strip()
            }

    legacy_csv = baseline_dir / "dnb.csv"
    if not legacy_csv.exists():
        return set()
    with legacy_csv.open(encoding="utf-8-sig", newline="") as fp:
        return {
            _record_key(row)
            for row in csv.DictReader(fp)
            if str(row.get("company_name", "")).strip()
        }
