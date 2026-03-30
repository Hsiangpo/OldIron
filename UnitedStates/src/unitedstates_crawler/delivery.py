"""UnitedStates 交付包装。"""

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

from oldiron_core.delivery.engine import parse_day_label
from oldiron_core.delivery.sanitize import sanitize_record


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 UnitedStates 日交付包。"""
    day = parse_day_label(day_label)
    delivery_dir = Path(delivery_root) / f"UnitedStates_day{day:03d}"
    if delivery_dir.exists():
        shutil.rmtree(delivery_dir)
    delivery_dir.mkdir(parents=True, exist_ok=True)
    current_records = _load_records(Path(data_root))
    baseline_keys = _load_baseline_keys(Path(delivery_root), day - 1)
    delta_records = [r for r in current_records if _record_key(r) not in baseline_keys]
    with (delivery_dir / "companies.csv").open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=["company_name", "representative", "emails", "website", "phone", "evidence_url"],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(delta_records)
    (delivery_dir / "keys.txt").write_text("\n".join(_record_key(r) for r in current_records), encoding="utf-8")
    summary = {
        "country": "UnitedStates",
        "day": day,
        "baseline_day": max(day - 1, 0),
        "delta_companies": len(delta_records),
        "total_current_companies": len(current_records),
    }
    (delivery_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _load_records(data_root: Path) -> list[dict[str, str]]:
    site_dir = data_root / "dnb"
    db_path = site_dir / "dnb_store.db"
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
    # 先按公司名归并，选字段最全的记录，邮箱合并去重
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = str(row["company_name"] or "").strip().lower()
        grouped.setdefault(key, []).append(row)
    records: list[dict[str, str]] = []
    for _key, group in grouped.items():
        # 对同名公司的多条记录，按字段填充数排序，取最全的作为主记录
        def _field_score(r: sqlite3.Row) -> int:
            score = 0
            for f in ("representative", "website", "phone", "address", "evidence_url"):
                if str(r[f] or "").strip():
                    score += 1
            score += len([e for e in str(r["emails"] or "").split(";") if e.strip()])
            return score

        group.sort(key=_field_score, reverse=True)
        best = group[0]
        entry = {
            "company_name": str(best["company_name"] or "").strip(),
            "representative": str(best["representative"] or "").strip(),
            "website": str(best["website"] or "").strip(),
            "phone": str(best["phone"] or "").strip(),
            "address": str(best["address"] or "").strip(),
            "evidence_url": str(best["evidence_url"] or "").strip(),
        }
        # 合并去重所有同名记录的邮箱
        all_emails: list[str] = []
        seen_emails: set[str] = set()
        for row in group:
            for item in str(row["emails"] or "").split(";"):
                email = item.strip().lower()
                if email and email not in seen_emails:
                    seen_emails.add(email)
                    all_emails.append(email)
        cleaned = sanitize_record(entry, all_emails)
        if cleaned is not None:
            records.append(cleaned)
    return records


def _load_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    keys_path = Path(delivery_root) / f"UnitedStates_day{baseline_day:03d}" / "keys.txt"
    if not keys_path.exists():
        return set()
    return {line.strip() for line in keys_path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _record_key(record: dict[str, str]) -> str:
    return str(record.get("company_name", "")).strip().lower()
