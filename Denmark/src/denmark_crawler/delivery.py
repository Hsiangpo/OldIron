"""Denmark 交付包装。"""

from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import parse_day_label


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Denmark 日交付包，只输出公司名、代表人、邮箱。"""
    day = parse_day_label(day_label)
    delivery_dir = Path(delivery_root) / f"Denmark_day{day:03d}"
    if delivery_dir.exists():
        shutil.rmtree(delivery_dir)
    delivery_dir.mkdir(parents=True, exist_ok=True)
    current_records = _deduplicate(_load_final_records(Path(data_root)))
    baseline_records = _load_previous_records(Path(delivery_root), day - 1)
    baseline_keys = {_record_key(record) for record in baseline_records}
    delta_records = [record for record in current_records if _record_key(record) not in baseline_keys]
    csv_path = delivery_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["company_name", "representative", "email"])
        writer.writeheader()
        writer.writerows(delta_records)
    keys_path = delivery_dir / "keys.txt"
    keys_path.write_text(
        "\n".join(_record_key(record) for record in current_records),
        encoding="utf-8",
    )
    summary = {
        "country": "Denmark",
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


def _load_final_records(data_root: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    if not data_root.exists():
        return records
    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir():
            continue
        path = site_dir / "final_companies.jsonl"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as fp:
            for line in fp:
                raw = line.strip()
                if not raw:
                    continue
                payload = json.loads(raw)
                records.append(
                    {
                        "company_name": str(payload.get("company_name", "")).strip(),
                        "representative": str(payload.get("representative", "")).strip(),
                        "email": str(payload.get("email", "")).strip().lower(),
                    }
                )
    return records


def _load_previous_records(delivery_root: Path, baseline_day: int) -> list[dict[str, str]]:
    if baseline_day <= 0:
        return []
    csv_path = Path(delivery_root) / f"Denmark_day{baseline_day:03d}" / "companies.csv"
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        return [
            {
                "company_name": str(row.get("company_name", "")).strip(),
                "representative": str(row.get("representative", "")).strip(),
                "email": str(row.get("email", "")).strip().lower(),
            }
            for row in reader
        ]


def _record_key(record: dict[str, str]) -> str:
    return "|".join(
        [
            str(record.get("company_name", "")).strip().lower(),
            str(record.get("representative", "")).strip().lower(),
            str(record.get("email", "")).strip().lower(),
        ]
    )


def _deduplicate(records: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in records:
        key = _record_key(record)
        if not key.replace("|", "") or key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped
