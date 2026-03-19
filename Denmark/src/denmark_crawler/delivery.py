"""Denmark 交付包装。"""

from __future__ import annotations

import csv
import json
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
    delivery_dir.mkdir(parents=True, exist_ok=True)
    records = _load_final_records(Path(data_root))
    deduped = _deduplicate(records)
    csv_path = delivery_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["company_name", "representative", "email"])
        writer.writeheader()
        writer.writerows(deduped)
    summary = {
        "country": "Denmark",
        "day": day,
        "baseline_day": max(day - 1, 0),
        "delta_companies": len(deduped),
        "total_current_companies": len(deduped),
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


def _deduplicate(records: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for record in records:
        key = (
            record["company_name"].strip().lower(),
            record["representative"].strip().lower(),
            record["email"].strip().lower(),
        )
        if not all(key) or key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped
