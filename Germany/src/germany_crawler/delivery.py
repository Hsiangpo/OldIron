"""Germany 交付包装。"""

from __future__ import annotations

import csv
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import prepare_delivery_dir
from oldiron_core.delivery.engine import validate_day_sequence
from oldiron_core.delivery.sanitize import sanitize_record

from germany_crawler.sites.common.store import GermanyCompanyStore


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Germany 日交付包，各站点独立落盘。"""
    day, _latest = validate_day_sequence(Path(delivery_root), "Germany", day_label)
    delivery_dir = Path(delivery_root) / f"Germany_day{day:03d}"
    baseline_day = max(day - 1, 0)
    prepare_delivery_dir(delivery_dir)

    total_current_companies = 0
    total_delta_companies = 0
    site_stats: dict[str, dict[str, int]] = {}
    skipped_sites_no_delta: list[str] = []

    if Path(data_root).exists():
        for site_dir in sorted(Path(data_root).iterdir()):
            if not site_dir.is_dir() or site_dir.name == "delivery":
                continue
            db_path = site_dir / "companies.db"
            if not db_path.exists():
                continue
            records = _load_site_records(db_path)
            raw_count = len(records)
            qualified = [record for record in records if record is not None]
            baseline_keys = _load_site_baseline_keys(Path(delivery_root), site_dir.name, baseline_day)
            delta_records = [record for record in qualified if _record_key(record) not in baseline_keys]
            current_keys = sorted(baseline_keys | {_record_key(record) for record in qualified})
            site_stats[site_dir.name] = {
                "qualified_current": len(qualified),
                "delta": len(delta_records),
            }
            total_current_companies += len(qualified)
            total_delta_companies += len(delta_records)
            if delta_records:
                _write_site_csv(delivery_dir / f"{site_dir.name}.csv", delta_records)
                (delivery_dir / f"{site_dir.name}.keys.txt").write_text("\n".join(current_keys), encoding="utf-8")
            else:
                skipped_sites_no_delta.append(site_dir.name)
            print(
                f"  {site_dir.name}: DB 总计 {raw_count} → 当前合格 {len(qualified)} 家公司 → 当日新增 {len(delta_records)} 家公司"
            )

    summary = {
        "country": "Germany",
        "day": day,
        "baseline_day": baseline_day,
        "delta_companies": total_delta_companies,
        "total_current_companies": total_current_companies,
        "sites": site_stats,
        "skipped_sites_no_delta": skipped_sites_no_delta,
    }
    (delivery_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _load_site_records(db_path: Path) -> list[dict[str, str] | None]:
    rows = GermanyCompanyStore(db_path).export_all_companies()
    records: list[dict[str, str] | None] = []
    for row in rows:
        sanitized = sanitize_record(
            {
                "company_name": str(row.get("company_name", "")).strip(),
                "representative": str(row.get("representative", "")).strip(),
                "website": str(row.get("website", "")).strip(),
                "phone": str(row.get("phone", "")).strip(),
                "evidence_url": str(row.get("evidence_url", "")).strip(),
            },
            [item.strip() for item in str(row.get("emails", "")).split(";") if item.strip()],
        )
        if sanitized is not None:
            sanitized["evidence_url"] = str(row.get("evidence_url", "")).strip()
        records.append(sanitized)
    return records


def _load_site_baseline_keys(delivery_root: Path, site_name: str, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    key_path = delivery_root / f"Germany_day{baseline_day:03d}" / f"{site_name}.keys.txt"
    if not key_path.exists():
        return set()
    return {line.strip() for line in key_path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _record_key(record: dict[str, str]) -> str:
    return "".join(ch.lower() for ch in str(record.get("company_name", "")).strip() if ch.isalnum())


def _write_site_csv(csv_path: Path, records: list[dict[str, str]]) -> None:
    fieldnames = ["company_name", "representative", "emails", "website", "phone", "evidence_url"]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
