"""UnitedStates 交付包装。"""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from pathlib import Path
import re


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import prepare_delivery_dir
from oldiron_core.delivery.engine import validate_day_sequence


def build_delivery_bundle(
    data_root: Path,
    delivery_root: Path,
    day_label: str,
    *,
    delivery_kind: str = "companies",
) -> dict[str, object]:
    """构建 UnitedStates 交付包。"""
    if delivery_kind == "websites":
        return _build_websites_delivery_bundle(data_root, delivery_root, day_label)
    day, _latest = validate_day_sequence(Path(delivery_root), "UnitedStates", day_label)
    delivery_dir = Path(delivery_root) / f"UnitedStates_day{day:03d}"
    baseline_day = max(day - 1, 0)

    prepare_delivery_dir(delivery_dir)

    total_current_companies = 0
    total_delta_companies = 0
    site_stats: dict[str, dict[str, int]] = {}
    skipped_sites_no_delta: list[str] = []

    if data_root.exists():
        for site_dir in sorted(data_root.iterdir()):
            if not site_dir.is_dir() or site_dir.name == "delivery":
                continue
            site_name = site_dir.name
            records = _load_site_records(site_name, site_dir)
            if not records:
                continue
            raw_count = len(records)
            qualified = [
                record for record in records
                if record.get("company_name", "").strip()
                and record.get("representative", "").strip()
                and record.get("emails", "").strip()
            ]
            baseline_keys = _load_site_baseline_keys(
                delivery_root=Path(delivery_root),
                site_name=site_name,
                baseline_day=baseline_day,
            )
            delta_records = [record for record in qualified if _record_key(record) not in baseline_keys]
            current_keys = sorted(baseline_keys | {_record_key(record) for record in qualified})
            site_stats[site_name] = {
                "qualified_current": len(qualified),
                "delta": len(delta_records),
            }
            total_current_companies += len(qualified)
            total_delta_companies += len(delta_records)
            if delta_records:
                csv_path = delivery_dir / f"{site_name}.csv"
                _write_site_csv(csv_path, delta_records)
                (delivery_dir / f"{site_name}.keys.txt").write_text(
                    "\n".join(current_keys), encoding="utf-8"
                )
            else:
                skipped_sites_no_delta.append(site_name)
                print(
                    f"  {site_name}: DB 总计 {raw_count} → 当前合格 {len(qualified)} 家公司 → 当日新增 {len(delta_records)} 家公司（无新增，已跳过交付文件）"
                )
                continue
            print(
                f"  {site_name}: DB 总计 {raw_count} → 当前合格 {len(qualified)} 家公司 → 当日新增 {len(delta_records)} 家公司"
            )

    summary = {
        "country": "UnitedStates",
        "day": day,
        "baseline_day": baseline_day,
        "delta_companies": total_delta_companies,
        "total_current_companies": total_current_companies,
        "sites": site_stats,
        "skipped_sites_no_delta": skipped_sites_no_delta,
    }
    (delivery_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


def _load_site_records(site_name: str, site_dir: Path) -> list[dict[str, str]]:
    if site_name == "dnb":
        return _load_dnb_data(site_dir)
    return []


def _load_dnb_data(site_dir: Path) -> list[dict[str, str]]:
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


def _load_site_baseline_keys(*, delivery_root: Path, site_name: str, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    baseline_dir = delivery_root / f"UnitedStates_day{baseline_day:03d}"
    key_path = baseline_dir / f"{site_name}.keys.txt"
    if key_path.exists():
        return {
            line.strip()
            for line in key_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    return set()


def _write_site_csv(csv_path: Path, records: list[dict[str, str]]) -> None:
    fieldnames = ["company_name", "representative", "emails", "website", "phone", "address", "evidence_url"]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def _write_websites_csv(csv_path: Path, websites: list[str]) -> None:
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["website"])
        writer.writeheader()
        writer.writerows({"website": website} for website in websites)


def _build_websites_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    day, latest = _validate_websites_day_sequence(Path(delivery_root), "UnitedStates", day_label)
    delivery_dir = Path(delivery_root) / f"UnitedStates_websites_day{day:03d}"
    baseline_day = max(day - 1, 0)
    prepare_delivery_dir(delivery_dir)
    total_current_websites = 0
    total_delta_websites = 0
    site_stats: dict[str, dict[str, int]] = {}
    skipped_sites_no_delta: list[str] = []
    for site_dir in _iter_website_site_dirs(Path(data_root)):
        current_websites = _load_site_websites(site_dir / "websites.txt")
        baseline_keys = _load_site_websites_baseline_keys(
            Path(delivery_root),
            "UnitedStates",
            site_dir.name,
            baseline_day,
        )
        delta_websites = [item for item in current_websites if item not in baseline_keys]
        if delta_websites:
            _write_websites_csv(delivery_dir / f"{site_dir.name}.csv", delta_websites)
        else:
            skipped_sites_no_delta.append(site_dir.name)
        (delivery_dir / f"{site_dir.name}.keys.txt").write_text("\n".join(current_websites), encoding="utf-8")
        site_stats[site_dir.name] = {
            "qualified_current": len(current_websites),
            "delta": len(delta_websites),
        }
        total_current_websites += len(current_websites)
        total_delta_websites += len(delta_websites)
    summary = {
        "country": "UnitedStates",
        "day": day,
        "baseline_day": 0 if latest == 0 else baseline_day,
        "delta_websites": total_delta_websites,
        "total_current_websites": total_current_websites,
        "sites": site_stats,
        "skipped_sites_no_delta": skipped_sites_no_delta,
    }
    (delivery_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


def _validate_websites_day_sequence(delivery_root: Path, country_name: str, day_label: str) -> tuple[int, int]:
    from oldiron_core.delivery.engine import parse_day_label

    target_day = parse_day_label(day_label)
    pattern = re.compile(rf"{re.escape(country_name)}_websites_day(\d{{3}})$")
    existing_days = [
        int(matched.group(1))
        for item in delivery_root.iterdir()
        if item.is_dir()
        for matched in [pattern.fullmatch(item.name)]
        if matched
    ] if delivery_root.exists() else []
    latest = max(existing_days, default=0)
    if latest == 0 and target_day != 1:
        raise ValueError("尚未有网站交付记录，首个交付只能执行 day1。")
    if target_day < latest:
        raise ValueError(f"网站第{target_day}天已交付，当前最新是第{latest}天。")
    if target_day > latest + 1:
        raise ValueError(f"网站交付只能执行 day{latest}（重跑）或 day{latest + 1}（新一天）。")
    return target_day, latest


def _iter_website_site_dirs(data_root: Path) -> list[Path]:
    if not data_root.exists():
        return []
    selected: list[Path] = []
    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir() or site_dir.name == "delivery":
            continue
        if not (site_dir / "websites.txt").exists():
            continue
        selected.append(site_dir)
    return selected


def _load_site_websites(site_websites_path: Path) -> list[str]:
    websites: set[str] = set()
    for line in site_websites_path.read_text(encoding="utf-8").splitlines():
        website = str(line or "").strip()
        if website:
            websites.add(website)
    return sorted(websites)


def _load_site_websites_baseline_keys(
    delivery_root: Path,
    country_name: str,
    site_name: str,
    baseline_day: int,
) -> set[str]:
    if baseline_day <= 0:
        return set()
    keys_path = delivery_root / f"{country_name}_websites_day{baseline_day:03d}" / f"{site_name}.keys.txt"
    if not keys_path.exists():
        return set()
    return {line.strip() for line in keys_path.read_text(encoding="utf-8").splitlines() if line.strip()}
