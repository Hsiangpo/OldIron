"""Japan 交付包装。"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import validate_day_sequence


PERSONAL_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "yahoo.co.jp",
    "hotmail.com",
    "outlook.com",
    "outlook.jp",
    "icloud.com",
    "live.com",
    "live.jp",
    "msn.com",
    "me.com",
    "aol.com",
    "docomo.ne.jp",
    "softbank.ne.jp",
    "ezweb.ne.jp",
    "au.com",
    "i.softbank.jp",
    "ymobile.ne.jp",
    "nifty.com",
    "ocn.ne.jp",
    "plala.or.jp",
    "biglobe.ne.jp",
    "so-net.ne.jp",
    "dion.ne.jp",
    "infoweb.ne.jp",
    "gol.com",
    "jcom.home.ne.jp",
    "ybb.ne.jp",
}
_SITE_FILTER_ENV = "JAPAN_DELIVERY_SITES"
_SUMMARY_ONLY_ENV = "JAPAN_DELIVERY_SUMMARY_ONLY"


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Japan 日交付包，各站点独立落盘。"""
    delivery_root = Path(delivery_root)
    day, _latest = validate_day_sequence(delivery_root, "Japan", day_label)
    delivery_dir = delivery_root / f"Japan_day{day:03d}"
    baseline_day = max(day - 1, 0)
    site_filter = _load_site_filter()
    summary_only = _summary_only_enabled()

    _prepare_japan_delivery_dir(delivery_dir)
    _render_local_site_outputs(
        data_root=Path(data_root),
        delivery_root=delivery_root,
        delivery_dir=delivery_dir,
        baseline_day=baseline_day,
        site_filter=site_filter,
        summary_only=summary_only,
    )
    summary = _build_summary_from_delivery_dir(delivery_dir=delivery_dir, day=day, baseline_day=baseline_day)
    (delivery_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


def _prepare_japan_delivery_dir(delivery_dir: Path) -> None:
    """Japan 同日重跑需要保留已落盘站点文件，只确保目录存在。"""
    delivery_dir.mkdir(parents=True, exist_ok=True)


def _render_local_site_outputs(
    *,
    data_root: Path,
    delivery_root: Path,
    delivery_dir: Path,
    baseline_day: int,
    site_filter: set[str] | None,
    summary_only: bool,
) -> None:
    if summary_only:
        print("  Japan 汇总模式：仅重建 summary.json，不重打本站点文件")
        return
    selected_dirs = _iter_selected_site_dirs(data_root, site_filter)
    if site_filter:
        print(f"  Japan 站点过滤：{', '.join(sorted(site_filter))}")
    for site_dir in selected_dirs:
        site_stats = _write_site_delivery_assets(
            site_name=site_dir.name,
            site_dir=site_dir,
            delivery_root=delivery_root,
            delivery_dir=delivery_dir,
            baseline_day=baseline_day,
        )
        if site_stats is None:
            continue
        print(
            "  {site}: DB 总计 {raw} → 当前合格 {qualified} 家公司 → 当日新增 {delta} 家公司".format(
                site=site_dir.name,
                raw=site_stats["raw_count"],
                qualified=site_stats["qualified_current"],
                delta=site_stats["delta"],
            )
        )


def _iter_selected_site_dirs(data_root: Path, site_filter: set[str] | None) -> list[Path]:
    if not data_root.exists():
        return []
    selected: list[Path] = []
    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir() or site_dir.name == "delivery":
            continue
        if site_filter and site_dir.name.lower() not in site_filter:
            continue
        selected.append(site_dir)
    return selected


def _write_site_delivery_assets(
    *,
    site_name: str,
    site_dir: Path,
    delivery_root: Path,
    delivery_dir: Path,
    baseline_day: int,
) -> dict[str, int] | None:
    db_path = _detect_site_db_path(site_name, site_dir)
    if not db_path.exists():
        return None
    records = _load_site_records(site_name, site_dir)
    raw_count = len(records)
    for record in records:
        if "emails" in record:
            record["emails"] = _filter_emails(record["emails"])
    qualified = [
        record
        for record in records
        if record.get("company_name", "").strip()
        and record.get("representative", "").strip()
        and record.get("representative", "").strip() != "-"
        and record.get("emails", "").strip()
    ]
    baseline_keys = _load_site_baseline_keys(
        delivery_root=delivery_root,
        site_name=site_name,
        baseline_day=baseline_day,
    )
    delta_records = [record for record in qualified if _record_key(record) not in baseline_keys]
    current_keys = sorted(baseline_keys | {_record_key(record) for record in qualified})
    _write_site_csv(delivery_dir / f"{site_name}.csv", delta_records)
    (delivery_dir / f"{site_name}.keys.txt").write_text("\n".join(current_keys), encoding="utf-8")
    return {
        "raw_count": raw_count,
        "qualified_current": len(qualified),
        "delta": len(delta_records),
    }


def _build_summary_from_delivery_dir(*, delivery_dir: Path, day: int, baseline_day: int) -> dict[str, object]:
    site_stats: dict[str, dict[str, int]] = {}
    total_current_companies = 0
    total_delta_companies = 0
    for site_name in _discover_delivery_sites(delivery_dir):
        current_count = _count_key_lines(delivery_dir / f"{site_name}.keys.txt")
        delta_count = _count_csv_rows(delivery_dir / f"{site_name}.csv")
        if current_count <= 0 and delta_count <= 0:
            continue
        site_stats[site_name] = {
            "qualified_current": current_count,
            "delta": delta_count,
        }
        total_current_companies += current_count
        total_delta_companies += delta_count
    return {
        "country": "Japan",
        "day": day,
        "baseline_day": baseline_day,
        "delta_companies": total_delta_companies,
        "total_current_companies": total_current_companies,
        "sites": site_stats,
    }


def _discover_delivery_sites(delivery_dir: Path) -> list[str]:
    site_names: set[str] = set()
    for csv_path in delivery_dir.glob("*.csv"):
        site_names.add(csv_path.stem)
    for key_path in delivery_dir.glob("*.keys.txt"):
        site_names.add(key_path.name[: -len(".keys.txt")])
    return sorted(site_names)


def _count_key_lines(key_path: Path) -> int:
    if not key_path.exists():
        return 0
    return sum(1 for line in key_path.read_text(encoding="utf-8").splitlines() if line.strip())


def _count_csv_rows(csv_path: Path) -> int:
    if not csv_path.exists():
        return 0
    with csv_path.open(encoding="utf-8-sig", newline="") as fp:
        return sum(1 for _ in csv.DictReader(fp))


def _load_site_filter() -> set[str] | None:
    raw = str(os.getenv(_SITE_FILTER_ENV, "") or "").strip()
    if not raw:
        return None
    values = {
        item.strip().lower()
        for item in raw.replace(";", ",").split(",")
        if item.strip()
    }
    return values or None


def _summary_only_enabled() -> bool:
    return str(os.getenv(_SUMMARY_ONLY_ENV, "") or "").strip().lower() in {"1", "true", "yes", "on"}


def _load_site_records(site_name: str, site_dir: Path) -> list[dict[str, str]]:
    db_path = _detect_site_db_path(site_name, site_dir)
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        return _load_company_records(conn)
    finally:
        conn.close()


def _detect_site_db_path(site_name: str, site_dir: Path) -> Path:
    primary = site_dir / f"{site_name}_store.db"
    if primary.exists():
        return primary
    matches = sorted(site_dir.glob("*_store.db"))
    if len(matches) == 1:
        return matches[0]
    return primary


def _load_company_records(conn: sqlite3.Connection) -> list[dict[str, str]]:
    try:
        col_info = conn.execute("PRAGMA table_info(companies)").fetchall()
    except sqlite3.OperationalError:
        return []
    existing_cols = {column["name"] for column in col_info}
    if "company_name" not in existing_cols:
        return []
    select_cols = _build_select_columns(existing_cols)
    order_by = "id" if "id" in existing_cols else "company_name"
    rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM companies WHERE company_name != '' AND company_name IS NOT NULL ORDER BY {order_by}"
    ).fetchall()
    return [_normalize_company_record(row, existing_cols) for row in rows]


def _build_select_columns(existing_cols: set[str]) -> list[str]:
    base_cols = ["company_name", "representative", "website", "address", "industry", "detail_url"]
    optional_cols = ["phone", "founded_year", "capital", "emails", "email", "source_job_url"]
    return [column for column in [*base_cols, *optional_cols] if column in existing_cols]


def _normalize_company_record(row: sqlite3.Row, existing_cols: set[str]) -> dict[str, str]:
    emails_value = ""
    if "emails" in existing_cols:
        emails_value = str(row["emails"] or "").strip()
    elif "email" in existing_cols:
        emails_value = str(row["email"] or "").strip()
    return {
        "company_name": str(row["company_name"] or "").strip(),
        "representative": str(row["representative"] or "").strip() if "representative" in existing_cols else "",
        "website": str(row["website"] or "").strip() if "website" in existing_cols else "",
        "address": str(row["address"] or "").strip() if "address" in existing_cols else "",
        "industry": str(row["industry"] or "").strip() if "industry" in existing_cols else "",
        "phone": str(row["phone"] or "").strip() if "phone" in existing_cols else "",
        "founded_year": str(row["founded_year"] or "").strip() if "founded_year" in existing_cols else "",
        "capital": str(row["capital"] or "").strip() if "capital" in existing_cols else "",
        "detail_url": str(row["detail_url"] or "").strip() if "detail_url" in existing_cols else "",
        "emails": emails_value,
        "source_job_url": str(row["source_job_url"] or "").strip() if "source_job_url" in existing_cols else "",
    }


def _filter_emails(emails_str: str) -> str:
    if not emails_str:
        return ""
    parts = [part.strip().lower() for part in emails_str.replace(";", ",").split(",") if part.strip()]
    filtered: list[str] = []
    for email in parts:
        if "@" not in email:
            continue
        domain = email.split("@", 1)[1]
        if domain in PERSONAL_EMAIL_DOMAINS and email not in filtered:
            filtered.append(email)
    return "; ".join(filtered)


def _record_key(record: dict[str, str]) -> str:
    parts = [
        str(record.get("company_name", "") or "").strip().lower(),
        str(record.get("representative", "") or "").strip().lower(),
        str(record.get("website", "") or "").strip().lower(),
        str(record.get("address", "") or "").strip().lower(),
    ]
    return "|".join(parts)


def _normalize_key_text(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    parts = [part.strip().lower() for part in text.split("|")]
    return "|".join(parts)


def _load_site_baseline_keys(*, delivery_root: Path, site_name: str, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    baseline_dir = delivery_root / f"Japan_day{baseline_day:03d}"
    key_path = baseline_dir / f"{site_name}.keys.txt"
    if key_path.exists():
        return {
            _normalize_key_text(line)
            for line in key_path.read_text(encoding="utf-8").splitlines()
            if _normalize_key_text(line)
        }
    csv_path = baseline_dir / f"{site_name}.csv"
    if csv_path.exists():
        with csv_path.open(encoding="utf-8-sig", newline="") as fp:
            return {
                _record_key(row)
                for row in csv.DictReader(fp)
                if row.get("company_name", "").strip()
            }
    legacy_country_keys = baseline_dir / "keys.txt"
    if legacy_country_keys.exists():
        return {
            _normalize_key_text(line)
            for line in legacy_country_keys.read_text(encoding="utf-8").splitlines()
            if _normalize_key_text(line)
        }
    legacy_country_csv = baseline_dir / "companies.csv"
    if not legacy_country_csv.exists():
        return set()
    with legacy_country_csv.open(encoding="utf-8-sig", newline="") as fp:
        return {
            _record_key(row)
            for row in csv.DictReader(fp)
            if row.get("company_name", "").strip()
        }


def _write_site_csv(csv_path: Path, records: list[dict[str, str]]) -> None:
    fieldnames = [
        "company_name",
        "representative",
        "website",
        "emails",
        "phone",
        "address",
        "industry",
        "founded_year",
        "capital",
        "detail_url",
        "source_job_url",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
