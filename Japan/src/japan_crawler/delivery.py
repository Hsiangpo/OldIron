"""Japan 交付包装。"""

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


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Japan 日交付包，各站点独立落盘。"""
    day, _latest = validate_day_sequence(Path(delivery_root), "Japan", day_label)
    delivery_dir = Path(delivery_root) / f"Japan_day{day:03d}"
    baseline_day = max(day - 1, 0)

    prepare_delivery_dir(delivery_dir)

    total_current_companies = 0
    total_delta_companies = 0
    site_stats: dict[str, dict[str, int]] = {}

    if data_root.exists():
        for site_dir in sorted(data_root.iterdir()):
            if not site_dir.is_dir() or site_dir.name == "delivery":
                continue
            site_name = site_dir.name
            records = _load_site_records(site_name, site_dir)
            if not records:
                continue
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
                delivery_root=Path(delivery_root),
                site_name=site_name,
                baseline_day=baseline_day,
            )
            delta_records = [record for record in qualified if _record_key(record) not in baseline_keys]
            current_keys = sorted(baseline_keys | {_record_key(record) for record in qualified})

            csv_path = delivery_dir / f"{site_name}.csv"
            _write_site_csv(csv_path, delta_records)
            (delivery_dir / f"{site_name}.keys.txt").write_text(
                "\n".join(current_keys),
                encoding="utf-8",
            )
            site_stats[site_name] = {
                "qualified_current": len(qualified),
                "delta": len(delta_records),
            }
            total_current_companies += len(qualified)
            total_delta_companies += len(delta_records)
            print(
                f"  {site_name}: DB 总计 {raw_count} → 当前合格 {len(qualified)} 家公司 → 当日新增 {len(delta_records)} 家公司"
            )

    summary = {
        "country": "Japan",
        "day": day,
        "baseline_day": baseline_day,
        "delta_companies": total_delta_companies,
        "total_current_companies": total_current_companies,
        "sites": site_stats,
    }
    (delivery_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


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

    records: list[dict[str, str]] = []
    for row in rows:
        records.append(
            {
                "company_name": str(row["company_name"] or "").strip(),
                "representative": str(row["representative"] or "").strip(),
                "website": str(row["website"] or "").strip(),
                "emails": str(row["email"] or "").strip(),
            }
        )
    return records


def _load_hellowork_data(site_dir: Path) -> list[dict[str, str]]:
    db_path = site_dir / "hellowork_store.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT company_name, representative, website, address,
               industry, phone, employees, capital, founded_year,
               corp_number, detail_url, emails
        FROM companies
        WHERE company_name != '' AND company_name IS NOT NULL
        ORDER BY id
        """
    ).fetchall()
    conn.close()

    records: list[dict[str, str]] = []
    for row in rows:
        records.append(
            {
                "company_name": str(row["company_name"] or "").strip(),
                "representative": str(row["representative"] or "").strip(),
                "website": str(row["website"] or "").strip(),
                "address": str(row["address"] or "").strip(),
                "industry": str(row["industry"] or "").strip(),
                "phone": str(row["phone"] or "").strip(),
                "founded_year": str(row["founded_year"] or "").strip(),
                "capital": str(row["capital"] or "").strip(),
                "detail_url": str(row["detail_url"] or "").strip(),
                "emails": str(row["emails"] or "").strip(),
            }
        )
    return records


def _load_bizmaps_data(site_dir: Path) -> list[dict[str, str]]:
    db_path = site_dir / "bizmaps_store.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    col_info = conn.execute("PRAGMA table_info(companies)").fetchall()
    existing_cols = {column["name"] for column in col_info}
    base_cols = ["company_name", "representative", "website", "address", "industry", "detail_url"]
    optional_cols = ["phone", "founded_year", "capital", "emails"]
    select_cols = base_cols + [column for column in optional_cols if column in existing_cols]
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
                "address": str(row["address"] or "").strip(),
                "industry": str(row["industry"] or "").strip(),
                "phone": str(row["phone"] or "").strip() if "phone" in existing_cols else "",
                "founded_year": str(row["founded_year"] or "").strip() if "founded_year" in existing_cols else "",
                "capital": str(row["capital"] or "").strip() if "capital" in existing_cols else "",
                "detail_url": str(row["detail_url"] or "").strip(),
                "emails": str(row["emails"] or "").strip() if "emails" in existing_cols else "",
            }
        )
    return records


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
    parts = (
        str(record.get("company_name", "") or "").strip().lower(),
        str(record.get("representative", "") or "").strip().lower(),
        str(record.get("website", "") or "").strip().lower(),
        str(record.get("address", "") or "").strip().lower(),
    )
    return " | ".join(parts)


def _load_site_baseline_keys(*, delivery_root: Path, site_name: str, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    baseline_dir = delivery_root / f"Japan_day{baseline_day:03d}"
    key_path = baseline_dir / f"{site_name}.keys.txt"
    if key_path.exists():
        return {
            line.strip()
            for line in key_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
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
            line.strip()
            for line in legacy_country_keys.read_text(encoding="utf-8").splitlines()
            if line.strip()
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
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
