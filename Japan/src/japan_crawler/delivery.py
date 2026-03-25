"""Japan 交付包装 — 新规范。

与 Denmark/England 的区别：
  - 各站点/路线独立落盘，**不合并、不去重**
  - 产出结构：Japan/output/delivery/Japan_dayN/bizmaps.csv, site2.csv, ...
  - 邮箱过滤：排除 @gmail.com, @icloud.com, @outlook.com 等个人邮箱
"""

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

# 需过滤的个人邮箱后缀
BLOCKED_EMAIL_DOMAINS = {
    "gmail.com", "icloud.com", "outlook.com",
    "hotmail.com", "live.com", "msn.com",
}

# 站点名 → 数据加载函数的映射
SITE_LOADERS = {
    "bizmaps": "_load_bizmaps_data",
}


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Japan 日交付包，各站点独立落盘。"""
    day = parse_day_label(day_label)
    delivery_dir = Path(delivery_root) / f"Japan_day{day:03d}"

    if delivery_dir.exists():
        shutil.rmtree(delivery_dir)
    delivery_dir.mkdir(parents=True, exist_ok=True)

    total_companies = 0
    site_stats: dict[str, int] = {}

    # 遍历 output/ 下每个站点目录
    if data_root.exists():
        for site_dir in sorted(data_root.iterdir()):
            if not site_dir.is_dir() or site_dir.name == "delivery":
                continue
            site_name = site_dir.name
            records = _load_site_records(site_name, site_dir)
            if not records:
                continue

            # 过滤邮箱
            for rec in records:
                if "emails" in rec:
                    rec["emails"] = _filter_emails(rec["emails"])

            # 写站点独立 CSV
            csv_path = delivery_dir / f"{site_name}.csv"
            _write_site_csv(csv_path, records)
            site_stats[site_name] = len(records)
            total_companies += len(records)
            print(f"  {site_name}: {len(records)} 家公司")

    summary = {
        "country": "Japan",
        "day": day,
        "baseline_day": max(day - 1, 0),
        "total_companies": total_companies,
        "sites": site_stats,
    }
    (delivery_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


def _load_site_records(site_name: str, site_dir: Path) -> list[dict[str, str]]:
    """根据站点名加载数据。"""
    if site_name == "bizmaps":
        return _load_bizmaps_data(site_dir)
    # 后续新站点在此扩展
    return []


def _load_bizmaps_data(site_dir: Path) -> list[dict[str, str]]:
    """从 bizmaps SQLite 加载公司数据。"""
    db_path = site_dir / "bizmaps_store.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    # 尝试查新字段（可能还不存在）
    try:
        rows = conn.execute("""
            SELECT company_name, representative, website, address, industry,
                   phone, founded_year, capital, detail_url, emails
            FROM companies
            WHERE company_name != ''
            ORDER BY id
        """).fetchall()
    except Exception:
        rows = conn.execute("""
            SELECT company_name, representative, website, address, industry, detail_url
            FROM companies WHERE company_name != '' ORDER BY id
        """).fetchall()
    conn.close()

    records: list[dict[str, str]] = []
    for row in rows:
        records.append({
            "company_name": str(row["company_name"] or "").strip(),
            "representative": str(row["representative"] or "").strip(),
            "website": str(row["website"] or "").strip(),
            "address": str(row["address"] or "").strip(),
            "industry": str(row["industry"] or "").strip(),
            "phone": str(dict(row).get("phone", "") or "").strip(),
            "founded_year": str(dict(row).get("founded_year", "") or "").strip(),
            "capital": str(dict(row).get("capital", "") or "").strip(),
            "detail_url": str(row["detail_url"] or "").strip(),
            "emails": str(dict(row).get("emails", "") or "").strip(),
        })
    return records


def _filter_emails(emails_str: str) -> str:
    """过滤掉个人邮箱后缀。"""
    if not emails_str:
        return ""
    # 支持逗号和分号分隔
    parts = [e.strip().lower() for e in emails_str.replace(";", ",").split(",") if e.strip()]
    filtered = []
    for email in parts:
        if "@" not in email:
            continue
        domain = email.split("@")[1]
        if domain in BLOCKED_EMAIL_DOMAINS:
            continue
        filtered.append(email)
    return "; ".join(filtered)


def _write_site_csv(csv_path: Path, records: list[dict[str, str]]) -> None:
    """写站点级 CSV。"""
    fieldnames = [
        "company_name", "representative", "website", "emails",
        "phone", "address", "industry", "founded_year", "capital", "detail_url",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
