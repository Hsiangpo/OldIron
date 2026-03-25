"""Japan 交付包装 — 新规范。

与 Denmark/England 的区别：
  - 各站点/路线独立落盘，**不合并、不去重**
  - 产出结构：Japan/output/delivery/Japan_dayN/bizmaps.csv, site2.csv, ...
  - 邮箱过滤：只保留指定白名单域名的邮箱
  - **落盘门槛**：公司名 + 代表人 + 邮箱 三者同时有值才落盘
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

# 白名单：只保留这些域名的邮箱
ALLOWED_EMAIL_DOMAINS = {
    "gmail.com",
    "icloud.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
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
            raw_count = len(records)

            # 白名单过滤邮箱域名
            for rec in records:
                if "emails" in rec:
                    rec["emails"] = _filter_emails(rec["emails"])

            # 落盘门槛：必须同时有公司名 + 代表人 + 邮箱
            qualified = [
                r for r in records
                if r.get("company_name", "").strip()
                and r.get("representative", "").strip()
                and r.get("representative", "").strip() != "-"
                and r.get("emails", "").strip()
            ]

            # 写站点独立 CSV
            csv_path = delivery_dir / f"{site_name}.csv"
            _write_site_csv(csv_path, qualified)
            site_stats[site_name] = len(qualified)
            total_companies += len(qualified)
            print(f"  {site_name}: DB 总计 {raw_count} → 合格落盘 {len(qualified)} 家公司")

    summary = {
        "country": "Japan",
        "day": day,
        "baseline_day": max(day - 1, 0),
        "total_companies": total_companies,
        # product.py 根入口需要的字段
        "delta_companies": total_companies,
        "total_current_companies": total_companies,
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

    # 探测实际存在的列名，避免查不存在的列导致 fallback 丢字段
    col_info = conn.execute("PRAGMA table_info(companies)").fetchall()
    existing_cols = {c["name"] for c in col_info}

    # 基础列 + 可选列
    base_cols = ["company_name", "representative", "website", "address",
                 "industry", "detail_url"]
    optional_cols = ["phone", "founded_year", "capital", "emails"]
    select_cols = base_cols + [c for c in optional_cols if c in existing_cols]

    sql = f"SELECT {', '.join(select_cols)} FROM companies WHERE company_name != '' ORDER BY id"
    rows = conn.execute(sql).fetchall()
    conn.close()

    records: list[dict[str, str]] = []
    for row in rows:
        d = dict(row)
        records.append({
            "company_name": str(d.get("company_name", "") or "").strip(),
            "representative": str(d.get("representative", "") or "").strip(),
            "website": str(d.get("website", "") or "").strip(),
            "address": str(d.get("address", "") or "").strip(),
            "industry": str(d.get("industry", "") or "").strip(),
            "phone": str(d.get("phone", "") or "").strip(),
            "founded_year": str(d.get("founded_year", "") or "").strip(),
            "capital": str(d.get("capital", "") or "").strip(),
            "detail_url": str(d.get("detail_url", "") or "").strip(),
            "emails": str(d.get("emails", "") or "").strip(),
        })
    return records


def _filter_emails(emails_str: str) -> str:
    """白名单过滤：只保留指定域名的邮箱。"""
    if not emails_str:
        return ""
    # 支持逗号和分号分隔
    parts = [e.strip().lower() for e in emails_str.replace(";", ",").split(",") if e.strip()]
    filtered = []
    for email in parts:
        if "@" not in email:
            continue
        domain = email.split("@")[1]
        if domain in ALLOWED_EMAIL_DOMAINS:
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

