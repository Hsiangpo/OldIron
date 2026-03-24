"""Finland 交付包装。

从三个站点的 SQLite DB 读取 final_companies，
按公司名去重后输出 CSV + keys.txt + summary.json。
"""

from __future__ import annotations

import csv
import json
import shutil
import sqlite3
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import parse_day_label


# 三个站点各自的 output 子目录名和 DB 文件名
_SITE_DB_MAP: list[tuple[str, str]] = [
    ("tyomarkkinatori", "tmt_store.db"),
    ("duunitori", "duunitori_store.db"),
    ("jobly", "jobly_store.db"),
]

# 通用免费邮箱域名，交付时过滤掉
_GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "live.com",
    "icloud.com", "me.com", "msn.com", "aol.com", "protonmail.com",
    "yahoo.fi", "mail.com",
}


def build_delivery_bundle(
    data_root: Path, delivery_root: Path, day_label: str,
) -> dict[str, object]:
    """构建 Finland 日交付包。"""
    day = parse_day_label(day_label)
    delivery_dir = Path(delivery_root) / f"Finland_day{day:03d}"
    if delivery_dir.exists():
        shutil.rmtree(delivery_dir)
    delivery_dir.mkdir(parents=True, exist_ok=True)

    current_records = _load_and_merge_records(Path(data_root))
    baseline_keys = _load_baseline_keys(Path(delivery_root), day - 1)
    delta_records = [r for r in current_records if _record_key(r) not in baseline_keys]

    # 写 CSV
    csv_path = delivery_dir / "companies.csv"
    fieldnames = ["company_name", "representative", "emails", "website", "phone", "evidence_url"]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(delta_records)

    # 写去重键（全量）
    keys_path = delivery_dir / "keys.txt"
    keys_path.write_text(
        "\n".join(_record_key(r) for r in current_records),
        encoding="utf-8",
    )

    summary = {
        "country": "Finland",
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


def _load_and_merge_records(data_root: Path) -> list[dict[str, str]]:
    """从三个站点的 SQLite 读取 final_companies，按公司名去重合并。"""
    grouped: dict[str, dict[str, str | list[str]]] = {}

    for site_dir_name, db_name in _SITE_DB_MAP:
        db_path = data_root / site_dir_name / db_name
        if not db_path.exists():
            continue
        _load_from_site_db(db_path, site_dir_name, grouped)

    # 过滤邮箱 + 组装输出
    records: list[dict[str, str]] = []
    for entry in grouped.values():
        emails_list = entry.pop("_emails", [])
        website = str(entry.get("website", "")).strip()
        site_domain = ""
        if website:
            try:
                site_domain = urlparse(website).netloc.replace("www.", "").lower()
            except Exception:
                pass
        # 保留和官网域名相关的邮箱，丢弃通用免费邮箱
        if site_domain:
            filtered = []
            for em in emails_list:
                if "@" not in em:
                    continue
                em_domain = em.split("@")[1].strip().lower()
                if em_domain in _GENERIC_DOMAINS:
                    continue
                if _domains_related(em_domain, site_domain):
                    filtered.append(em)
            emails_list = filtered
        # 丢弃域名首段过短的邮箱（如 bc@v.fi）
        emails_list = [
            em for em in emails_list
            if "@" not in em or len(em.split("@")[1].split(".")[0]) >= 3
        ]
        entry["emails"] = "; ".join(emails_list)
        if entry["emails"]:
            records.append(entry)
    return records


def _load_from_site_db(db_path: Path, site_name: str, grouped: dict) -> None:
    """从某站点的 SQLite 加载 final_companies + jobs 表。"""
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    # final_companies 表包含 job_id, company_name, representative, email, phone, homepage, evidence_url
    rows = conn.execute("""
        SELECT fc.company_name, fc.representative, fc.email,
               fc.phone, fc.homepage, fc.evidence_url
        FROM final_companies fc
    """).fetchall()
    conn.close()

    for row in rows:
        name = str(row["company_name"] or "").strip()
        if not name:
            continue
        key = name.lower()
        email = str(row["email"] or "").strip().lower()
        homepage = unquote(str(row["homepage"] or "").strip())
        if key not in grouped:
            grouped[key] = {
                "company_name": name,
                "representative": str(row["representative"] or "").strip(),
                "website": homepage,
                "phone": str(row["phone"] or "").strip(),
                "evidence_url": str(row["evidence_url"] or "").strip(),
                "_emails": [],
            }
        else:
            # 补充缺失字段
            existing = grouped[key]
            for field_src, field_dst in [
                ("representative", "representative"),
                ("phone", "phone"),
                ("evidence_url", "evidence_url"),
            ]:
                if not str(existing.get(field_dst, "")).strip():
                    val = str(row[field_src] or "").strip()
                    if val:
                        existing[field_dst] = val
            if not str(existing.get("website", "")).strip() and homepage:
                existing["website"] = homepage
        if email and email not in grouped[key]["_emails"]:
            grouped[key]["_emails"].append(email)


def _domains_related(d1: str, d2: str) -> bool:
    """判断两个域名是否相关。"""
    if d1 == d2 or d1.endswith("." + d2) or d2.endswith("." + d1):
        return True
    p1 = d1.split(".")[0] if "." in d1 else d1
    p2 = d2.split(".")[0] if "." in d2 else d2
    if len(p1) >= 3 and len(p2) >= 3:
        if p1 in p2 or p2 in p1:
            return True
    return False


def _load_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    """加载前一天交付的公司去重键集合。"""
    if baseline_day <= 0:
        return set()
    csv_path = Path(delivery_root) / f"Finland_day{baseline_day:03d}" / "companies.csv"
    if not csv_path.exists():
        return set()
    keys: set[str] = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
        for row in csv.DictReader(fp):
            keys.add(str(row.get("company_name", "")).strip().lower())
    return keys


def _record_key(record: dict[str, str]) -> str:
    """去重键：按公司名。"""
    return str(record.get("company_name", "")).strip().lower()
