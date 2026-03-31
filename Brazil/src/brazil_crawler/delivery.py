"""Brazil 交付包装。

与 Japan 保持一致：
  - 各站点独立落盘，不合并、不去重
  - 产出结构：Brazil/output/delivery/Brazil_dayN/dnb.csv, site2.csv, ...
  - 邮箱：全部保留，不过滤
  - 落盘门槛：公司名 + 代表人 + 邮箱 三者同时有值才落盘
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


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Brazil 日交付包，各站点独立落盘。"""
    day = parse_day_label(day_label)
    delivery_dir = Path(delivery_root) / f"Brazil_day{day:03d}"
    baseline_day = max(day - 1, 0)

    if delivery_dir.exists():
        shutil.rmtree(delivery_dir)
    delivery_dir.mkdir(parents=True, exist_ok=True)

    total_current_companies = 0
    total_delta_companies = 0
    site_stats: dict[str, dict[str, int]] = {}

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

            # 落盘门槛：公司名 + 代表人 + 邮箱 三者同时有值
            qualified = [
                r for r in records
                if r.get("company_name", "").strip()
                and r.get("representative", "").strip()
                and r.get("emails", "").strip()
            ]
            baseline_keys = _load_site_baseline_keys(
                delivery_root=Path(delivery_root),
                site_name=site_name,
                baseline_day=baseline_day,
            )
            delta_records = [r for r in qualified if _record_key(r) not in baseline_keys]
            current_keys = sorted(baseline_keys | {_record_key(r) for r in qualified})

            # 写站点独立 CSV
            csv_path = delivery_dir / f"{site_name}.csv"
            _write_site_csv(csv_path, delta_records)
            (delivery_dir / f"{site_name}.keys.txt").write_text(
                "\n".join(current_keys), encoding="utf-8"
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
        "country": "Brazil",
        "day": day,
        "baseline_day": baseline_day,
        "total_companies": total_current_companies,
        "delta_companies": total_delta_companies,
        "total_current_companies": total_current_companies,
        "sites": site_stats,
    }
    (delivery_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


def _load_site_records(site_name: str, site_dir: Path) -> list[dict[str, str]]:
    """根据站点名加载数据。"""
    if site_name == "dnb":
        return _load_dnb_data(site_dir)
    return []


def _load_dnb_data(site_dir: Path) -> list[dict[str, str]]:
    """从 DNB SQLite 加载 final_companies 数据。"""
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

    # 按交付键归并，只合并完全相同的交付实体，不提前按公司名压扁
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
    for _key, group in grouped.items():
        def _score(r: sqlite3.Row) -> int:
            score = 0
            for f in ("representative", "website", "phone", "address", "evidence_url"):
                if str(r[f] or "").strip():
                    score += 1
            score += len([e for e in str(r["emails"] or "").split(";") if e.strip()])
            return score

        group.sort(key=_score, reverse=True)
        best = group[0]
        # 合并所有同名记录的邮箱
        seen: set[str] = set()
        all_emails: list[str] = []
        for row in group:
            for item in str(row["emails"] or "").split(";"):
                email = item.strip().lower()
                if email and email not in seen:
                    seen.add(email)
                    all_emails.append(email)
        records.append({
            "company_name": str(best["company_name"] or "").strip(),
            "representative": str(best["representative"] or "").strip(),
            "emails": "; ".join(all_emails),
            "website": str(best["website"] or "").strip(),
            "phone": str(best["phone"] or "").strip(),
            "address": str(best["address"] or "").strip(),
            "evidence_url": str(best["evidence_url"] or "").strip(),
        })
    return records


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
    baseline_dir = delivery_root / f"Brazil_day{baseline_day:03d}"
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
