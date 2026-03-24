"""England 交付包装。"""

from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import parse_day_label
from oldiron_core.delivery.sanitize import sanitize_record


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 England 日交付包，每家公司一行，邮箱合并到 emails 字段。"""
    day = parse_day_label(day_label)
    delivery_dir = Path(delivery_root) / f"England_day{day:03d}"
    if delivery_dir.exists():
        shutil.rmtree(delivery_dir)
    delivery_dir.mkdir(parents=True, exist_ok=True)
    current_records = _load_and_merge_records(Path(data_root))
    baseline_keys = _load_baseline_keys(Path(delivery_root), day - 1)
    delta_records = [r for r in current_records if _record_key(r) not in baseline_keys]
    csv_path = delivery_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["company_name", "representative", "emails", "website", "phone", "evidence_url"])
        writer.writeheader()
        writer.writerows(delta_records)
    keys_path = delivery_dir / "keys.txt"
    keys_path.write_text(
        "\n".join(_record_key(r) for r in current_records),
        encoding="utf-8",
    )
    summary = {
        "country": "England",
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
    """直接从 SQLite 读取 final_companies + companies 数据，实时反映最新结果。"""
    import sqlite3

    grouped: dict[str, dict[str, str | list[str]]] = {}
    if not data_root.exists():
        return []
    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir():
            continue
        db_path = site_dir / "store.db"
        if not db_path.exists():
            # 兼容旧 JSONL 模式
            _load_from_jsonl(site_dir, grouped)
            continue
        conn = sqlite3.connect(str(db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT fc.orgnr, fc.company_name, fc.representative, fc.email,
                   fc.evidence_url,
                   COALESCE(NULLIF(c.phone,''), c.gmap_phone, '') AS phone,
                   c.homepage
            FROM final_companies fc
            LEFT JOIN companies c ON c.orgnr = fc.orgnr
        """).fetchall()
        conn.close()
        for row in rows:
            key = str(row["orgnr"] or row["company_name"] or "").strip()
            if not key:
                continue
            email = str(row["email"] or "").strip().lower()
            homepage = str(row["homepage"] or "").strip()
            if key not in grouped:
                grouped[key] = {
                    "company_name": str(row["company_name"] or "").strip(),
                    "representative": str(row["representative"] or "").strip(),
                    "website": homepage,
                    "phone": str(row["phone"] or "").strip(),
                    "evidence_url": str(row["evidence_url"] or "").strip(),
                    "_emails": [],
                }
            if email and email not in grouped[key]["_emails"]:
                grouped[key]["_emails"].append(email)

    # 按 company_name.lower() 合并
    merged: dict[str, dict[str, str | list[str]]] = {}
    for entry in grouped.values():
        name_key = str(entry.get("company_name", "")).strip().lower()
        if not name_key:
            continue
        if name_key not in merged:
            merged[name_key] = entry
        else:
            existing = merged[name_key]
            for em in entry.get("_emails", []):
                if em not in existing["_emails"]:
                    existing["_emails"].append(em)
            for field in ("representative", "website", "phone", "evidence_url"):
                if not str(existing.get(field, "")).strip() and str(entry.get(field, "")).strip():
                    existing[field] = entry[field]

    # 过滤邮箱
    generic_domains = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "live.com",
        "icloud.com", "me.com", "yahoo.co.uk", "msn.com", "aol.com",
        "protonmail.com", "btinternet.com", "sky.com", "virginmedia.com",
    }
    records: list[dict[str, str]] = []
    for entry in merged.values():
        emails_list = entry.pop("_emails", [])
        website = str(entry.get("website", "")).strip()
        site_domain = ""
        if website:
            try:
                site_domain = urlparse(website).netloc.replace("www.", "").lower()
            except Exception:
                pass
        if site_domain:
            filtered = []
            for em in emails_list:
                if "@" not in em:
                    continue
                em_domain = em.split("@")[1].strip().lower()
                if em_domain in generic_domains:
                    continue
                if _domains_related(em_domain, site_domain):
                    filtered.append(em)
            emails_list = filtered
        emails_list = [
            em for em in emails_list
            if "@" not in em or len(em.split("@")[1].split(".")[0]) >= 3
        ]
        # --- 数据清洗 + 三项齐全门禁 ---
        entry = sanitize_record(entry, emails_list)
        if entry is None:
            continue
        records.append(entry)
    return records


def _load_from_jsonl(site_dir: Path, grouped: dict) -> None:
    """兼容旧 JSONL 模式。"""
    path = site_dir / "final_companies.jsonl"
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            raw = line.strip()
            if not raw:
                continue
            payload = json.loads(raw)
            key = str(payload.get("orgnr") or payload.get("company_name", "")).strip()
            if not key:
                continue
            email = str(payload.get("email", "")).strip().lower()
            homepage = unquote(str(payload.get("homepage", "")).strip())
            if key not in grouped:
                grouped[key] = {
                    "company_name": str(payload.get("company_name", "")).strip(),
                    "representative": str(payload.get("representative", "")).strip(),
                    "website": homepage,
                    "phone": str(payload.get("phone", "")).strip(),
                    "evidence_url": str(payload.get("evidence_url", "")).strip(),
                    "_emails": [],
                }
            if email and email not in grouped[key]["_emails"]:
                grouped[key]["_emails"].append(email)


def _normalize_domain(domain: str) -> str:
    import unicodedata
    d = unicodedata.normalize("NFKD", domain)
    d = "".join(c for c in d if not unicodedata.combining(c))
    return d.replace("-", "").lower()


def _domains_related(d1: str, d2: str) -> bool:
    if d1 == d2 or d1.endswith("." + d2) or d2.endswith("." + d1):
        return True
    n1 = _normalize_domain(d1)
    n2 = _normalize_domain(d2)
    if n1 == n2:
        return True
    p1 = n1.split(".")[0] if "." in n1 else n1
    p2 = n2.split(".")[0] if "." in n2 else n2
    if len(p1) >= 3 and len(p2) >= 3:
        if p1 in p2 or p2 in p1:
            return True
    return False


def _load_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    keys_path = Path(delivery_root) / f"England_day{baseline_day:03d}" / "keys.txt"
    if keys_path.exists():
        return set(keys_path.read_text(encoding="utf-8").strip().splitlines())
    csv_path = Path(delivery_root) / f"England_day{baseline_day:03d}" / "companies.csv"
    if not csv_path.exists():
        return set()
    keys: set[str] = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
        for row in csv.DictReader(fp):
            keys.add(str(row.get("company_name", "")).strip().lower())
    return keys


def _record_key(record: dict[str, str]) -> str:
    return str(record.get("company_name", "")).strip().lower()
