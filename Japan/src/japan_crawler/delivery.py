"""Japan 交付包装。"""

from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from urllib.parse import urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery.engine import validate_day_sequence
_SITE_FILTER_ENV = "JAPAN_DELIVERY_SITES"
_SUMMARY_ONLY_ENV = "JAPAN_DELIVERY_SUMMARY_ONLY"
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_SUSPICIOUS_EMAIL_DOMAINS = {
    "group.calendar.google.com",
    "example.jp",
    "example.co.jp",
    "gmaii.com",
    "gmai.com",
    "gmail.jp",
    "48g9-.bybgnptut",
}
_STRICT_EMAIL_DOMAIN_SITES = {"mynavi", "onecareer"}
_MULTI_LABEL_PUBLIC_SUFFIXES = {
    "co.jp",
    "or.jp",
    "ne.jp",
    "go.jp",
    "ac.jp",
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
}
_PRIORITY_EMAIL_LOCAL_PARTS = {
    "contact",
    "customer",
    "hello",
    "help",
    "hr",
    "info",
    "inquiry",
    "office",
    "privacy",
    "pr",
    "press",
    "recruit",
    "recruiting",
    "sales",
    "service",
    "support",
    "saiyo",
    "soumu",
    "kojinjoho",
}
_REPRESENTATIVE_TITLE_PATTERNS = (
    r"代表取締役社長",
    r"代表執行役社長",
    r"代表執行取締役",
    r"代表取締役",
    r"取締役代表執行役社長",
    r"代表理事",
    r"理事長",
    r"社長",
    r"ceo",
    r"coo",
    r"cfo",
)


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Japan 日交付包，各站点独立落盘。"""
    delivery_root = Path(delivery_root)
    day, _latest = validate_day_sequence(delivery_root, "Japan", day_label)
    delivery_dir = delivery_root / f"Japan_day{day:03d}"
    baseline_day = max(day - 1, 0)
    site_filter = _load_site_filter()
    summary_only = _summary_only_enabled()

    _prepare_japan_delivery_dir(delivery_dir)
    rendered_site_stats, skipped_sites_no_delta = _render_local_site_outputs(
        data_root=Path(data_root),
        delivery_root=delivery_root,
        delivery_dir=delivery_dir,
        baseline_day=baseline_day,
        site_filter=site_filter,
        summary_only=summary_only,
    )
    summary = _build_summary_from_delivery_dir(
        delivery_dir=delivery_dir,
        day=day,
        baseline_day=baseline_day,
        rendered_site_stats=rendered_site_stats,
        skipped_sites_no_delta=skipped_sites_no_delta,
    )
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
) -> tuple[dict[str, dict[str, int]], set[str]]:
    if summary_only:
        print("  Japan 汇总模式：仅重建 summary.json，不重打本站点文件")
        return {}, set()
    selected_dirs = _iter_selected_site_dirs(data_root, site_filter)
    rendered_site_stats: dict[str, dict[str, int]] = {}
    skipped_sites_no_delta: set[str] = set()
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
        rendered_site_stats[site_dir.name] = site_stats
        if site_stats["delta"] <= 0:
            skipped_sites_no_delta.add(site_dir.name)
            print(
                "  {site}: DB 总计 {raw} → 当前合格 {qualified} 家公司 → 当日新增 {delta} 家公司（无新增，已跳过交付文件）".format(
                    site=site_dir.name,
                    raw=site_stats["raw_count"],
                    qualified=site_stats["qualified_current"],
                    delta=site_stats["delta"],
                )
            )
            continue
        print(
            "  {site}: DB 总计 {raw} → 当前合格 {qualified} 家公司 → 当日新增 {delta} 家公司".format(
                site=site_dir.name,
                raw=site_stats["raw_count"],
                qualified=site_stats["qualified_current"],
                delta=site_stats["delta"],
            )
        )
    return rendered_site_stats, skipped_sites_no_delta


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
    prepared = _prepare_delivery_records(site_name, records)
    qualified = [record for record in prepared if _is_delivery_qualified(record)]
    deduped = _dedupe_delivery_records(qualified)
    baseline_keys = _load_site_baseline_keys(
        delivery_root=delivery_root,
        site_name=site_name,
        baseline_day=baseline_day,
    )
    delta_records = [record for record in deduped if _record_key(record) not in baseline_keys]
    current_keys = sorted(baseline_keys | {_record_key(record) for record in deduped})
    _write_site_delivery_files(
        delivery_dir=delivery_dir,
        site_name=site_name,
        delta_records=delta_records,
        current_keys=current_keys,
    )
    return {
        "raw_count": raw_count,
        "qualified_current": len(deduped),
        "delta": len(delta_records),
    }


def _write_site_delivery_files(
    *,
    delivery_dir: Path,
    site_name: str,
    delta_records: list[dict[str, str]],
    current_keys: list[str],
) -> None:
    csv_path = delivery_dir / f"{site_name}.csv"
    keys_path = delivery_dir / f"{site_name}.keys.txt"
    if delta_records:
        _write_site_csv(csv_path, delta_records)
        keys_path.write_text("\n".join(current_keys), encoding="utf-8")
        return
    _clear_site_delivery_files(csv_path, keys_path)


def _clear_site_delivery_files(csv_path: Path, keys_path: Path) -> None:
    if csv_path.exists():
        csv_path.unlink()
    if keys_path.exists():
        keys_path.unlink()


def _build_summary_from_delivery_dir(
    *,
    delivery_dir: Path,
    day: int,
    baseline_day: int,
    rendered_site_stats: dict[str, dict[str, int]],
    skipped_sites_no_delta: set[str],
) -> dict[str, object]:
    site_stats = {
        site_name: {
            "qualified_current": int(stats["qualified_current"]),
            "delta": int(stats["delta"]),
        }
        for site_name, stats in rendered_site_stats.items()
    }
    for site_name in _discover_delivery_sites(delivery_dir):
        if site_name in site_stats:
            continue
        current_count = _count_key_lines(delivery_dir / f"{site_name}.keys.txt")
        delta_count = _count_csv_rows(delivery_dir / f"{site_name}.csv")
        if current_count <= 0 and delta_count <= 0:
            continue
        site_stats[site_name] = {
            "qualified_current": current_count,
            "delta": delta_count,
        }
    total_current_companies = sum(stats["qualified_current"] for stats in site_stats.values())
    total_delta_companies = sum(stats["delta"] for stats in site_stats.values())
    return {
        "country": "Japan",
        "day": day,
        "baseline_day": baseline_day,
        "delta_companies": total_delta_companies,
        "total_current_companies": total_current_companies,
        "sites": site_stats,
        "skipped_sites_no_delta": sorted(skipped_sites_no_delta),
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


def _prepare_delivery_records(site_name: str, records: list[dict[str, str]]) -> list[dict[str, str]]:
    prepared: list[dict[str, str]] = []
    for record in records:
        copied = dict(record)
        copied["emails"] = _normalize_delivery_emails(
            site_name,
            str(record.get("website", "") or ""),
            str(record.get("emails", "") or ""),
        )
        prepared.append(copied)
    return prepared


def _normalize_delivery_emails(site_name: str, website: str, emails_text: str) -> str:
    tokens = _split_emails(emails_text)
    if site_name != "xlsximport":
        tokens = [email for email in tokens if _is_delivery_email_allowed(email)]
    if site_name in _STRICT_EMAIL_DOMAIN_SITES:
        tokens = [email for email in tokens if _email_matches_website_domain(website, email)]
        tokens = _prioritize_emails(tokens)[:10]
    return "; ".join(tokens)


def _split_emails(emails_text: str) -> list[str]:
    result: list[str] = []
    for raw in str(emails_text or "").replace(",", ";").split(";"):
        value = raw.strip().lower()
        if value and value not in result:
            result.append(value)
    return result


def _is_delivery_email_allowed(email: str) -> bool:
    if not _EMAIL_RE.fullmatch(email):
        return False
    domain = email.split("@", 1)[1]
    if domain in _SUSPICIOUS_EMAIL_DOMAINS:
        return False
    if domain.startswith("example."):
        return False
    return True


def _is_delivery_qualified(record: dict[str, str]) -> bool:
    return bool(
        record.get("company_name", "").strip()
        and record.get("representative", "").strip()
        and record.get("representative", "").strip() != "-"
        and record.get("emails", "").strip()
    )


def _dedupe_delivery_records(records: list[dict[str, str]]) -> list[dict[str, str]]:
    merged_by_key: dict[str, dict[str, str]] = {}
    order: list[str] = []
    for record in records:
        key = _delivery_merge_key(record)
        existing = merged_by_key.get(key)
        if existing is None:
            merged_by_key[key] = dict(record)
            order.append(key)
            continue
        merged_by_key[key] = _merge_delivery_record(existing, record)
    return [merged_by_key[key] for key in order]


def _merge_delivery_record(existing: dict[str, str], incoming: dict[str, str]) -> dict[str, str]:
    merged = dict(existing)
    merged["emails"] = _merge_email_text(existing.get("emails", ""), incoming.get("emails", ""))
    for field in ["phone", "industry", "founded_year", "capital", "detail_url", "source_job_url"]:
        if not str(merged.get(field, "") or "").strip() and str(incoming.get(field, "") or "").strip():
            merged[field] = incoming[field]
    return merged


def _merge_email_text(left: str, right: str) -> str:
    merged: list[str] = []
    for value in [*_split_emails(left), *_split_emails(right)]:
        if value not in merged:
            merged.append(value)
    return "; ".join(merged)


def _delivery_merge_key(record: dict[str, str]) -> str:
    company = _normalize_merge_text(record.get("company_name", ""))
    representative = _normalize_representative_for_key(record.get("representative", ""))
    website_domain = _registrable_domain(record.get("website", ""))
    if company and representative and website_domain:
        return f"{company}|{representative}|{website_domain}"
    address = _normalize_merge_text(record.get("address", ""))
    if company and representative and address:
        return f"{company}|{representative}|{address}"
    return _record_key(record)


def _record_key(record: dict[str, str]) -> str:
    parts = [
        str(record.get("company_name", "") or "").strip().lower(),
        str(record.get("representative", "") or "").strip().lower(),
        str(record.get("website", "") or "").strip().lower(),
        str(record.get("address", "") or "").strip().lower(),
    ]
    return "|".join(parts)


def _normalize_merge_text(raw: str) -> str:
    return re.sub(r"\s+", "", str(raw or "").strip().lower())


def _normalize_representative_for_key(raw: str) -> str:
    text = _normalize_merge_text(raw)
    for pattern in _REPRESENTATIVE_TITLE_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    return text


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


def _registrable_domain(url: str) -> str:
    target = str(url or "").strip()
    if not target:
        return ""
    if "://" not in target:
        target = f"https://{target}"
    parsed = urlparse(target)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return host
    suffix2 = ".".join(labels[-2:])
    if suffix2 in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix2


def _domain_label(domain: str) -> str:
    labels = [label for label in str(domain or "").split(".") if label]
    if len(labels) < 2:
        return str(domain or "")
    suffix2 = ".".join(labels[-2:])
    if suffix2 in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return labels[-3]
    return labels[-2]


def _email_matches_website_domain(website: str, email: str) -> bool:
    website_domain = _registrable_domain(website)
    if not website_domain:
        return True
    value = str(email or "").strip().lower()
    if "@" not in value:
        return False
    email_domain = _registrable_domain(value.split("@", 1)[1])
    if not email_domain:
        return False
    if website_domain == email_domain:
        return True
    website_label = _domain_label(website_domain)
    email_label = _domain_label(email_domain)
    if len(website_label) >= 4 and website_label in email_label:
        return True
    if len(email_label) >= 4 and email_label in website_label:
        return True
    return False


def _prioritize_emails(emails: list[str]) -> list[str]:
    unique = _split_emails(";".join(emails))
    return sorted(unique, key=lambda item: (-_email_priority(item), unique.index(item)))


def _email_priority(email: str) -> int:
    local = str(email or "").split("@", 1)[0].strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "", local)
    if local in _PRIORITY_EMAIL_LOCAL_PARTS or normalized in _PRIORITY_EMAIL_LOCAL_PARTS:
        return 100
    if any(token in normalized for token in _PRIORITY_EMAIL_LOCAL_PARTS):
        return 60
    if re.fullmatch(r"[a-z]+", normalized):
        return 20
    return 10
