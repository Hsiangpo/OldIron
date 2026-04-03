"""共享交付引擎。"""

from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from datetime import timezone
from pathlib import Path
from urllib.parse import urlparse

from .spec import DeliverySpec
from .trash import move_path_to_recycle_bin


DAY_PATTERN = re.compile(r"^day(\d+)$", flags=re.I)


def parse_day_label(raw: str) -> int:
    """解析 dayN 标签为数字。"""
    matched = DAY_PATTERN.fullmatch(str(raw or "").strip())
    if matched is None:
        raise ValueError("参数必须是 dayN，例如 day1。")
    value = int(matched.group(1))
    if value <= 0:
        raise ValueError("dayN 中的 N 必须大于 0。")
    return value


def validate_day_sequence(delivery_root: Path, country_name: str, day_label: str) -> tuple[int, int]:
    """校验目标 day 是否符合交付顺序要求。"""
    target_day = parse_day_label(day_label)
    pattern = re.compile(rf"{re.escape(str(country_name or '').strip())}_day(\d{{3}})$")
    existing_days: list[int] = []
    if delivery_root.exists():
        for item in delivery_root.iterdir():
            if not item.is_dir():
                continue
            matched = pattern.fullmatch(item.name)
            if matched:
                existing_days.append(int(matched.group(1)))
    latest = max(existing_days, default=0)
    if latest == 0 and target_day != 1:
        raise ValueError("尚未有交付记录，首个交付只能执行 day1。")
    if target_day < latest:
        raise ValueError(f"第{target_day}天已交付，当前最新是第{latest}天。")
    if target_day > latest + 1:
        raise ValueError(f"只能执行 day{latest}（重跑）或 day{latest + 1}（新一天）。")
    return target_day, latest


def extract_domain(url: str) -> str:
    """从 URL 提取域名。"""
    if not url:
        return ""
    target = str(url).strip()
    if "://" not in target:
        target = f"https://{target}"
    parsed = urlparse(target)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def _build_key(company_name: str, domain: str) -> str:
    """构建去重 key。"""
    name_norm = re.sub(r"[^a-z0-9]+", "", str(company_name or "").lower())
    if name_norm:
        return f"name|{name_norm}"
    return f"domain|{str(domain or '').strip().lower()}"


def _build_domain_key(domain: str) -> str:
    value = str(domain or "").strip().lower()
    return f"domain|{value}" if value else ""


def _record_score(record: dict[str, object]) -> tuple[int, int, int, int]:
    emails = record.get("emails", [])
    email_count = len(emails) if isinstance(emails, list) else 0
    return (
        1 if str(record.get("homepage", "")).strip() else 0,
        email_count,
        1 if str(record.get("ceo", "")).strip() else 0,
        1 if str(record.get("phone", "")).strip() else 0,
    )


def _delivery_dir_name(country_name: str, day: int) -> str:
    return f"{country_name}_day{int(day):03d}"


def _list_existing_days(delivery_root: Path, spec: DeliverySpec) -> list[int]:
    """列出已交付的日期。"""
    if not delivery_root.exists():
        return []
    pattern = re.compile(rf"{re.escape(spec.country_name)}_day(\d{{3}})$")
    days: list[int] = []
    for item in delivery_root.iterdir():
        if not item.is_dir():
            continue
        matched = pattern.fullmatch(item.name)
        if matched:
            days.append(int(matched.group(1)))
    return sorted(days)


def _read_baseline_keys(
    delivery_root: Path,
    spec: DeliverySpec,
    baseline_day: int,
) -> set[str]:
    """读取基线已交付的 key 集合。"""
    if baseline_day <= 0:
        return set()
    baseline_dir = delivery_root / _delivery_dir_name(spec.country_name, baseline_day)
    keys_path = baseline_dir / "keys.txt"
    if not keys_path.exists():
        return set()
    keys: set[str] = set()
    for raw_line in keys_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        keys.add(line)
        if line.startswith("domain|") or line.startswith("name|") or "|" not in line:
            continue
        legacy_name, legacy_domain = line.rsplit("|", 1)
        legacy_name = legacy_name.strip()
        legacy_domain = legacy_domain.strip().lower()
        if legacy_domain:
            keys.add(f"domain|{legacy_domain}")
        elif legacy_name:
            keys.add(f"name|{legacy_name}")
    return keys


def _pick_candidate_file(site_dir: Path, spec: DeliverySpec) -> Path | None:
    for filename in spec.candidate_filenames:
        data_file = site_dir / filename
        if data_file.exists():
            return data_file
    return None


def _load_all_records(data_root: Path, spec: DeliverySpec | None = None) -> list[dict[str, object]]:
    """从各站点加载所有记录。优先读 final，再回退到实时文件。"""
    current_spec = spec or DeliverySpec(country_name="Generic")
    records: list[dict[str, object]] = []
    if not data_root.exists():
        return records
    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir() or site_dir.name in current_spec.ignored_site_dirs:
            continue
        data_file = _pick_candidate_file(site_dir, current_spec)
        if data_file is None:
            continue
        with data_file.open("r", encoding="utf-8") as fp:
            for line in fp:
                raw = line.strip()
                if not raw:
                    continue
                record = json.loads(raw)
                record["_source"] = site_dir.name
                records.append(record)
    return records


def _load_historical_baseline_records(
    delivery_root: Path,
    spec: DeliverySpec,
    baseline_day: int,
    baseline_keys: set[str],
) -> list[dict[str, object]]:
    """从历史交付增量恢复截至基线日的全量当前集。"""
    if baseline_day <= 0 or not baseline_keys:
        return []
    best_by_key: dict[str, dict[str, object]] = {}
    order: list[str] = []
    for day in range(1, baseline_day + 1):
        csv_path = delivery_root / _delivery_dir_name(spec.country_name, day) / "companies.csv"
        if not csv_path.exists():
            continue
        with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                record = _record_from_csv_row(row)
                key = _build_key(str(record["company_name"]), str(record["domain"]))
                domain_key = _build_domain_key(str(record["domain"]))
                if key not in baseline_keys and (not domain_key or domain_key not in baseline_keys):
                    continue
                record["_source"] = "zz_delivery_history"
                record["_history_baseline"] = True
                if key not in best_by_key:
                    best_by_key[key] = record
                    order.append(key)
                    continue
                if _record_score(record) > _record_score(best_by_key[key]):
                    best_by_key[key] = record
    return [best_by_key[key] for key in order]


def _record_from_csv_row(row: dict[str, str]) -> dict[str, object]:
    company_name = str(row.get("company_name", "")).strip()
    ceo = str(row.get("ceo", "")).strip()
    homepage = str(row.get("homepage", "")).strip()
    domain = str(row.get("domain", "")).strip() or extract_domain(homepage)
    phone = str(row.get("phone", "")).strip()
    emails_text = str(row.get("emails", "")).strip()
    emails = [item.strip() for item in emails_text.split(";") if item.strip()]
    return {
        "company_name": company_name,
        "ceo": ceo,
        "homepage": homepage,
        "domain": domain,
        "phone": phone,
        "emails": emails,
    }


def prepare_delivery_dir(day_dir: Path) -> None:
    """准备交付目录，重跑同一天时先送回收站。"""
    if day_dir.exists():
        move_path_to_recycle_bin(day_dir)
    day_dir.mkdir(parents=True, exist_ok=True)


def _has_emails(record: dict[str, object]) -> bool:
    emails = record.get("emails", [])
    if isinstance(emails, list):
        return any(str(item or "").strip() for item in emails)
    return bool(str(emails or "").strip())


def _qualified_records(
    all_records: list[dict[str, object]],
    spec: DeliverySpec,
) -> tuple[list[dict[str, object]], int, int]:
    qualified: list[dict[str, object]] = []
    skipped = 0
    suspicious = 0
    for record in all_records:
        company_name = str(record.get("company_name", "")).strip()
        ceo = str(record.get("ceo", "")).strip()
        is_history_baseline = bool(record.get("_history_baseline"))
        if not (company_name and ceo and _has_emails(record)):
            skipped += 1
            continue
        if not is_history_baseline and spec.suspicious_filter(record):
            suspicious += 1
            continue
        qualified.append(record)
    return qualified, skipped, suspicious


def _deduplicate_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    keyed_records: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for record in records:
        domain = extract_domain(str(record.get("homepage", "")))
        key = _build_key(str(record.get("company_name", "")), domain)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        record["_key"] = key
        keyed_records.append(record)
    return keyed_records


def _delta_records(
    keyed_records: list[dict[str, object]],
    baseline_keys: set[str],
) -> list[dict[str, object]]:
    delta: list[dict[str, object]] = []
    for record in keyed_records:
        domain_key = _build_domain_key(extract_domain(str(record.get("homepage", ""))))
        if record["_key"] in baseline_keys or (domain_key and domain_key in baseline_keys):
            continue
        delta.append(record)
    return delta


def _write_bundle_files(
    day_dir: Path,
    keyed_records: list[dict[str, object]],
    delta_records: list[dict[str, object]],
    summary: dict[str, object],
) -> None:
    csv_path = day_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=["company_name", "ceo", "homepage", "domain", "phone", "emails"],
        )
        writer.writeheader()
        for record in delta_records:
            emails = record.get("emails", [])
            emails_str = "; ".join(emails) if isinstance(emails, list) else str(emails or "")
            writer.writerow(
                {
                    "company_name": str(record.get("company_name", "")),
                    "ceo": str(record.get("ceo", "")),
                    "homepage": str(record.get("homepage", "")),
                    "domain": extract_domain(str(record.get("homepage", ""))),
                    "phone": str(record.get("phone", "")),
                    "emails": emails_str,
                }
            )
    (day_dir / "keys.txt").write_text(
        "\n".join(str(record["_key"]) for record in keyed_records),
        encoding="utf-8",
    )
    (day_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_delivery_bundle(
    *,
    data_root: Path,
    delivery_root: Path,
    day_label: str,
    spec: DeliverySpec,
) -> dict[str, object]:
    """构建国家级日交付包。"""
    target_day = parse_day_label(day_label)
    existing = _list_existing_days(delivery_root, spec)
    latest = existing[-1] if existing else 0
    if latest == 0 and target_day != 1:
        raise ValueError("尚未有交付记录，首个交付只能执行 day1。")
    if target_day < latest:
        raise ValueError(f"第{target_day}天已交付，当前最新是第{latest}天。")
    if target_day > latest + 1:
        raise ValueError(f"只能执行 day{latest}（重跑）或 day{latest + 1}（新一天）。")

    baseline_day = target_day - 1
    baseline_keys = _read_baseline_keys(delivery_root, spec, baseline_day)
    all_records = _load_all_records(data_root, spec)
    all_records.extend(
        _load_historical_baseline_records(
            delivery_root=delivery_root,
            spec=spec,
            baseline_day=baseline_day,
            baseline_keys=baseline_keys,
        )
    )
    qualified, skipped, suspicious = _qualified_records(all_records, spec)
    if skipped:
        print(f"跳过 {skipped} 条不完整记录（缺少公司名/法人/邮箱）")
    if suspicious:
        print(f"跳过 {suspicious} 条疑似海外错配记录（官网/电话异常）")

    keyed_records = _deduplicate_records(qualified)
    delta_records = _delta_records(keyed_records, baseline_keys)

    day_dir = delivery_root / _delivery_dir_name(spec.country_name, target_day)
    prepare_delivery_dir(day_dir)

    summary = {
        "day": target_day,
        "baseline_day": baseline_day,
        "total_current_companies": len(keyed_records),
        "delta_companies": len(delta_records),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    _write_bundle_files(day_dir, keyed_records, delta_records, summary)
    return summary
