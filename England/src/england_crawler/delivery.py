"""按日交付打包工具。"""

from __future__ import annotations

import csv
import json
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

DAY_PATTERN = re.compile(r"^day(\d+)$", flags=re.I)
FOREIGN_TLDS = (".hk", ".com.hk", ".in", ".my", ".cn", ".sg")
FOREIGN_URL_MARKERS = ("hong-kong", "hongkong", "/locations/cn/", "/hk/", ".hk/")


def parse_day_label(raw: str) -> int:
    """解析 dayN 标签为数字。"""
    matched = DAY_PATTERN.fullmatch(raw.strip())
    if matched is None:
        raise ValueError("参数必须是 dayN，例如 day1。")
    value = int(matched.group(1))
    if value <= 0:
        raise ValueError("dayN 中的 N 必须大于 0。")
    return value


def _extract_domain(url: str) -> str:
    """从 URL 提取域名。"""
    if not url:
        return ""
    if "://" not in url:
        url = f"https://{url}"
    parsed = urlparse(url)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def _build_key(company_name: str, domain: str) -> str:
    """构建去重 key。"""
    name_norm = re.sub(r"[^a-z0-9]+", "", company_name.lower())
    if name_norm:
        return f"name|{name_norm}"
    return f"domain|{domain}"


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


def _looks_suspicious_uk_record(homepage: str, phone: str) -> bool:
    domain = _extract_domain(homepage)
    lower_homepage = str(homepage or "").strip().lower()
    if domain and any(domain.endswith(suffix) for suffix in FOREIGN_TLDS):
        return True
    if any(marker in lower_homepage for marker in FOREIGN_URL_MARKERS):
        return True
    return False


def _list_existing_days(delivery_root: Path) -> list[int]:
    """列出已交付的日期。"""
    if not delivery_root.exists():
        return []
    days: list[int] = []
    for item in delivery_root.iterdir():
        if not item.is_dir():
            continue
        matched = re.fullmatch(r"England_day(\d{3})", item.name)
        if matched:
            days.append(int(matched.group(1)))
    return sorted(days)


def _read_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    """读取基线（前一天）已交付的 key 集合。"""
    if baseline_day <= 0:
        return set()
    baseline_dir = delivery_root / f"England_day{baseline_day:03d}"
    keys_path = baseline_dir / "keys.txt"
    if keys_path.exists():
        keys: set[str] = set()
        for raw_line in keys_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            keys.add(line)
            # 兼容历史 key 格式：name_norm|domain
            if line.startswith("domain|") or line.startswith("name|"):
                continue
            if "|" not in line:
                continue
            legacy_name, legacy_domain = line.rsplit("|", 1)
            legacy_name = legacy_name.strip()
            legacy_domain = legacy_domain.strip().lower()
            if legacy_domain:
                keys.add(f"domain|{legacy_domain}")
            elif legacy_name:
                keys.add(f"name|{legacy_name}")
        return keys
    return set()


def _load_all_records(data_root: Path) -> list[dict]:
    """从各站点加载所有记录。优先读实时文件以支持边跑边交付。"""
    records: list[dict] = []

    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir():
            continue
        if site_dir.name == "delivery":
            continue

        # 优先读站点最终产物；若站点还在跑且未生成 final，再回退到实时文件。
        data_file = site_dir / "final_companies.jsonl"
        if not data_file.exists():
            data_file = site_dir / "companies_with_emails.jsonl"
        if not data_file.exists():
            continue

        with data_file.open("r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if line:
                    record = json.loads(line)
                    record["_source"] = site_dir.name
                    records.append(record)

    return records


def _load_historical_baseline_records(
    delivery_root: Path,
    baseline_day: int,
    baseline_keys: set[str],
) -> list[dict[str, object]]:
    """从历史交付增量恢复截至基线日的全量当前集。"""
    if baseline_day <= 0 or not baseline_keys:
        return []

    best_by_key: dict[str, dict[str, object]] = {}
    order: list[str] = []
    for day in range(1, baseline_day + 1):
        csv_path = delivery_root / f"England_day{day:03d}" / "companies.csv"
        if not csv_path.exists():
            continue
        with csv_path.open("r", encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                company_name = str(row.get("company_name", "")).strip()
                ceo = str(row.get("ceo", "")).strip()
                homepage = str(row.get("homepage", "")).strip()
                domain = str(row.get("domain", "")).strip() or _extract_domain(homepage)
                phone = str(row.get("phone", "")).strip()
                emails_text = str(row.get("emails", "")).strip()
                emails = [item.strip() for item in emails_text.split(";") if item.strip()]
                key = _build_key(company_name, domain)
                domain_key = _build_domain_key(domain)
                if key not in baseline_keys and (not domain_key or domain_key not in baseline_keys):
                    continue
                record = {
                    "company_name": company_name,
                    "ceo": ceo,
                    "homepage": homepage,
                    "domain": domain,
                    "phone": phone,
                    "emails": emails,
                    "_source": "zz_delivery_history",
                    "_history_baseline": True,
                }
                if key not in best_by_key:
                    best_by_key[key] = record
                    order.append(key)
                    continue
                if _record_score(record) > _record_score(best_by_key[key]):
                    best_by_key[key] = record
    return [best_by_key[key] for key in order]


def _remove_dir_safe(day_dir: Path) -> None:
    """安全删除交付目录（Windows 兼容）。"""
    if not day_dir.exists():
        return
    for attempt in range(8):
        try:
            shutil.rmtree(day_dir)
            return
        except (PermissionError, OSError):
            time.sleep(0.6)
    _clear_dir_contents_safe(day_dir)


def _clear_dir_contents_safe(day_dir: Path) -> None:
    """目录被占用时，尽量复用目录并清空其内容。"""
    if not day_dir.exists():
        return
    for child in sorted(day_dir.iterdir()):
        if child.is_dir():
            shutil.rmtree(child)
            continue
        child.unlink()


def build_delivery_bundle(
    data_root: Path,
    delivery_root: Path,
    day_label: str,
) -> dict:
    """
    构建日交付包。

    参数:
        data_root: 数据根目录 (output/)，下面有各站点子目录
        delivery_root: 交付根目录 (output/delivery/)
        day_label: 如 "day1"

    返回:
        交付摘要 dict
    """
    target_day = parse_day_label(day_label)
    existing = _list_existing_days(delivery_root)
    latest = existing[-1] if existing else 0

    if latest == 0 and target_day != 1:
        raise ValueError("尚未有交付记录，首个交付只能执行 day1。")
    if target_day < latest:
        raise ValueError(f"第{target_day}天已交付，当前最新是第{latest}天。")
    if target_day > latest + 1:
        raise ValueError(f"只能执行 day{latest}（重跑）或 day{latest + 1}（新一天）。")

    baseline_day = target_day - 1
    baseline_keys = _read_baseline_keys(delivery_root, baseline_day)

    # 加载所有站点数据
    all_records = _load_all_records(data_root)
    all_records.extend(_load_historical_baseline_records(delivery_root, baseline_day, baseline_keys))

    # 过滤：只保留同时有 公司名+法人+邮箱 的完整记录
    qualified: list[dict] = []
    skipped = 0
    suspicious = 0
    for record in all_records:
        company_name = str(record.get("company_name", "")).strip()
        ceo = str(record.get("ceo", "")).strip()
        emails = record.get("emails", [])
        has_emails = bool(emails and any(e.strip() for e in emails)) if isinstance(emails, list) else bool(str(emails).strip())
        homepage = str(record.get("homepage", "")).strip()
        phone = str(record.get("phone", "")).strip()
        is_history_baseline = bool(record.get("_history_baseline"))

        if company_name and ceo and has_emails:
            if not is_history_baseline and _looks_suspicious_uk_record(homepage, phone):
                suspicious += 1
                continue
            qualified.append(record)
        else:
            skipped += 1

    if skipped:
        print(f"跳过 {skipped} 条不完整记录（缺少公司名/法人/邮箱）")
    if suspicious:
        print(f"跳过 {suspicious} 条疑似海外错配记录（官网/电话异常）")

    # 构建 key 并去重
    keyed_records: list[dict] = []
    seen_keys: set[str] = set()
    for record in qualified:
        domain = _extract_domain(record.get("homepage", ""))
        key = _build_key(record.get("company_name", ""), domain)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        record["_key"] = key
        keyed_records.append(record)

    # 筛选增量（相对基线）
    delta_records = []
    for record in keyed_records:
        domain_key = _build_domain_key(_extract_domain(record.get("homepage", "")))
        if record["_key"] in baseline_keys or (domain_key and domain_key in baseline_keys):
            continue
        delta_records.append(record)

    # 构建交付目录
    day_dir = delivery_root / f"England_day{target_day:03d}"
    _remove_dir_safe(day_dir)
    day_dir.mkdir(parents=True, exist_ok=True)

    # 写 CSV
    csv_path = day_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=["company_name", "ceo", "homepage", "domain", "phone", "emails"],
        )
        writer.writeheader()
        for record in delta_records:
            emails = record.get("emails", [])
            if isinstance(emails, list):
                emails_str = "; ".join(emails)
            else:
                emails_str = str(emails)
            writer.writerow({
                "company_name": record.get("company_name", ""),
                "ceo": record.get("ceo", ""),
                "homepage": record.get("homepage", ""),
                "domain": _extract_domain(record.get("homepage", "")),
                "phone": record.get("phone", ""),
                "emails": emails_str,
            })

    # 写 keys.txt（用于下次增量对比）
    (day_dir / "keys.txt").write_text(
        "\n".join(r["_key"] for r in keyed_records),
        encoding="utf-8",
    )

    # 写摘要
    summary = {
        "day": target_day,
        "baseline_day": baseline_day,
        "total_current_companies": len(keyed_records),
        "delta_companies": len(delta_records),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    (day_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return summary
