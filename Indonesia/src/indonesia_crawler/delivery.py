"""按日交付打包工具。"""

from __future__ import annotations

import csv
import json
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

DAY_PATTERN = re.compile(r"^day(\d+)$", flags=re.I)


def parse_day_label(raw: str) -> int:
    """解析 dayN 标签为数字。"""
    matched = DAY_PATTERN.fullmatch(raw.strip())
    if matched is None:
        raise ValueError("参数必须是 dayN，例如 day1。")
    value = int(matched.group(1))
    if value <= 0:
        raise ValueError("dayN 中的 N 必须大于 0。")
    return value


def _build_key(company_name: str, email: str) -> str:
    """构建去重 key（公司名+邮箱）。"""
    name_norm = re.sub(r"[^a-z0-9]+", "", company_name.lower())
    return f"{name_norm}|{email.lower()}"


def _list_existing_days(delivery_root: Path) -> list[int]:
    """列出已交付的日期。"""
    if not delivery_root.exists():
        return []
    days: list[int] = []
    for item in delivery_root.iterdir():
        if not item.is_dir():
            continue
        matched = re.fullmatch(r"Indonesia_day(\d{3})", item.name)
        if matched:
            days.append(int(matched.group(1)))
    return sorted(days)


def _read_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    """读取基线（前一天）已交付的 key 集合。"""
    if baseline_day <= 0:
        return set()
    baseline_dir = delivery_root / f"Indonesia_day{baseline_day:03d}"
    keys_path = baseline_dir / "keys.txt"
    if keys_path.exists():
        return {
            line.strip()
            for line in keys_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    return set()


def _load_all_records(data_root: Path) -> list[dict]:
    """从各站点目录加载所有记录。优先读实时文件，支持边跑边交付。"""
    records: list[dict] = []
    for site_dir in sorted(data_root.iterdir()):
        if not site_dir.is_dir() or site_dir.name == "delivery":
            continue

        # 与 SouthKorea 对齐：优先使用已补邮箱文件，再回退到最终/原始文件。
        data_file = site_dir / "companies_with_emails.jsonl"
        if not data_file.exists():
            data_file = site_dir / "final_companies.jsonl"
        if not data_file.exists():
            data_file = site_dir / "companies.jsonl"
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
    raise ValueError(f"交付目录被占用，请关闭文件后重试: {day_dir}")


def build_delivery_bundle(
    data_root: Path,
    delivery_root: Path,
    day_label: str,
) -> dict:
    """
    构建日交付包。

    参数:
        data_root: 数据根目录 (output/)
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

    all_records = _load_all_records(data_root)

    # 过滤：只保留有公司名+法人+邮箱的完整记录
    qualified: list[dict] = []
    skipped = 0
    for record in all_records:
        company_name = str(record.get("company_name", "")).strip()
        ceo = str(record.get("ceo", "")).strip()
        emails = record.get("emails", [])
        has_emails = bool(emails and any(str(e).strip() for e in emails)) if isinstance(emails, list) else bool(str(emails).strip())

        if company_name and ceo and has_emails:
            qualified.append(record)
        else:
            skipped += 1

    if skipped:
        print(f"跳过 {skipped} 条不完整记录（缺少公司名/法人/邮箱）")

    # 构建 key 并去重
    keyed_records: list[dict] = []
    seen_keys: set[str] = set()
    for record in qualified:
        emails = record.get("emails", [])
        email_str = emails[0] if isinstance(emails, list) and emails else str(emails)
        key = _build_key(record.get("company_name", ""), email_str)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        record["_key"] = key
        keyed_records.append(record)

    # 筛选增量（相对基线）
    delta_records = [r for r in keyed_records if r["_key"] not in baseline_keys]

    # 构建交付目录
    day_dir = delivery_root / f"Indonesia_day{target_day:03d}"
    _remove_dir_safe(day_dir)
    day_dir.mkdir(parents=True, exist_ok=True)

    # 写 CSV（交付只需 3 个字段：公司名、法人、邮箱）
    csv_path = day_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=["company_name", "ceo", "emails"],
        )
        writer.writeheader()
        for record in delta_records:
            emails = record.get("emails", [])
            emails_str = "; ".join(emails) if isinstance(emails, list) else str(emails)
            writer.writerow({
                "company_name": record.get("company_name", ""),
                "ceo": record.get("ceo", ""),
                "emails": emails_str,
            })

    # 写 keys.txt
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
