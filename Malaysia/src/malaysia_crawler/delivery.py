"""日交付打包工具。"""

from __future__ import annotations

import csv
import json
import re
import shutil
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path

from malaysia_crawler.common.io_utils import ensure_dir

DAY_PATTERN = re.compile(r"^day(\d+)$", flags=re.I)


@dataclass(slots=True)
class DeliveryPlan:
    target_day: int
    baseline_day: int
    latest_day: int


def parse_day_label(raw: str) -> int:
    matched = DAY_PATTERN.fullmatch(raw.strip())
    if matched is None:
        raise ValueError("参数必须是 dayN，例如 day1。")
    value = int(matched.group(1))
    if value <= 0:
        raise ValueError("dayN 中的 N 必须大于 0。")
    return value


def _normalize_company_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _build_fallback_key(company_name: str, domain: str) -> str:
    return f"{_normalize_company_name(company_name)}|{domain.strip().lower()}"


def _list_existing_days(delivery_root: Path) -> list[int]:
    if not delivery_root.exists():
        return []
    days: list[int] = []
    for item in delivery_root.iterdir():
        if not item.is_dir():
            continue
        matched = re.fullmatch(r"Malaysia_day(\d{3})", item.name)
        if matched is None:
            continue
        days.append(int(matched.group(1)))
    return sorted(days)


def resolve_delivery_plan(delivery_root: Path, target_day: int) -> DeliveryPlan:
    existing = _list_existing_days(delivery_root)
    latest = existing[-1] if existing else 0
    if latest == 0:
        if target_day != 1:
            raise ValueError("尚未有交付记录，首个交付只能执行 day1。")
        return DeliveryPlan(target_day=1, baseline_day=0, latest_day=0)
    if target_day < latest:
        raise ValueError(f"第{target_day}天已经交付，当前最新是第{latest}天。")
    if target_day > latest + 1:
        raise ValueError(f"只能执行 day{latest}（重跑）或 day{latest + 1}（新一天）。")
    return DeliveryPlan(target_day=target_day, baseline_day=target_day - 1, latest_day=latest)


def _read_baseline_keys(delivery_root: Path, baseline_day: int) -> set[str]:
    if baseline_day <= 0:
        return set()
    baseline_dir = delivery_root / f"Malaysia_day{baseline_day:03d}"
    keys_path = baseline_dir / "keys.txt"
    if keys_path.exists():
        return {
            line.strip()
            for line in keys_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    csv_path = baseline_dir / "companies.csv"
    if not csv_path.exists():
        return set()
    keys: set[str] = set()
    with csv_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            key = str(row.get("normalized_name", "")).strip()
            if not key:
                key = _build_fallback_key(
                    str(row.get("company_name", "")),
                    str(row.get("domain", "")),
                )
            if key:
                keys.add(key)
    return keys


def _load_final_records(db_path: Path) -> list[dict[str, str]]:
    if not db_path.exists():
        raise ValueError(f"未找到主流程数据库：{db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(final_companies)").fetchall()
        }
        has_phone = "phone" in columns
        phone_expr = "phone" if has_phone else "'' AS phone"
        rows = conn.execute(
            f"""
            SELECT normalized_name, company_name, domain, contact_eamils, company_manager, {phone_expr}
            FROM final_companies
            ORDER BY normalized_name ASC
            """
        ).fetchall()
    finally:
        conn.close()
    result: list[dict[str, str]] = []
    for row in rows:
        result.append(
            {
                "_key": str(row["normalized_name"]),
                "company_name": str(row["company_name"]),
                "domain": str(row["domain"]),
                "contact_eamils": str(row["contact_eamils"]),
                "company_manager": str(row["company_manager"]),
                "phone": str(row["phone"]),
            }
        )
    return result


def _write_delivery_files(day_dir: Path, rows: list[dict[str, str]], summary: dict[str, int | str]) -> None:
    ensure_dir(day_dir)
    csv_path = day_dir / "companies.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "company_name",
                "domain",
                "company_manager",
                "contact_eamils",
                "phone",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "company_name": row["company_name"],
                    "domain": row["domain"],
                    "company_manager": row["company_manager"],
                    "contact_eamils": row["contact_eamils"],
                    "phone": row["phone"],
                }
            )
    (day_dir / "keys.txt").write_text(
        "\n".join(str(row["_key"]) for row in rows),
        encoding="utf-8",
    )
    (day_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _remove_delivery_dir_with_retry(day_dir: Path, attempts: int = 8, sleep_seconds: float = 0.6) -> None:
    if not day_dir.exists():
        return
    last_error: Exception | None = None
    for _ in range(max(attempts, 1)):
        try:
            shutil.rmtree(day_dir)
            return
        except PermissionError as exc:
            last_error = exc
        except OSError as exc:
            winerror = getattr(exc, "winerror", 0)
            if winerror == 32:
                last_error = exc
            else:
                raise
        time.sleep(max(sleep_seconds, 0.1))
    if last_error is None:
        return
    raise ValueError(
        f"交付目录被占用，请关闭文件后重试：{day_dir}"
    ) from last_error


def build_delivery_bundle(*, db_path: Path, delivery_root: Path, day_label: str) -> dict[str, int | str]:
    target_day = parse_day_label(day_label)
    plan = resolve_delivery_plan(delivery_root, target_day)
    baseline_keys = _read_baseline_keys(delivery_root, plan.baseline_day)
    all_rows = _load_final_records(db_path)
    delta_rows = [row for row in all_rows if row["_key"] not in baseline_keys]
    day_dir = delivery_root / f"Malaysia_day{plan.target_day:03d}"
    _remove_delivery_dir_with_retry(day_dir)
    summary: dict[str, int | str] = {
        "day": plan.target_day,
        "baseline_day": plan.baseline_day,
        "latest_day_before_run": plan.latest_day,
        "total_current_companies": len(all_rows),
        "delta_companies": len(delta_rows),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    _write_delivery_files(day_dir, delta_rows, summary)
    return summary
