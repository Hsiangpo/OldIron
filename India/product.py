from __future__ import annotations

import argparse
import csv
import re
import shutil
import sys
from pathlib import Path
from typing import Iterable, Tuple


BASE_DIR = Path(__file__).resolve().parent / "output" / "zauba_active"


def parse_day(arg: str) -> int:
    match = re.fullmatch(r"day(\d+)", arg.strip(), re.IGNORECASE)
    if not match:
        raise ValueError("argument format should be dayN, e.g. day1/day2/day10")
    value = int(match.group(1))
    if value < 1:
        raise ValueError("dayN requires N >= 1")
    return value


def day_filename(day: int) -> str:
    return f"companies_{day:03d}.csv"


def row_key(row: dict, fieldnames: Iterable[str]) -> Tuple[str, object]:
    cin = row.get("cin")
    if cin:
        return ("cin", cin)
    for name in fieldnames:
        value = row.get(name)
        if value:
            return (name, value)
    return ("row", tuple(row.get(name, "") for name in fieldnames))


def load_keys(paths: Iterable[Path]) -> set:
    keys: set = set()
    for path in paths:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for row in reader:
                keys.add(row_key(row, fieldnames))
    return keys


def write_delta(total_path: Path, output_path: Path, seen: set) -> int:
    count = 0
    with total_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            raise ValueError("总文件 companies.csv 为空或缺少表头")
        with output_path.open("w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                if row_key(row, fieldnames) in seen:
                    continue
                writer.writerow(row)
                count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="按天生成增量 companies_N.csv")
    parser.add_argument("day", help="例如 day1/day2/day10")
    parser.add_argument("--force", action="store_true", help="覆盖已存在的 day 文件")
    args = parser.parse_args()

    try:
        day = parse_day(args.day)
    except ValueError as exc:
        print(f"参数错误: {exc}")
        sys.exit(1)

    total_path = BASE_DIR / "companies.csv"
    if not total_path.exists():
        print(f"未找到总文件: {total_path}")
        sys.exit(1)

    output_path = BASE_DIR / day_filename(day)
    if output_path.exists() and not args.force:
        print(f"目标文件已存在: {output_path}")
        return

    if day == 1:
        shutil.copyfile(total_path, output_path)
        print(f"已生成 {output_path} (day1 全量快照)")
        return

    prev_paths = [BASE_DIR / day_filename(i) for i in range(1, day)]
    missing = [str(p) for p in prev_paths if not p.exists()]
    if missing:
        print("缺少历史日文件，无法生成增量：")
        for item in missing:
            print(f"- {item}")
        sys.exit(1)

    seen = load_keys(prev_paths)
    count = write_delta(total_path, output_path, seen)
    print(f"已生成 {output_path}，新增 {count} 条记录")


if __name__ == "__main__":
    main()
