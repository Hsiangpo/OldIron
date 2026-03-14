# -*- coding: utf-8 -*-
"""
按天交付增量数据脚本

用法:
    python product.py day1    # 第一天交付（全量）
    python product.py day2    # 第二天交付（增量 = 当前全量 - day1）
    python product.py day3    # 第三天交付（增量 = 当前全量 - day1 - day2）
    ...

特性:
    - 自动合并 web_jobs 下所有城市的 output.success.csv
    - 自动扣除历史已交付的数据（按"网站"字段去重）
    - 同一天重复运行会覆盖之前的产物
    - 产物文件命名: all_success_日期_时间_000N.csv
"""
import csv
import sys
import re
import os
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime
from typing import Set, List, Dict, Tuple


# ============ 配置 ============
BASE_DIR = Path(__file__).parent  # 项目根目录
OUTPUT_DIR = BASE_DIR / "output"  # output 目录
WEB_JOBS_DIR = OUTPUT_DIR / "web_jobs"
DELIVERY_DIR = OUTPUT_DIR / "delivery"  # 交付产物目录

# 用于判断记录唯一性的字段（网站URL）
UNIQUE_KEY_FIELD = "网站"

_REP_PLACEHOLDERS = {"未找到代表人", "未找到代表", "代表人未找到", "未知", "无"}
_PREFERRED_HEADERS = [
    "输入名称",
    "网站",
    "公司名称",
    "代表人",
    "注册资金",
    "公司人数",
    "座机",
    "邮箱",
    "邮箱列表",
    "邮箱数量",
    "公司名称来源",
    "代表人来源",
    "注册资金来源",
    "公司人数来源",
    "座机来源",
    "邮箱来源",
    "备注",
    "状态",
    "错误信息",
    "提取时间",
]


def _build_header_index(header: List[str]) -> Dict[str, int]:
    return {name: idx for idx, name in enumerate(header) if isinstance(name, str)}


def _canonical_site_key(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = "https://" + text
    parsed = urlparse(text)
    host = (parsed.hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _align_row(row: List[str], file_header: List[str], base_header: List[str]) -> List[str]:
    if file_header == base_header:
        if len(row) < len(base_header):
            return row + [""] * (len(base_header) - len(row))
        if len(row) > len(base_header):
            return row[: len(base_header)]
        return row
    file_index = _build_header_index(file_header)
    aligned = [""] * len(base_header)
    for i, name in enumerate(base_header):
        src = file_index.get(name)
        if isinstance(src, int) and src < len(row):
            aligned[i] = row[src]
    return aligned


def _cell(row: List[str], idx: int | None) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    value = row[idx]
    return value.strip() if isinstance(value, str) else ""


def _is_rep_placeholder(value: str) -> bool:
    if not value:
        return True
    cleaned = value.strip().replace("　", "")
    return cleaned in _REP_PLACEHOLDERS


def _parse_int(value: str) -> int | None:
    if not value:
        return None
    text = value.strip()
    return int(text) if text.isdigit() else None

def _row_is_complete(row: List[str], header_index: Dict[str, int]) -> bool:
    company = _cell(row, header_index.get("公司名称"))
    phone = _cell(row, header_index.get("座机"))
    email = _cell(row, header_index.get("邮箱"))
    rep = _cell(row, header_index.get("代表人"))
    # 交付完整口径（放松）：公司名 + 座机，或 公司名 + 代表人 + 邮箱
    return bool(company) and (bool(phone) or (bool(email) and not _is_rep_placeholder(rep)))


def _row_quality(row: List[str], header_index: Dict[str, int]) -> Tuple[int, int, str]:
    score = 0
    company = _cell(row, header_index.get("公司名称"))
    rep = _cell(row, header_index.get("代表人"))
    email = _cell(row, header_index.get("邮箱"))
    phone = _cell(row, header_index.get("座机"))
    emails = _cell(row, header_index.get("邮箱列表"))
    email_count = _parse_int(_cell(row, header_index.get("邮箱数量"))) or 0
    status = _cell(row, header_index.get("状态")).lower()
    extracted_at = _cell(row, header_index.get("提取时间"))

    if company:
        score += 2
    if phone:
        score += 4
    if rep and not _is_rep_placeholder(rep):
        score += 1
    if email:
        score += 1
    if emails:
        score += 1
    if email_count > 1:
        score += 1
    if status == "ok":
        score += 1
    return score, email_count, extracted_at


def _pick_better_row(
    existing: List[str],
    candidate: List[str],
    header_index: Dict[str, int],
) -> List[str]:
    if not existing:
        return candidate
    if not candidate:
        return existing
    existing_score, existing_email_count, existing_ts = _row_quality(existing, header_index)
    candidate_score, candidate_email_count, candidate_ts = _row_quality(candidate, header_index)
    if candidate_score > existing_score:
        return candidate
    if candidate_score < existing_score:
        return existing
    if candidate_email_count > existing_email_count:
        return candidate
    if candidate_email_count < existing_email_count:
        return existing
    if candidate_ts and existing_ts:
        return candidate if candidate_ts >= existing_ts else existing
    return existing


def parse_day_arg(arg: str) -> int:
    """解析命令行参数 day1/day2/... 返回天数"""
    match = re.match(r'^day(\d+)$', arg.lower())
    if not match:
        print(f"❌ 参数格式错误: {arg}")
        print("   正确格式: day1, day2, day3, ...")
        sys.exit(1)
    return int(match.group(1))


def day_to_suffix(day: int) -> str:
    """将天数转换为文件后缀 1 -> 0001, 2 -> 0002"""
    return f"{day:04d}"


def collect_all_success_data() -> Tuple[List[str], List[List[str]], Set[str]]:
    """
    收集 web_jobs 下所有城市的 output.success.csv 数据
    
    返回:
        header: 表头
        all_rows: 所有数据行
        all_keys: 所有唯一标识的集合
    """
    header = None
    all_rows = []
    all_keys = set()
    key_index = None
    header_index: Dict[str, int] = {}
    row_map: Dict[str, List[str]] = {}
    require_complete = (
        str(os.environ.get("PRODUCT_REQUIRE_COMPLETE") or "").strip().lower()
        not in {"0", "false", "no"}
    )
    
    # 遍历所有任务文件夹（含子目录），跳过 backup
    all_success_files = list(WEB_JOBS_DIR.rglob("site/output.success.csv"))
    success_files = [
        p
        for p in all_success_files
        if not any(part.name.lower() == "backup" for part in p.parents)
    ]
    skipped_backup = len(all_success_files) - len(success_files)
    
    if not success_files:
        print("❌ 未找到任何 output.success.csv 文件")
        sys.exit(1)

    # 先扫描所有 success 文件的表头，生成一个“稳定的 superset 表头”，确保新增字段不会丢失。
    header_union: Set[str] = set()
    for success_file in success_files:
        try:
            with open(success_file, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                file_header = next(reader, None)
            if not file_header:
                continue
            if UNIQUE_KEY_FIELD not in set(file_header):
                print(f"❌ 表头中未找到唯一键字段: {UNIQUE_KEY_FIELD} ({success_file})")
                sys.exit(1)
            for col in file_header:
                if isinstance(col, str) and col.strip():
                    header_union.add(col.strip())
        except Exception as exc:
            print(f"   ⚠️ 读取表头失败 {success_file}: {exc}")
            continue

    extras = sorted([c for c in header_union if c not in set(_PREFERRED_HEADERS)])
    header = list(_PREFERRED_HEADERS) + extras
    header_index = _build_header_index(header)
    key_index = header_index.get(UNIQUE_KEY_FIELD)
    if key_index is None:
        print("❌ 无法解析表头或唯一键索引")
        sys.exit(1)

    # 按城市名分组，避免同一城市多次跑导致重复展示
    city_files: Dict[str, List[Path]] = {}
    for success_file in success_files:
        city_folder = success_file.parent.parent.name
        city_name = city_folder.split("_")[-1] if "_" in city_folder else city_folder
        city_files.setdefault(city_name, []).append(success_file)

    print(f"📂 找到 {len(success_files)} 个 success 文件（{len(city_files)} 个城市）")
    if skipped_backup:
        print(f"   🚫 跳过 {skipped_backup} 个 backup 文件")
    
    for city_name in sorted(city_files.keys()):
        files = sorted(city_files[city_name])
        before_count = len(row_map)
        for success_file in files:
            try:
                with open(success_file, "r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.reader(f)
                    file_header = next(reader, None)
                    if not file_header:
                        continue
                    file_header_index = _build_header_index(file_header)
                    if UNIQUE_KEY_FIELD not in file_header_index:
                        print(f"❌ 表头中未找到唯一键字段: {UNIQUE_KEY_FIELD} ({success_file})")
                        sys.exit(1)

                    for row in reader:
                        if not row:
                            continue
                        aligned = _align_row(row, file_header, header)
                        if len(aligned) <= key_index:
                            continue
                        if require_complete and not _row_is_complete(aligned, header_index):
                            continue
                        raw_key = aligned[key_index]
                        key = _canonical_site_key(raw_key)
                        if not key:
                            key = (raw_key or "").strip().lower()
                        if not key:
                            continue
                        existing = row_map.get(key)
                        if existing is None:
                            row_map[key] = aligned
                        else:
                            row_map[key] = _pick_better_row(existing, aligned, header_index)
            except Exception as e:
                print(f"   ⚠️ 读取失败 {city_name}: {e}")
                continue

        added = len(row_map) - before_count
        print(f"   ✅ {city_name}: 累计 {len(row_map)} 条（新增 {added}）")
    
    all_rows = list(row_map.values())
    all_keys = set(row_map.keys())
    return header, all_rows, all_keys


def load_delivered_keys(day: int) -> Tuple[Set[str], Dict[str, bool]]:
    """
    Load day1 ~ day(N-1) delivered keys and their completeness.
    """
    delivered_keys: Set[str] = set()
    delivered_complete: Dict[str, bool] = {}

    if day <= 1:
        return delivered_keys, delivered_complete

    print(f"加载历史交付记录 (day1 ~ day{day-1})...")

    for d in range(1, day):
        suffix = day_to_suffix(d)
        pattern = f"*_{suffix}.csv"
        matching_files = list(DELIVERY_DIR.glob(pattern))
        if not matching_files:
            print(f"   day{d} ({suffix}) 的交付文件不存在，跳过")
            continue
        delivery_file = sorted(matching_files)[-1]
        try:
            with open(delivery_file, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                rows = list(reader)
                if len(rows) < 2:
                    continue
                header = rows[0]
                if UNIQUE_KEY_FIELD not in header:
                    continue
                key_index = header.index(UNIQUE_KEY_FIELD)
                header_index = _build_header_index(header)
                count_before = len(delivered_keys)
                for row in rows[1:]:
                    if len(row) > key_index:
                        raw_key = row[key_index]
                        key = _canonical_site_key(raw_key)
                        if not key:
                            key = (raw_key or "").strip().lower()
                        if key:
                            delivered_keys.add(key)
                            complete = False
                            if header_index:
                                complete = _row_is_complete(row, header_index)
                            delivered_complete[key] = delivered_complete.get(key, False) or complete
                count_added = len(delivered_keys) - count_before
                print(f"   {delivery_file.name}: +{count_added} 条")
        except Exception as e:
            print(f"   读取失败 {delivery_file.name}: {e}")

    print(f"   历史已交付总计: {len(delivered_keys)} 条")
    return delivered_keys, delivered_complete


def compute_incremental_data(
    header: List[str],
    all_rows: List[List[str]],
    delivered_keys: Set[str],
    delivered_complete: Dict[str, bool],
    allow_upgrade: bool,
    require_complete: bool,
) -> Tuple[List[List[str]], int]:
    """
    Incremental = current - delivered.
    If allow_upgrade=True, include sites that were delivered but incomplete
    and are complete now.
    """
    key_index = header.index(UNIQUE_KEY_FIELD)
    header_index = _build_header_index(header)

    incremental_rows: List[List[str]] = []
    upgrade_count = 0
    for row in all_rows:
        if len(row) <= key_index:
            continue
        raw_key = row[key_index]
        key = _canonical_site_key(raw_key)
        if not key:
            key = (raw_key or "").strip().lower()
        if not key:
            continue
        complete = _row_is_complete(row, header_index)
        if require_complete and not complete:
            continue
        if key not in delivered_keys:
            incremental_rows.append(row)
            continue
        if allow_upgrade and complete and not delivered_complete.get(key, False):
            incremental_rows.append(row)
            upgrade_count += 1

    return incremental_rows, upgrade_count


def save_delivery_file(
    header: List[str],
    rows: List[List[str]],
    day: int
) -> Path:
    """
    保存交付文件
    
    文件命名: all_success_年月日_时分秒_000N.csv
    """
    # 确保交付目录存在
    DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
    
    # 生成文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    suffix = day_to_suffix(day)
    filename = f"all_success_{timestamp}_{suffix}.csv"
    output_path = DELIVERY_DIR / filename
    
    # 删除同一天的旧文件（覆盖机制）
    old_files = list(DELIVERY_DIR.glob(f"*_{suffix}.csv"))
    for old_file in old_files:
        print(f"   🗑️ 删除旧文件: {old_file.name}")
        old_file.unlink()
    
    # 写入新文件
    with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    
    return output_path


def main():
    # 检查参数
    if len(sys.argv) < 2:
        print("用法: python product.py dayN")
        print("示例: python product.py day1")
        print("      python product.py day2")
        sys.exit(1)
    
    day = parse_day_arg(sys.argv[1])
    suffix = day_to_suffix(day)
    
    print("=" * 60)
    print(f"🚀 开始生成 day{day} ({suffix}) 的交付数据")
    print("=" * 60)
    
    # Step 1: 收集当前所有 success 数据
    print(f"\n📥 Step 1: 收集当前所有城市的 success 数据...")
    header, all_rows, all_keys = collect_all_success_data()
    print(f"   📊 当前全量数据: {len(all_rows)} 条（唯一网站数={len(all_keys)}）")
    
    # Step 2: 加载历史已交付数据
    delivered_keys, delivered_complete = load_delivered_keys(day)
    overlap_keys = all_keys & delivered_keys
    allow_upgrade = (
        str(os.environ.get("PRODUCT_ALLOW_UPGRADE") or "1").strip().lower()
        not in {"0", "false", "no"}
    )
    require_complete = (
        str(os.environ.get("PRODUCT_REQUIRE_COMPLETE") or "").strip().lower()
        not in {"0", "false", "no"}
    )
    delivered_missing_in_current = delivered_keys - all_keys

    # Step 3: 计算增量
    print(f"\n🔄 Step 2: 计算增量数据...")
    incremental_rows, upgrade_count = compute_incremental_data(
        header, all_rows, delivered_keys, delivered_complete, allow_upgrade, require_complete
    )
    print(f"   📊 增量数据: {len(incremental_rows)} 条")
    print(f"   📊 已交付唯一网站数: {len(delivered_keys)} 条")
    print(f"   📊 与当前全量重合: {len(overlap_keys)} 条")
    if delivered_missing_in_current:
        print(f"   ℹ️ 历史交付中有 {len(delivered_missing_in_current)} 条当前不存在（可能被规则过滤/重复/源文件变化）")
    print(f"   📊 计算公式: {len(all_keys)} (当前唯一) - {len(overlap_keys)} (已交付且仍存在) = {len(incremental_rows)} (增量)")
    
    if len(incremental_rows) == 0:
        print("\n⚠️ 警告: 增量数据为空，没有新数据需要交付")
        print("   可能原因: 所有数据都已在之前的交付中包含")
    
    # Step 4: 保存交付文件
    print(f"\n💾 Step 3: 保存交付文件...")
    output_path = save_delivery_file(header, incremental_rows, day)
    
    # 完成
    print("\n" + "=" * 60)
    print(f"✅ day{day} 交付数据生成完成!")
    print(f"   📁 文件: {output_path}")
    print(f"   📊 记录数: {len(incremental_rows)} 条")
    print("=" * 60)


if __name__ == "__main__":
    main()
