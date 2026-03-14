from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from ..core.pipeline import export_companies


def _print_ts(message: str, *, end: str = "\n", flush: bool = True) -> None:
    text = str(message or "")
    if not text:
        print(text, end=end, flush=flush)
        return
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} {text}", end=end, flush=flush)


def main() -> None:
    parser = argparse.ArgumentParser(description="国税庁 法人番号（全件）公司名录导出")
    parser.add_argument("--location", required=True, help="地区（都道府县，如：大阪府 / 北海道 / 東京都 / 全国）")
    parser.add_argument("--city", default=None, help="城市过滤（可选，如：札幌市；会在市名包含时保留）")
    parser.add_argument("--output-dir", default="output", help="输出目录（会创建时间戳子目录）")
    parser.add_argument("--cache-dir", default="output/hojin_cache", help="下载缓存目录")
    parser.add_argument("--all-kinds", action="store_true", help="不过滤公司形态（默认只导出 kind=301 公司法人）")
    parser.add_argument("--include-closed", action="store_true", help="包含已关闭法人（默认过滤 close_date）")
    parser.add_argument("--include-non-latest", action="store_true", help="包含非最新记录（默认 latest=1）")
    parser.add_argument("--max-records", type=int, default=None, help="最多导出 N 条（调试用）")
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir) / f"hojin_{stamp}"
    cache_dir = Path(args.cache_dir)

    meta = export_companies(
        location=str(args.location),
        city_filter=args.city,
        output_dir=out_dir,
        cache_dir=cache_dir,
        company_only=not bool(args.all_kinds),
        active_only=not bool(args.include_closed),
        latest_only=not bool(args.include_non_latest),
        max_records=args.max_records,
        log_sink=_print_ts,
    )
    _print_ts(f"[完成] 输出：{out_dir}")
    stats = meta.get("stats") if isinstance(meta, dict) else None
    if isinstance(stats, dict):
        _print_ts(f"[完成] 导出公司：{stats.get('exported_rows')} 条（读取 {stats.get('read_rows')} 条）")


if __name__ == "__main__":
    main()
