"""England 静态切片与合并 CLI。"""

from __future__ import annotations

import argparse
from pathlib import Path

from england_crawler.distributed.ch_planner import plan_companies_house_shards
from england_crawler.distributed.dnb_planner import plan_dnb_shards
from england_crawler.distributed.site_merge import merge_site_runs


ROOT = Path(__file__).resolve().parents[3]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="England 静态切片与合并命令")
    sub = parser.add_subparsers(dest="command", required=True)

    plan_ch = sub.add_parser("plan-ch", help="生成 Companies House 静态切片")
    plan_ch.add_argument("--input-xlsx", default=str(ROOT / "docs" / "英国.xlsx"))
    plan_ch.add_argument("--output-dir", default=str(ROOT / "output" / "distributed" / "ch"))
    plan_ch.add_argument("--shards", type=int, required=True)

    plan_dnb = sub.add_parser("plan-dnb", help="生成 DNB 静态切片")
    plan_dnb.add_argument("--output-dir", default=str(ROOT / "output" / "distributed" / "dnb"))
    plan_dnb.add_argument("--country", default="gb")
    plan_dnb.add_argument("--shards", type=int, required=True)

    merge_site = sub.add_parser("merge-site", help="合并站点产物")
    merge_site.add_argument("site", choices=["dnb", "companies-house"])
    merge_site.add_argument("--run-dir", action="append", required=True)
    merge_site.add_argument("--output-dir", default="")

    return parser


def _default_site_output(site: str) -> Path:
    if site == "dnb":
        return ROOT / "output" / "dnb"
    return ROOT / "output" / "companies_house"


def run_dist(argv: list[str]) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "plan-ch":
        summary = plan_companies_house_shards(
            args.input_xlsx,
            args.output_dir,
            shard_count=max(int(args.shards), 1),
        )
        print(
            "England CH 静态切片完成：shards={shards} total={total}".format(
                shards=int(summary["shard_count"]),
                total=int(summary["total_companies"]),
            )
        )
        print(f"目录：{summary['output_dir']}")
        return 0
    if args.command == "plan-dnb":
        summary = plan_dnb_shards(
            args.output_dir,
            shard_count=max(int(args.shards), 1),
            country_iso_two_code=str(args.country).strip().lower() or "gb",
        )
        print(
            "England DNB 静态切片完成：shards={shards} total={total}".format(
                shards=int(summary["shard_count"]),
                total=int(summary["total_segments"]),
            )
        )
        print(f"目录：{summary['output_dir']}")
        return 0
    if args.command == "merge-site":
        output_dir = Path(args.output_dir).resolve() if str(args.output_dir).strip() else _default_site_output(args.site)
        summary = merge_site_runs(list(args.run_dir), output_dir)
        print(
            "England 站点合并完成：records={records} merged={merged}".format(
                records=int(summary["input_records"]),
                merged=int(summary["merged_companies"]),
            )
        )
        print(f"目录：{summary['output_dir']}")
        return 0
    raise ValueError(f"不支持的 dist 命令: {args.command}")
