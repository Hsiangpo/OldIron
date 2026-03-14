"""快捷执行入口 — 按网站名指定爬取目标，支持断点续跑。"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

load_dotenv(ROOT / ".env")

USAGE_TEXT = """用法：
  python run.py <site> [额外参数]

说明：
  gapensi：
    爬取 gapensi.or.id 全量公司数据（公司名、法人、邮箱），支持断点续跑
    可选参数:
      --max-pages N      最大页数（默认全量）
      --skip-crawl       跳过爬取阶段（已跑完时使用）
      --log-level LEVEL  日志级别（DEBUG/INFO/WARNING/ERROR）

  indonesiayp：
    爬取 indonesiayp.com + AHU + Snov 的联动数据
    可选参数:
      --max-pages N      列表最大页数
      --max-items N      详情/AHU/Snov 最大条数
      --skip-list        跳过列表阶段
      --skip-detail      跳过详情阶段
      --skip-ahu         跳过 AHU 法人阶段
      --skip-snov        跳过 Snov 邮箱阶段
      --serial           串行执行（默认并行流水线）
      --log-level LEVEL  日志级别（DEBUG/INFO/WARNING/ERROR）

  交付:
    python run.py deliver day1   打包第1天交付

  代理探针:
    python run.py probe-proxy --rounds 0 --interval 30 --stop-on-success

  桥接代理:
    python run.py proxy-bridge --prefix CHAIN
"""


def _dispatch(argv: list[str]) -> int:
    if not argv or argv[0].lower() in {"-h", "--help", "help"}:
        print(USAGE_TEXT)
        return 0

    site = argv[0].strip().lower()
    rest = argv[1:]

    if site == "gapensi":
        from indonesia_crawler.sites.gapensi import run_gapensi

        return run_gapensi(rest)

    if site == "indonesiayp":
        from indonesia_crawler.sites.indonesiayp import run_indonesiayp

        return run_indonesiayp(rest)

    if site == "deliver":
        if not rest:
            print("请指定交付日期，如: python run.py deliver day1")
            return 1
        from indonesia_crawler.delivery import build_delivery_bundle

        output_dir = ROOT / "output"
        delivery_dir = output_dir / "delivery"
        summary = build_delivery_bundle(output_dir, delivery_dir, rest[0])
        day = int(summary["day"])
        print(
            "交付完成：day{day}，基线 day{baseline}，当日增量 {delta}，当前总量 {total}".format(
                day=day,
                baseline=int(summary["baseline_day"]),
                delta=int(summary["delta_companies"]),
                total=int(summary["total_current_companies"]),
            )
        )
        print(f"目录：{delivery_dir / f'Indonesia_day{day:03d}'}")
        return 0

    if site in {"probe-proxy", "proxy-probe", "probe_proxy"}:
        from indonesia_crawler.proxy import run_proxy_probe

        return run_proxy_probe(rest)

    if site in {"proxy-bridge", "bridge-proxy", "proxy_bridge"}:
        from indonesia_crawler.proxy import run_proxy_bridge

        return run_proxy_bridge(rest)

    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
