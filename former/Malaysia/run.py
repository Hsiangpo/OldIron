"""快捷执行入口。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from malaysia_crawler.cli import run_cli  # noqa: E402

USAGE_TEXT = """用法：
  python run.py
  python run.py CTOS [额外参数]
  python run.py BusinessList [额外参数]
  python run.py SnovPipeline [额外参数]
  python run.py Cookie [额外参数]

说明：
  直接执行 python run.py：
    启动三线并发主流程（CTOS + BusinessList + Snov），默认 BusinessList 走 cf 协议模式，支持断点续跑
  CTOS：
    抓取 CTOS 公共目录（公司名+注册号），可选 --with-detail 抓免费详情
  BusinessList：
    按 company_id 区间抓取公开公司档案（邮箱、管理人、官网、电话）
  SnovPipeline：
    CTOS公司名 -> BusinessList官网 -> Snov域名邮箱，默认目标 30 家
  Cookie：
    连接 9222 调试浏览器，手动通过 cf 后自动写入运行期 cookie
"""


def _dispatch(argv: list[str]) -> int:
    if not argv:
        return run_cli(["streaming-run"])
    if argv[0].lower() in {"-h", "--help", "help"}:
        print(USAGE_TEXT)
        return 0

    task = argv[0].strip().lower()
    rest = argv[1:]
    if task == "ctos":
        return run_cli(["ctos-directory-crawl", *rest])
    if task == "businesslist":
        return run_cli(["businesslist-crawl", *rest])
    if task in {"snovpipeline", "ctosbusinesslistsnov"}:
        return run_cli(["ctos-businesslist-snov", *rest])
    if task in {"cookie", "synccookie"}:
        return run_cli(["sync-businesslist-cookie", *rest])
    if task in {"streaming", "main", "all"}:
        return run_cli(["streaming-run", *rest])

    print(f"不支持的任务：{argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
