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

站点：
  catch     — catch.co.kr 全量公司数据
  incheon   — bizok.incheon.go.kr 仁川企业信息
  dart      — DART Open API 全韩法人数据
  saramin   — saramin.co.kr 招聘企业信息
  khia      — khia.or.kr 韩国HRD企业协会会员
  kssba     — kssba.or.kr 韩国강소企业协会会员
  dsnuri    — dsnuri.com 세종 사회적경제기업현황
  gpsc      — gpsc.or.kr 경기광역자활센터 자활기업
  dnb       — dnb.com 韩国全站行业企业目录
  dnbkorea  — dnb.com 韩国全站行业企业目录（兼容旧命令）

通用参数:
  --max-pages N      列表最大页数（默认全量）
  --max-items N      详情/Snov最大条数（默认全量）
  --skip-list        跳过列表阶段
  --skip-detail      跳过详情阶段
  --skip-gmap        跳过 Google Maps 官网补齐（仅 khia/gpsc/catch）
  --skip-snov        跳过Snov阶段
  --log-level LEVEL  日志级别（DEBUG/INFO/WARNING/ERROR）
"""


def _dispatch(argv: list[str]) -> int:
    if not argv or argv[0].lower() in {"-h", "--help", "help"}:
        print(USAGE_TEXT)
        return 0

    site = argv[0].strip().lower()
    rest = argv[1:]

    if site == "catch":
        from korea_crawler.sites.catch import run_catch
        return run_catch(rest)

    if site == "incheon":
        from korea_crawler.sites.incheon import run_incheon
        return run_incheon(rest)

    if site == "dart":
        from korea_crawler.sites.dart import run_dart
        return run_dart(rest)

    if site == "saramin":
        from korea_crawler.sites.saramin import run_saramin
        return run_saramin(rest)

    if site == "khia":
        from korea_crawler.sites.khia import run_khia
        return run_khia(rest)

    if site == "kssba":
        from korea_crawler.sites.kssba import run_kssba
        return run_kssba(rest)

    if site == "dsnuri":
        from korea_crawler.sites.dsnuri import run_dsnuri
        return run_dsnuri(rest)

    if site == "gpsc":
        from korea_crawler.sites.gpsc import run_gpsc
        return run_gpsc(rest)

    if site in {"dnb", "dnbkorea"}:
        from korea_crawler.dnb.cli import run_dnbkorea
        return run_dnbkorea(rest)

    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
