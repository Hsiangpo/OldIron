"""英国站点执行入口。"""

from __future__ import annotations

import importlib.util
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
  dnb               — dnb.com 英国全站行业目录（邮箱阶段默认 Firecrawl）
  companies-house   — 英国.xlsx -> Companies House + GMap + Firecrawl
  cluster           — England 集群模式（Postgres + coordinator + worker）
"""

REQUIRED_MODULES = (
    ("curl_cffi", "curl_cffi"),
    ("dotenv", "python-dotenv"),
    ("lxml", "lxml"),
    ("requests", "requests"),
    ("openpyxl", "openpyxl"),
    ("websocket", "websocket-client"),
    ("openai", "openai"),
    ("psycopg", "psycopg[binary]"),
)


def _ensure_runtime_dependencies() -> bool:
    missing = [
        package_name
        for module_name, package_name in REQUIRED_MODULES
        if importlib.util.find_spec(module_name) is None
    ]
    if not missing:
        return True
    requirements_path = ROOT / "requirements.txt"
    print("当前 Python 缺少 England 运行依赖。")
    print(f"解释器: {sys.executable}")
    print(f"缺少: {', '.join(missing)}")
    print(f"安装命令: {sys.executable} -m pip install -r {requirements_path}")
    return False


def _dispatch(argv: list[str]) -> int:
    if not _ensure_runtime_dependencies():
        return 1
    if not argv or argv[0].lower() in {"-h", "--help", "help"}:
        print(USAGE_TEXT)
        return 0

    site = argv[0].strip().lower()
    rest = argv[1:]
    if site == "dnb":
        from england_crawler.dnb.cli import run_dnb

        return run_dnb(rest)
    if site in {"companies-house", "companies_house"}:
        from england_crawler.companies_house.cli import run_companies_house

        return run_companies_house(rest)
    if site.startswith("cluster"):
        from england_crawler.cluster.cli import run_cluster

        cluster_args = [site.removeprefix("cluster").strip("-_")] + rest if site != "cluster" else rest
        cluster_args = [item for item in cluster_args if item]
        return run_cluster(cluster_args)

    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
