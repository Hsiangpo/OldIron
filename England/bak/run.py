"""英国站点执行入口。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

USAGE_TEXT = """用法：
  python run.py <site> [额外参数]

站点：
  dnb               — dnb.com 英国全站行业目录（邮箱阶段默认 Firecrawl）
  companies-house   — 英国.xlsx -> Companies House + GMap + Firecrawl
  dist              — England 静态切片执行与集中合并
"""

BASE_REQUIRED_MODULES = (("dotenv", "python-dotenv"),)
DNB_REQUIRED_MODULES = (
    ("curl_cffi", "curl_cffi"),
    ("lxml", "lxml"),
    ("requests", "requests"),
    ("websocket", "websocket-client"),
    ("openai", "openai"),
)
COMPANIES_HOUSE_REQUIRED_MODULES = (
    ("curl_cffi", "curl_cffi"),
    ("lxml", "lxml"),
    ("requests", "requests"),
    ("openpyxl", "openpyxl"),
    ("openai", "openai"),
)
DIST_PLAN_CH_REQUIRED_MODULES = (("openpyxl", "openpyxl"),)


def _load_project_env() -> bool:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return False
    load_dotenv(ROOT / ".env")
    return True


def _required_modules_for_command(site: str, rest: list[str]) -> tuple[tuple[str, str], ...]:
    normalized = str(site or "").strip().lower()
    if normalized == "dnb":
        return BASE_REQUIRED_MODULES + DNB_REQUIRED_MODULES
    if normalized in {"companies-house", "companies_house"}:
        return BASE_REQUIRED_MODULES + COMPANIES_HOUSE_REQUIRED_MODULES
    if normalized == "dist":
        subcommand = str(rest[0]).strip().lower() if rest else ""
        if subcommand == "plan-ch":
            return BASE_REQUIRED_MODULES + DIST_PLAN_CH_REQUIRED_MODULES
        return BASE_REQUIRED_MODULES
    return BASE_REQUIRED_MODULES


def _ensure_runtime_dependencies(site: str, rest: list[str]) -> bool:
    required_modules = _required_modules_for_command(site, rest)
    missing = [
        package_name
        for module_name, package_name in required_modules
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
    if not argv or argv[0].lower() in {"-h", "--help", "help"}:
        _load_project_env()
        print(USAGE_TEXT)
        return 0

    site = argv[0].strip().lower()
    rest = argv[1:]
    if not _ensure_runtime_dependencies(site, rest):
        return 1
    _load_project_env()
    if site == "dnb":
        from england_crawler.dnb.cli import run_dnb

        return run_dnb(rest)
    if site in {"companies-house", "companies_house"}:
        from england_crawler.companies_house.cli import run_companies_house

        return run_companies_house(rest)
    if site == "dist":
        from england_crawler.distributed.cli import run_dist

        return run_dist(rest)

    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
