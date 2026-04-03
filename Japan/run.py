"""日本新框架执行入口。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent  # OldIron/ — 包含 shared/oldiron_core
SHARED_DIR = SHARED_PARENT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

USAGE_TEXT = """用法：
  python run.py <site> [额外参数]

站点：
  bizmaps     — biz-maps.com 日本企业信息列表
  hellowork   — ハローワーク 日本企业信息（求人検索）
  openwork    — OpenWork 日本企业信息列表
  onecareer   — One Career 日本企业信息列表
  xlsximport  — xlsx 导入官网+邮箱，Protocol+LLM 补全公司名和代表人
"""

BASE_REQUIRED_MODULES = (
    ("dotenv", "python-dotenv"),
    ("requests", "requests"),
    ("curl_cffi", "curl_cffi"),
    ("lxml", "lxml"),
)


def _load_project_env() -> bool:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return False
    # 国家目录下的 .env 必须覆盖宿主机残留环境变量，避免错用别国或测试 key。
    load_dotenv(ROOT / ".env", override=True)
    return True


def _ensure_runtime_dependencies(site: str) -> bool:
    missing = [
        package_name
        for module_name, package_name in BASE_REQUIRED_MODULES
        if importlib.util.find_spec(module_name) is None
    ]
    if not missing:
        return True
    requirements_path = ROOT / "requirements.txt"
    print("当前 Python 缺少 Japan 新框架运行依赖。")
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
    if not _ensure_runtime_dependencies(site):
        return 1
    _load_project_env()
    if site == "bizmaps":
        from japan_crawler.sites.bizmaps.cli import run_bizmaps

        return run_bizmaps(rest)

    if site == "hellowork":
        from japan_crawler.sites.hellowork.cli import run_hellowork

        return run_hellowork(rest)

    if site == "openwork":
        from japan_crawler.sites.openwork.cli import run_openwork

        return run_openwork(rest)

    if site == "onecareer":
        from japan_crawler.sites.onecareer.cli import run_onecareer

        return run_onecareer(rest)

    if site == "xlsximport":
        from japan_crawler.sites.xlsximport.cli import run_xlsximport

        return run_xlsximport(rest)

    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
