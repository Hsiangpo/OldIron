"""芬兰新框架执行入口。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent  # OldIron/ — 包含 shared/oldiron_core
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

USAGE_TEXT = """用法：
  python run.py <site> [额外参数]

站点：
  tmt        — tyomarkkinatori.fi 芬兰劳动力市场（有公开 API，优先）
  duunitori  — duunitori.fi 芬兰求职网（SSR HTML 解析）
  jobly      — jobly.fi 芬兰职位网（SSR HTML 解析）
"""

BASE_REQUIRED_MODULES = (
    ("dotenv", "python-dotenv"),
    ("requests", "requests"),
    ("curl_cffi", "curl_cffi"),
    ("openai", "openai"),
    ("bs4", "beautifulsoup4"),
)


def _load_project_env() -> bool:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return False
    load_dotenv(ROOT / ".env")
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
    print("当前 Python 缺少 Finland 新框架运行依赖。")
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

    if site == "tmt":
        from finland_crawler.sites.tyomarkkinatori.cli import run_tmt
        return run_tmt(rest)

    if site == "duunitori":
        from finland_crawler.sites.duunitori.cli import run_duunitori
        return run_duunitori(rest)

    if site == "jobly":
        from finland_crawler.sites.jobly.cli import run_jobly
        return run_jobly(rest)

    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
