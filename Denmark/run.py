"""丹麦新框架执行入口。"""

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
  proff  — proff.dk 新框架主站点
  virk   — datacvr.virk.dk 丹麦 CVR 公司注册库
"""

BASE_REQUIRED_MODULES = (
    ("dotenv", "python-dotenv"),
    ("requests", "requests"),
    ("curl_cffi", "curl_cffi"),
    ("openai", "openai"),
)


def _load_project_env() -> bool:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return False
    load_dotenv(ROOT / ".env")
    return True


def _required_modules_for_command(site: str) -> tuple[tuple[str, str], ...]:
    normalized = str(site or "").strip().lower()
    if normalized == "proff":
        return BASE_REQUIRED_MODULES
    if normalized == "virk":
        return BASE_REQUIRED_MODULES
    return BASE_REQUIRED_MODULES


def _ensure_runtime_dependencies(site: str) -> bool:
    required_modules = _required_modules_for_command(site)
    missing = [
        package_name
        for module_name, package_name in required_modules
        if importlib.util.find_spec(module_name) is None
    ]
    if not missing:
        return True
    requirements_path = ROOT / "requirements.txt"
    print("当前 Python 缺少 Denmark 新框架运行依赖。")
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
    if site == "proff":
        from denmark_crawler.sites.proff.cli import run_proff

        return run_proff(rest)

    if site == "virk":
        from denmark_crawler.sites.virk.cli import run_virk

        return run_virk(rest)

    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
