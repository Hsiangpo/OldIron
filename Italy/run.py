"""意大利新框架执行入口。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
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
  wiza  — Wiza 意大利官网列表
"""

REQUIRED_MODULES = (
    ("dotenv", "python-dotenv"),
    ("curl_cffi", "curl_cffi"),
)


def _load_project_env() -> bool:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return False
    load_dotenv(ROOT / ".env", override=True)
    return True


def _ensure_runtime_dependencies() -> bool:
    missing: list[str] = []
    for module_name, package_name in REQUIRED_MODULES:
        if importlib.util.find_spec(module_name) is None:
            missing.append(package_name)
    if not missing:
        return True
    print("当前 Python 缺少 Italy 运行依赖。")
    print(f"解释器: {sys.executable}")
    print(f"缺少: {', '.join(missing)}")
    print(f"安装命令: {sys.executable} -m pip install -r {ROOT / 'requirements.txt'}")
    return False


def _dispatch(argv: list[str]) -> int:
    if not argv or argv[0].lower() in {"-h", "--help", "help"}:
        _load_project_env()
        print(USAGE_TEXT)
        return 0
    if not _ensure_runtime_dependencies():
        return 1
    _load_project_env()
    site = argv[0].strip().lower()
    if site == "wiza":
        from italy_crawler.sites.wiza.cli import run_site

        return run_site(argv[1:])
    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
