"""美国新框架执行入口。"""

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
  dnb  — DNB 美国企业目录
"""

BASE_REQUIRED_MODULES = (
    ("dotenv", "python-dotenv"),
    ("curl_cffi", "curl_cffi"),
    ("playwright", "playwright"),
    ("openai", "openai"),
)


def _load_project_env() -> bool:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return False
    # 国家目录下的 .env 必须覆盖宿主机残留环境变量，避免错用别国或测试 key。
    load_dotenv(ROOT / ".env", override=True)
    return True


def _ensure_runtime_dependencies() -> bool:
    missing: list[str] = []
    incompatible: list[str] = []
    for module_name, package_name in BASE_REQUIRED_MODULES:
        if importlib.util.find_spec(module_name) is None:
            missing.append(package_name)
            continue
        if module_name == "openai" and not _openai_client_ready():
            incompatible.append("openai>=1.0")
    if not missing:
        if not incompatible:
            return True
    if missing:
        print("当前 Python 缺少 UnitedStates 运行依赖。")
    else:
        print("当前 Python 的 UnitedStates 运行依赖版本不兼容。")
    print(f"解释器: {sys.executable}")
    if missing:
        print(f"缺少: {', '.join(missing)}")
    if incompatible:
        print(f"不兼容: {', '.join(incompatible)}")
    print(f"安装命令: {sys.executable} -m pip install -r {ROOT / 'requirements.txt'}")
    return False


def _openai_client_ready() -> bool:
    try:
        import openai
    except Exception:
        return False
    return hasattr(openai, "OpenAI")


def _dispatch(argv: list[str]) -> int:
    if not argv or argv[0].lower() in {"-h", "--help", "help"}:
        _load_project_env()
        print(USAGE_TEXT)
        return 0
    if not _ensure_runtime_dependencies():
        return 1
    _load_project_env()
    site = argv[0].strip().lower()
    if site == "dnb":
        from unitedstates_crawler.sites.dnb.cli import run_dnb

        return run_dnb(argv[1:])
    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
