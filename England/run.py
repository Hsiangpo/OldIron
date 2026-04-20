"""英国新框架执行入口。"""

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
# 注入 shared/ 目录，以便 import oldiron_core
SHARED_DIR = SHARED_PARENT / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))


def _configure_stdio_utf8() -> None:
    """统一入口标准流编码，避免 Windows 默认代码页导致中文输出崩溃。"""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if not callable(reconfigure):
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            continue


_configure_stdio_utf8()

USAGE_TEXT = """用法：
  python run.py <site> [额外参数]

站点：
  companyname  — 从 Excel 公司名单出发，GMap + 邮箱补充
  kompass      — Kompass 英国官网列表
  wiza         — Wiza 英国网站列表
"""

BASE_REQUIRED_MODULES = (
    ("dotenv", "python-dotenv"),
    ("curl_cffi", "curl_cffi"),
)
COMPANYNAME_REQUIRED_MODULES = (
    ("openpyxl", "openpyxl"),
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


def _ensure_runtime_dependencies(site: str) -> bool:
    required_modules = list(BASE_REQUIRED_MODULES)
    if site == "companyname":
        required_modules.extend(COMPANYNAME_REQUIRED_MODULES)
    missing = [
        package_name
        for module_name, package_name in required_modules
        if importlib.util.find_spec(module_name) is None
    ]
    if not missing:
        return True
    requirements_path = ROOT / "requirements.txt"
    print("当前 Python 缺少 England 新框架运行依赖。")
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
    if site == "companyname":
        from england_crawler.sites.companyname.cli import run_companyname

        return run_companyname(rest)
    if site == "kompass":
        from england_crawler.sites.kompass.cli import run_site

        return run_site(rest)
    if site == "wiza":
        from england_crawler.sites.wiza.cli import run_site

        return run_site(rest)

    print(f"不支持的网站: {argv[0]}")
    print(USAGE_TEXT)
    return 1


if __name__ == "__main__":
    raise SystemExit(_dispatch(sys.argv[1:]))
