"""统一执行入口。"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

load_dotenv(ROOT / ".env", override=True)

from thailand_crawler.cli import run_cli  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(run_cli(sys.argv[1:]))

