"""项目命令行入口。"""

from __future__ import annotations

import sys

from malaysia_crawler.cli import run_cli


if __name__ == "__main__":
    raise SystemExit(run_cli(sys.argv[1:]))

