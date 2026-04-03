"""协调开工前检查脚本。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from coordination.coord_cli import main


if __name__ == "__main__":
    raise SystemExit(main(["check", *sys.argv[1:]]))
