"""Clash unix-socket 控制器测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from brazil_crawler.sites.cnpjbiz.clash_controller import ClashUnixController


class ClashControllerTests(unittest.TestCase):
    def test_cycle_selector_moves_to_next_proxy(self) -> None:
        controller = ClashUnixController("/tmp/demo.sock")
        calls: list[tuple[str, str]] = []

        def fake_curl_json(path: str, *, method: str = "GET", data=None):
            if method == "GET":
                return {
                    "name": "PROXY",
                    "now": "A",
                    "all": ["A", "B", "DIRECT"],
                }
            calls.append((path, data["name"]))
            return {}

        with patch.object(controller, "_curl_json", side_effect=fake_curl_json):
            picked = controller.cycle_selector("PROXY", ignore={"DIRECT"})

        self.assertEqual("B", picked)
        self.assertEqual([("/proxies/PROXY", "B")], calls)
