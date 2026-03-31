"""美国 run.py 分发测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import run as run_module


class RunDispatchTests(unittest.TestCase):
    @patch("run.importlib.util.find_spec", return_value=object())
    @patch("unitedstates_crawler.sites.dnb.cli.run_dnb", return_value=0)
    def test_dispatches_to_dnb_cli(self, run_dnb, _find_spec) -> None:
        result = run_module._dispatch(["dnb"])
        self.assertEqual(0, result)
        run_dnb.assert_called_once_with([])


if __name__ == "__main__":
    unittest.main()
