from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_DIR = ROOT.parent / "shared"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))


def _load_run_module():
    spec = importlib.util.spec_from_file_location("italy_run", ROOT / "run.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class RunDispatchTests(unittest.TestCase):
    def test_dispatches_to_wiza_cli(self) -> None:
        run_module = _load_run_module()
        with patch.object(run_module.importlib.util, "find_spec", return_value=object()):
            with patch("italy_crawler.sites.wiza.cli.run_site", return_value=0) as run_site:
                result = run_module._dispatch(["wiza", "list"])
        self.assertEqual(0, result)
        run_site.assert_called_once_with(["list"])
