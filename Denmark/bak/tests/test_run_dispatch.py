from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _load_run_module():
    spec = importlib.util.spec_from_file_location("denmark_run", ROOT / "run.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class RunDispatchTests(unittest.TestCase):
    def test_dispatch_calls_dnb_runner(self) -> None:
        run = _load_run_module()
        called: list[list[str]] = []
        fake_module = types.ModuleType("denmark_crawler.dnb.cli")
        fake_module.run_dnb = lambda argv: called.append(list(argv)) or 0
        with patch.object(run, "_ensure_runtime_dependencies", return_value=True):
            with patch.dict(sys.modules, {"denmark_crawler.dnb.cli": fake_module}):
                code = run._dispatch(["dnb", "--max-companies", "5"])
        self.assertEqual(0, code)
        self.assertEqual([["--max-companies", "5"]], called)

    def test_dispatch_calls_dist_runner(self) -> None:
        run = _load_run_module()
        called: list[list[str]] = []
        fake_module = types.ModuleType("denmark_crawler.distributed.cli")
        fake_module.run_dist = lambda argv: called.append(list(argv)) or 0
        with patch.object(run, "_ensure_runtime_dependencies", return_value=True):
            with patch.dict(sys.modules, {"denmark_crawler.distributed.cli": fake_module}):
                code = run._dispatch(["dist", "plan-dnb", "--shards", "2"])
        self.assertEqual(0, code)
        self.assertEqual([["plan-dnb", "--shards", "2"]], called)

    def test_dispatch_calls_virk_runner(self) -> None:
        run = _load_run_module()
        called: list[list[str]] = []
        fake_module = types.ModuleType("denmark_crawler.virk.cli")
        fake_module.run_virk = lambda argv: called.append(list(argv)) or 0
        with patch.object(run, "_ensure_runtime_dependencies", return_value=True):
            with patch.dict(sys.modules, {"denmark_crawler.virk.cli": fake_module}):
                code = run._dispatch(["virk", "--max-companies", "5"])
        self.assertEqual(0, code)
        self.assertEqual([["--max-companies", "5"]], called)

    def test_dispatch_calls_proff_runner(self) -> None:
        run = _load_run_module()
        called: list[list[str]] = []
        fake_module = types.ModuleType("denmark_crawler.sites.proff.cli")
        fake_module.run_proff = lambda argv: called.append(list(argv)) or 0
        with patch.object(run, "_ensure_runtime_dependencies", return_value=True):
            with patch.dict(sys.modules, {"denmark_crawler.sites.proff.cli": fake_module}):
                code = run._dispatch(["proff", "--max-pages-per-query", "2"])
        self.assertEqual(0, code)
        self.assertEqual([["--max-pages-per-query", "2"]], called)


if __name__ == "__main__":
    unittest.main()
