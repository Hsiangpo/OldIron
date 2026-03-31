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
    spec = importlib.util.spec_from_file_location("taiwan_run", ROOT / "run.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class RunDispatchTests(unittest.TestCase):
    def test_dispatch_calls_ieatpe_runner(self) -> None:
        run = _load_run_module()
        called: list[list[str]] = []
        fake_module = types.ModuleType("taiwan_crawler.sites.ieatpe.cli")
        fake_module.run_ieatpe = lambda argv: called.append(list(argv)) or 0
        with patch.object(run, "_ensure_runtime_dependencies", return_value=True):
            with patch.dict(sys.modules, {"taiwan_crawler.sites.ieatpe.cli": fake_module}):
                code = run._dispatch(["ieatpe", "--letters", "A,C"])
        self.assertEqual(0, code)
        self.assertEqual([["--letters", "A,C"]], called)

    def test_dispatch_rejects_unknown_site(self) -> None:
        run = _load_run_module()
        with patch.object(run, "_ensure_runtime_dependencies", return_value=True):
            code = run._dispatch(["unknown"])
        self.assertEqual(1, code)


if __name__ == "__main__":
    unittest.main()
