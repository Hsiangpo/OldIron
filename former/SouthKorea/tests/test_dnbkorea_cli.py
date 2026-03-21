import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import run  # noqa: E402


class DnbKoreaCliTests(unittest.TestCase):
    def test_dispatch_calls_dnb_runner_alias(self) -> None:
        module_name = "korea_crawler.dnb.cli"
        fake_module = types.ModuleType(module_name)
        called: dict[str, object] = {}

        def _runner(argv: list[str]) -> int:
            called["argv"] = argv
            return 11

        fake_module.run_dnbkorea = _runner
        with patch.dict(sys.modules, {module_name: fake_module}):
            code = run._dispatch(["dnb", "--max-companies", "5"])

        self.assertEqual(11, code)
        self.assertEqual(["--max-companies", "5"], called["argv"])

    def test_dispatch_calls_dnbkorea_runner(self) -> None:
        module_name = "korea_crawler.dnb.cli"
        fake_module = types.ModuleType(module_name)
        called: dict[str, object] = {}

        def _runner(argv: list[str]) -> int:
            called["argv"] = argv
            return 7

        fake_module.run_dnbkorea = _runner
        with patch.dict(sys.modules, {module_name: fake_module}):
            code = run._dispatch(["dnbkorea", "--max-companies", "5"])

        self.assertEqual(7, code)
        self.assertEqual(["--max-companies", "5"], called["argv"])


if __name__ == "__main__":
    unittest.main()
