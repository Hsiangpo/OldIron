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


class CompaniesHouseCliTests(unittest.TestCase):
    def test_parser_defaults_ch_workers_to_two(self) -> None:
        from england_crawler.companies_house.cli import _build_parser

        args = _build_parser().parse_args([])

        self.assertEqual(2, args.ch_workers)

    def test_parser_accepts_input_file_and_output_dir(self) -> None:
        from england_crawler.companies_house.cli import _build_parser

        args = _build_parser().parse_args([
            "--input-file",
            "companies.txt",
            "--output-dir",
            "custom-output",
        ])

        self.assertEqual("companies.txt", args.input_file)
        self.assertEqual("custom-output", args.output_dir)

    def test_dispatch_calls_companies_house_runner(self) -> None:
        module_name = "england_crawler.companies_house.cli"
        fake_module = types.ModuleType(module_name)
        called: dict[str, object] = {}

        def _runner(argv: list[str]) -> int:
            called["argv"] = argv
            return 11

        fake_module.run_companies_house = _runner
        with patch.dict(sys.modules, {module_name: fake_module}):
            code = run._dispatch(["companies-house", "--max-companies", "5"])

        self.assertEqual(11, code)
        self.assertEqual(["--max-companies", "5"], called["argv"])


if __name__ == "__main__":
    unittest.main()
