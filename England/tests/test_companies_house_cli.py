import sys
import types
import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import run  # noqa: E402


class CompaniesHouseCliTests(unittest.TestCase):
    def test_parser_defaults_ch_workers_to_four(self) -> None:
        from england_crawler.companies_house.cli import _build_parser

        args = _build_parser().parse_args([])

        self.assertEqual(4, args.ch_workers)

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

    def test_config_accepts_firecrawl_keys_file(self) -> None:
        from england_crawler.companies_house.config import CompaniesHouseConfig

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_file = root / "companies.txt"
            keys_file = root / "firecrawl_keys.txt"
            input_file.write_text("ALPHA LTD\n", encoding="utf-8")
            keys_file.write_text("fc-demo-key\n", encoding="utf-8")

            with patch.dict(
                "os.environ",
                {
                    "FIRECRAWL_KEYS": "",
                    "FIRECRAWL_KEYS_FILE": str(keys_file),
                    "LLM_API_KEY": "llm-demo",
                    "LLM_MODEL": "gpt-5.1-codex-mini",
                },
                clear=False,
            ):
                config = CompaniesHouseConfig.from_env(
                    project_root=root,
                    input_xlsx=input_file,
                    output_dir=root / "output",
                    max_companies=10,
                    ch_workers=1,
                    gmap_workers=1,
                    snov_workers=1,
                )

                config.validate(skip_firecrawl=False)

    def test_config_defaults_firecrawl_key_file_to_project_output(self) -> None:
        from england_crawler.companies_house.config import CompaniesHouseConfig

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_file = root / "companies.txt"
            input_file.write_text("ALPHA LTD\n", encoding="utf-8")
            project_keys = root / "output" / "firecrawl_keys.txt"
            project_keys.parent.mkdir(parents=True, exist_ok=True)
            project_keys.write_text("fc-demo-key\n", encoding="utf-8")

            with patch.dict(
                "os.environ",
                {
                    "FIRECRAWL_KEYS": "",
                    "FIRECRAWL_KEYS_FILE": "",
                    "LLM_API_KEY": "llm-demo",
                    "LLM_MODEL": "gpt-5.1-codex-mini",
                },
                clear=False,
            ):
                config = CompaniesHouseConfig.from_env(
                    project_root=root,
                    input_xlsx=input_file,
                    output_dir=root / "smoke-run",
                    max_companies=10,
                    ch_workers=1,
                    gmap_workers=1,
                    snov_workers=1,
                )

                self.assertEqual(project_keys, config.firecrawl_keys_file)
                config.validate(skip_firecrawl=False)


if __name__ == "__main__":
    unittest.main()
