from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
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

    def test_dispatch_rejects_archived_sites(self) -> None:
        run = _load_run_module()
        with patch.object(run, "_ensure_runtime_dependencies", return_value=True):
            code = run._dispatch(["dnb"])
        self.assertEqual(1, code)

    def test_myip_false_should_not_auto_start(self) -> None:
        from denmark_crawler.sites.proff.cli import _auto_start_go_backends
        from denmark_crawler.sites.proff.config import ProffDenmarkConfig

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = ProffDenmarkConfig.from_env(
                project_root=root,
                output_dir=root / "output",
                query_file=None,
                inline_queries=["ApS"],
                max_pages_per_query=1,
                max_companies=0,
                search_workers=1,
                gmap_workers=1,
                firecrawl_workers=1,
            )
            with patch("denmark_crawler.sites.proff.cli.ensure_services_started", return_value=["gmap"]) as mocked:
                with patch.dict("os.environ", {"MYIP_ENABLED": "false"}, clear=False):
                    started = _auto_start_go_backends(config=config, skip_gmap=False, skip_firecrawl=False)
            mocked.assert_called_once_with(["gmap"], quiet=True)
            self.assertEqual(["gmap"], started)

    def test_go_firecrawl_enabled_should_auto_start_firecrawl(self) -> None:
        from denmark_crawler.sites.proff.cli import _auto_start_go_backends
        from denmark_crawler.sites.proff.config import ProffDenmarkConfig

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = ProffDenmarkConfig.from_env(
                project_root=root,
                output_dir=root / "output",
                query_file=None,
                inline_queries=["ApS"],
                max_pages_per_query=1,
                max_companies=0,
                search_workers=1,
                gmap_workers=1,
                firecrawl_workers=1,
            )
            config.prefer_go_firecrawl_backend = True
            with patch("denmark_crawler.sites.proff.cli.ensure_services_started", return_value=["gmap", "firecrawl"]) as mocked:
                started = _auto_start_go_backends(config=config, skip_gmap=False, skip_firecrawl=False)
            mocked.assert_called_once_with(["gmap", "firecrawl"], quiet=True)
            self.assertEqual(["gmap", "firecrawl"], started)


if __name__ == "__main__":
    unittest.main()
