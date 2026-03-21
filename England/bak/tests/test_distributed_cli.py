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


class DistributedCliDispatchTests(unittest.TestCase):
    def test_dispatch_calls_dist_runner(self) -> None:
        module_name = "england_crawler.distributed.cli"
        fake_module = types.ModuleType(module_name)
        called: dict[str, object] = {}

        def _runner(argv: list[str]) -> int:
            called["argv"] = argv
            return 13

        fake_module.run_dist = _runner
        with patch.dict(sys.modules, {module_name: fake_module}):
            code = run._dispatch(["dist", "plan-ch", "--shards", "2"])

        self.assertEqual(13, code)
        self.assertEqual(["plan-ch", "--shards", "2"], called["argv"])

    def test_dispatch_rejects_cluster_mode(self) -> None:
        code = run._dispatch(["cluster", "produce", "day2"])

        self.assertEqual(1, code)

    def test_usage_text_omits_cluster_mode(self) -> None:
        self.assertNotIn("cluster", run.USAGE_TEXT.lower())

    def test_dist_merge_does_not_require_dnb_runtime_modules(self) -> None:
        modules = run._required_modules_for_command("dist", ["merge-site", "dnb"])
        module_names = {name for name, _package in modules}

        self.assertNotIn("curl_cffi", module_names)
        self.assertNotIn("websocket", module_names)
        self.assertNotIn("openai", module_names)


if __name__ == "__main__":
    unittest.main()
