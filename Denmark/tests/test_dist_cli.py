from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DistCliTests(unittest.TestCase):
    def test_parser_defaults_country_to_dk(self) -> None:
        from denmark_crawler.distributed.cli import _build_parser

        args = _build_parser().parse_args(["plan-dnb", "--shards", "2"])
        self.assertEqual("dk", args.country)

    def test_plan_dnb_writes_manifest(self) -> None:
        from denmark_crawler.distributed.dnb_planner import plan_dnb_shards

        with tempfile.TemporaryDirectory() as tmp:
            summary = plan_dnb_shards(tmp, shard_count=2, country_iso_two_code="dk")
            self.assertEqual("dk", summary["country_iso_two_code"])
            self.assertTrue((Path(tmp) / "manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
