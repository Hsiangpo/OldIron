from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class VirkCliTests(unittest.TestCase):
    def test_parser_defaults(self) -> None:
        from denmark_crawler.virk.cli import _build_parser

        args = _build_parser().parse_args([])
        self.assertEqual(96, args.firecrawl_workers)
        self.assertEqual(2, args.search_workers)
        self.assertEqual(2, args.detail_workers)


if __name__ == "__main__":
    unittest.main()
