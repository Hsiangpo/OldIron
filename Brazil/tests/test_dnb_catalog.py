"""DNB 分类清单测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from brazil_crawler.sites.dnb.catalog import LEGACY_SUBCATEGORY_SLUGS
from brazil_crawler.sites.dnb.catalog import TOP_LEVEL_CATEGORY_SLUGS
from brazil_crawler.sites.dnb.catalog import build_initial_segments


class DnbCatalogTests(unittest.TestCase):
    def test_top_level_categories_keep_twenty_seed_entries(self) -> None:
        self.assertEqual(20, len(TOP_LEVEL_CATEGORY_SLUGS))

    def test_legacy_subcategory_seed_is_large_and_unique(self) -> None:
        self.assertGreaterEqual(len(LEGACY_SUBCATEGORY_SLUGS), 289)
        self.assertEqual(len(LEGACY_SUBCATEGORY_SLUGS), len(set(LEGACY_SUBCATEGORY_SLUGS)))

    def test_initial_segments_are_seeded_from_subcategories(self) -> None:
        segments = build_initial_segments()
        self.assertGreaterEqual(len(segments), 289)
        self.assertEqual("br", segments[0]["country_iso_two_code"])
        self.assertEqual("subcategory", segments[0]["segment_type"])

    def test_initial_segments_can_be_limited_to_specific_industries(self) -> None:
        segments = build_initial_segments(industry_paths=["beverage_manufacturing", "construction"])
        self.assertEqual(2, len(segments))
        self.assertEqual("beverage_manufacturing|br||", segments[0]["segment_id"])
        self.assertEqual("construction|br||", segments[1]["segment_id"])


if __name__ == "__main__":
    unittest.main()
