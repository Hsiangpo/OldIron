"""DNB 巴西分片规划测试。"""

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

from brazil_crawler.sites.dnb.client import DnbListPage
from brazil_crawler.sites.dnb.pipeline import _build_child_segments
from brazil_crawler.sites.dnb.pipeline import _needs_geo_split
from brazil_crawler.sites.dnb.store import DnbSegmentTask


class DnbPipelineTests(unittest.TestCase):
    def test_country_segment_with_geos_splits_into_region_children(self) -> None:
        task = DnbSegmentTask(
            segment_id="software_publishers|br||",
            industry_path="software_publishers",
            country_iso_two_code="br",
            region_name="",
            city_name="",
            expected_count=0,
            next_page=1,
            status="pending",
            updated_at="2026-03-31 00:00:00",
        )
        result = DnbListPage(
            current_page=1,
            total_pages=20,
            page_size=50,
            country_name="Brazil",
            industry_name="Software Publishers",
            matched_count=49922,
            geos=[
                {"name": "Sao Paulo", "href": "br.sao_paulo", "quantity": 20116},
                {"name": "Minas Gerais", "href": "br.minas_gerais", "quantity": 4766},
            ],
            records=[],
        )
        self.assertTrue(_needs_geo_split(task, result, 20))
        children = _build_child_segments(task, result.geos)
        self.assertEqual("software_publishers|br|sao_paulo|", children[0]["segment_id"])
        self.assertEqual("software_publishers|br|minas_gerais|", children[1]["segment_id"])

    def test_region_segment_with_geos_splits_into_city_children(self) -> None:
        task = DnbSegmentTask(
            segment_id="software_publishers|br|sao_paulo|",
            industry_path="software_publishers",
            country_iso_two_code="br",
            region_name="sao_paulo",
            city_name="",
            expected_count=0,
            next_page=1,
            status="pending",
            updated_at="2026-03-31 00:00:00",
        )
        result = DnbListPage(
            current_page=1,
            total_pages=20,
            page_size=50,
            country_name="Brazil",
            industry_name="Software Publishers",
            matched_count=20116,
            geos=[
                {"name": "Sao Paulo", "href": "br.sao_paulo.sao_paulo", "quantity": 11494},
            ],
            records=[],
        )
        self.assertTrue(_needs_geo_split(task, result, 20))
        children = _build_child_segments(task, result.geos)
        self.assertEqual("software_publishers|br|sao_paulo|sao_paulo", children[0]["segment_id"])


if __name__ == "__main__":
    unittest.main()
