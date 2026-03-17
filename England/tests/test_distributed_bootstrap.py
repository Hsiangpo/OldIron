import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DistributedBootstrapTests(unittest.TestCase):
    def test_bootstrap_companies_house_shards_splits_legacy_store(self) -> None:
        from england_crawler.companies_house.store import CompaniesHouseStore
        from england_crawler.distributed.bootstrap import bootstrap_companies_house_shards
        from england_crawler.distributed.ch_planner import plan_companies_house_shards

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workbook = Workbook()
            sheet = workbook.active
            sheet.append(["CompanyName"])
            sheet.append(["ALPHA LTD"])
            sheet.append(["BETA LTD"])
            sheet.append(["GAMMA LTD"])
            source = root / "英国.xlsx"
            workbook.save(source)

            legacy_db = root / "legacy-ch.db"
            store = CompaniesHouseStore(legacy_db)
            try:
                store.import_company_names(["ALPHA LTD", "BETA LTD", "GAMMA LTD"])
            finally:
                store.close()

            shard_dir = root / "distributed" / "ch"
            output_root = root / "runs"
            plan_companies_house_shards(source, shard_dir, shard_count=2)
            summary = bootstrap_companies_house_shards(legacy_db, shard_dir, output_root)

            self.assertEqual(2, summary["shard_count"])
            counts = []
            for shard_path in sorted(output_root.glob("ch-shard-*")):
                conn = sqlite3.connect(shard_path / "store.db")
                try:
                    counts.append(conn.execute("select count(*) from companies").fetchone()[0])
                finally:
                    conn.close()
            self.assertEqual(3, sum(counts))

    def test_bootstrap_dnb_shards_splits_legacy_store(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore
        from england_crawler.distributed.bootstrap import bootstrap_dnb_shards
        from england_crawler.distributed.dnb_planner import plan_dnb_shards

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy_db = root / "legacy-dnb.db"
            store = DnbEnglandStore(legacy_db)
            try:
                store.upsert_leaf_segment(
                    segment_id="accommodation_and_food_services|gb||",
                    industry_path="accommodation_and_food_services",
                    country_iso_two_code="gb",
                    region_name="",
                    city_name="",
                    expected_count=10,
                )
                store.upsert_leaf_segment(
                    segment_id="agriculture_forestry_fishing_and_hunting|gb||",
                    industry_path="agriculture_forestry_fishing_and_hunting",
                    country_iso_two_code="gb",
                    region_name="",
                    city_name="",
                    expected_count=10,
                )
                store._upsert_company(
                    duns="D1",
                    company_name_en_dnb="Alpha Ltd",
                    company_name_url="alpha.1",
                    key_principal="Alice",
                    detail_done=True,
                )
                store._upsert_company(
                    duns="D2",
                    company_name_en_dnb="Beta Ltd",
                    company_name_url="beta.1",
                    key_principal="Bob",
                    detail_done=True,
                )
            finally:
                store.close()

            shard_dir = root / "distributed" / "dnb"
            output_root = root / "runs"
            plan_dnb_shards(shard_dir, shard_count=2, country_iso_two_code="gb")
            summary = bootstrap_dnb_shards(legacy_db, shard_dir, output_root)

            self.assertEqual(2, summary["shard_count"])
            company_counts = []
            segment_counts = []
            for shard_path in sorted(output_root.glob("dnb-shard-*")):
                conn = sqlite3.connect(shard_path / "store.db")
                try:
                    company_counts.append(conn.execute("select count(*) from companies").fetchone()[0])
                    segment_counts.append(conn.execute("select count(*) from dnb_segments").fetchone()[0])
                finally:
                    conn.close()
            self.assertEqual(2, sum(company_counts))
            self.assertEqual(2, sum(segment_counts))


if __name__ == "__main__":
    unittest.main()
