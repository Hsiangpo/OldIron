import sys
import tempfile
import unittest
from pathlib import Path
import json


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbEnglandStoreTests(unittest.TestCase):
    def test_snapshot_writer_tolerates_invalid_unicode(self) -> None:
        from england_crawler.dnb.runtime.snapshot_export import _write_json_line

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rows.jsonl"
            with path.open("wb") as fp:
                _write_json_line(
                    fp,
                    {
                        "company_name": "Bad\ud800Name",
                        "ceo": "Alice",
                        "homepage": "https://example.com",
                        "emails": ["boss@example.com"],
                    },
                )

            text = path.read_text(encoding="utf-8")
            self.assertIn("Bad", text)
            self.assertIn("boss@example.com", text)

    def test_queue_claim_queries_use_indexes(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbEnglandStore(Path(tmp) / "store.db")
            try:
                store._upsert_company(
                    duns="D1",
                    company_name_en_dnb="Foo Ltd",
                    company_name_url="foo.1",
                    detail_done=True,
                )
                store.enqueue_gmap_task("D1")
                store.enqueue_snov_task("D1")

                plans = {
                    "gmap": store._conn.execute(
                        """
                        EXPLAIN QUERY PLAN
                        SELECT q.duns, q.retries, c.company_name_en_dnb
                        FROM gmap_queue q
                        JOIN companies c ON c.duns = q.duns
                        WHERE q.status = 'pending' AND q.next_run_at <= ?
                        ORDER BY q.next_run_at ASC, q.updated_at ASC
                        LIMIT 1
                        """,
                        ("9999-12-31T23:59:59Z",),
                    ).fetchall(),
                    "snov": store._conn.execute(
                        """
                        EXPLAIN QUERY PLAN
                        SELECT q.duns, q.retries, c.company_name_en_dnb
                        FROM snov_queue q
                        JOIN companies c ON c.duns = q.duns
                        WHERE q.status = 'pending' AND q.next_run_at <= ?
                        ORDER BY q.next_run_at ASC, q.updated_at ASC
                        LIMIT 1
                        """,
                        ("9999-12-31T23:59:59Z",),
                    ).fetchall(),
                }

                for rows in plans.values():
                    details = " | ".join(str(row[3]) for row in rows)
                    self.assertNotIn("SCAN q", details)
                    self.assertNotIn("USE TEMP B-TREE FOR ORDER BY", details)
            finally:
                store.close()

    def test_store_connection_configures_busy_timeout(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbEnglandStore(Path(tmp) / "store.db")
            try:
                timeout_ms = store._conn.execute("PRAGMA busy_timeout").fetchone()[0]
                self.assertEqual(30000, timeout_ms)
            finally:
                store.close()

    def test_claim_segment_is_exclusive(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbEnglandStore(Path(tmp) / "store.db")
            store.upsert_leaf_segment(
                segment_id="construction|gb|na|",
                industry_path="construction",
                country_iso_two_code="gb",
                region_name="na",
                city_name="",
                expected_count=100,
            )
            store.upsert_leaf_segment(
                segment_id="foundation_structure_and_building_exterior_contractors|gb||",
                industry_path="foundation_structure_and_building_exterior_contractors",
                country_iso_two_code="gb",
                region_name="",
                city_name="",
                expected_count=80,
            )

            first = store.claim_segment(50)
            second = store.claim_segment(50)

            self.assertIsNotNone(first)
            self.assertIsNotNone(second)
            self.assertNotEqual(first.segment_id, second.segment_id)
            store.close()

    def test_reset_segment_requeues_failed_segment(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbEnglandStore(Path(tmp) / "store.db")
            store.upsert_leaf_segment(
                segment_id="construction|gb|na|",
                industry_path="construction",
                country_iso_two_code="gb",
                region_name="na",
                city_name="",
                expected_count=100,
            )

            claimed = store.claim_segment(50)
            self.assertIsNotNone(claimed)
            store.reset_segment(claimed.segment_id)
            claimed_again = store.claim_segment(50)

            self.assertIsNotNone(claimed_again)
            self.assertEqual(claimed.segment_id, claimed_again.segment_id)
            store.close()

    def test_store_reopen_keeps_generic_segments(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "store.db"
            store = DnbEnglandStore(db_path)
            store.upsert_leaf_segment(
                segment_id="construction|gb|na|nottinghamshire",
                industry_path="construction",
                country_iso_two_code="gb",
                region_name="na",
                city_name="nottinghamshire",
                expected_count=10,
            )
            store.upsert_leaf_segment(
                segment_id="foundation_structure_and_building_exterior_contractors|gb||",
                industry_path="foundation_structure_and_building_exterior_contractors",
                country_iso_two_code="gb",
                region_name="",
                city_name="",
                expected_count=100,
            )
            store.enqueue_discovery_node(
                segment_id="construction|gb|na|nottinghamshire",
                industry_path="construction",
                country_iso_two_code="gb",
                region_name="na",
                city_name="nottinghamshire",
                expected_count=10,
            )
            store.close()

            reopened = DnbEnglandStore(db_path)
            try:
                stats = reopened.get_stats()
                self.assertEqual(2, stats["segments_total"])
                claimed = reopened.claim_segment(50)
                self.assertIsNotNone(claimed)
                self.assertIn(
                    claimed.segment_id,
                    {
                        "construction|gb|na|nottinghamshire",
                        "foundation_structure_and_building_exterior_contractors|gb||",
                    },
                )
            finally:
                reopened.close()

    def test_ensure_discovery_seed_repairs_legacy_non_uk_seed(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbEnglandStore(Path(tmp) / "store.db")
            try:
                now = "2026-03-11T00:00:00Z"
                store._conn.execute(
                    """
                    INSERT INTO dnb_discovery_queue(
                        segment_id, industry_path, country_iso_two_code, region_name, city_name, expected_count, status, updated_at
                    ) VALUES(?, 'construction', 'kr', '', '', 1, 'pending', ?)
                    """,
                    ("construction|gb||", now),
                )
                store._conn.commit()

                store.ensure_discovery_seed("construction|gb||", 461092)
                row = store._conn.execute(
                    """
                    SELECT industry_path, country_iso_two_code, expected_count
                    FROM dnb_discovery_queue
                    WHERE segment_id = ?
                    """,
                    ("construction|gb||",),
                ).fetchone()

                self.assertEqual("construction", row["industry_path"])
                self.assertEqual("gb", row["country_iso_two_code"])
                self.assertEqual(461092, row["expected_count"])
            finally:
                store.close()

    def test_ensure_discovery_seeds_adds_missing_catalog_segments(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbEnglandStore(Path(tmp) / "store.db")
            try:
                store.ensure_discovery_seed("construction|gb||", 461092)
                store.ensure_discovery_seeds(
                    [
                        {
                            "segment_id": "construction|gb||",
                            "industry_path": "construction",
                            "country_iso_two_code": "gb",
                            "region_name": "",
                            "city_name": "",
                            "expected_count": 461092,
                        },
                        {
                            "segment_id": "general_medical_and_surgical_hospitals|gb||",
                            "industry_path": "general_medical_and_surgical_hospitals",
                            "country_iso_two_code": "gb",
                            "region_name": "",
                            "city_name": "",
                            "expected_count": 0,
                        },
                    ]
                )

                self.assertEqual(2, store._scalar("SELECT COUNT(*) FROM dnb_discovery_queue"))
            finally:
                store.close()

    def test_claim_segment_caps_pages_to_visible_limit(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbEnglandStore(Path(tmp) / "store.db")
            try:
                store.upsert_leaf_segment(
                    segment_id="foundation_structure_and_building_exterior_contractors|gb||",
                    industry_path="foundation_structure_and_building_exterior_contractors",
                    country_iso_two_code="gb",
                    region_name="",
                    city_name="",
                    expected_count=43414,
                )
                claimed = store.claim_segment(50, 20)

                self.assertIsNotNone(claimed)
                self.assertEqual(20, claimed.total_pages)
            finally:
                store.close()

    def test_export_snapshots_filters_dirty_gmap_domains(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = DnbEnglandStore(root / "store.db")
            try:
                store._upsert_company(
                    duns="D1",
                    company_name_en_dnb="Sujeong Industrial Development Co., Ltd.",
                    key_principal="홍길동",
                    dnb_website="",
                    detail_done=True,
                )
                store.mark_gmap_done(
                    duns="D1",
                    website="https://ko.wikipedia.org/wiki/%EC%88%98%EC%A0%95%EA%B5%AC",
                    source="gmap",
                    company_name_local_gmap="수정구",
                    phone="",
                )
                store.mark_snov_done(duns="D1", emails=["bad@example.com"])
                store.export_jsonl_snapshots(root)
                rows = [
                    json.loads(line)
                    for line in (root / "final_companies.jsonl").read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]
                self.assertEqual([], rows)
            finally:
                store.close()

    def test_mark_gmap_done_keeps_gmap_website_without_korean_name(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = DnbEnglandStore(root / "store.db")
            try:
                store._upsert_company(
                    duns="D2",
                    company_name_en_dnb="World Vision Co., Ltd.",
                    key_principal="홍길동",
                    dnb_website="",
                    detail_done=True,
                )
                store.mark_gmap_done(
                    duns="D2",
                    website="http://www.worldvision.org.hk",
                    source="gmap",
                    company_name_local_gmap="",
                    phone="",
                )
                row = store.get_company("D2")
                self.assertEqual("http://www.worldvision.org.hk", row["website"])
                self.assertEqual("gmap", row["website_source"])
                self.assertEqual("", row["company_name_en_gmap"])
                self.assertEqual("World Vision Co., Ltd.", row["company_name_resolved"])
                self.assertEqual(0, store._scalar("SELECT COUNT(*) FROM site_queue"))
            finally:
                store.close()

    def test_foreign_country_counts_detects_non_uk_rows(self) -> None:
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = DnbEnglandStore(root / "store.db")
            try:
                store._upsert_company(
                    duns="D3",
                    company_name_en_dnb="Legacy Korea Co., Ltd.",
                    key_principal="",
                    address="",
                    city="Pocheon",
                    region="Gyeonggi",
                    country="Republic Of Korea",
                    detail_done=False,
                )

                self.assertEqual(
                    [("Republic Of Korea", 1)],
                    store.foreign_country_counts("United Kingdom"),
                )
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
