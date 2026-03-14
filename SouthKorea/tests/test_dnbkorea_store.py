import sys
import tempfile
import unittest
from pathlib import Path
import json


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbKoreaStoreTests(unittest.TestCase):
    def test_store_connection_configures_busy_timeout(self) -> None:
        from korea_crawler.dnb.store import DnbKoreaStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbKoreaStore(Path(tmp) / "store.db")
            try:
                timeout_ms = store._conn.execute("PRAGMA busy_timeout").fetchone()[0]
                self.assertEqual(30000, timeout_ms)
            finally:
                store.close()

    def test_claim_segment_is_exclusive(self) -> None:
        from korea_crawler.dnb.store import DnbKoreaStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbKoreaStore(Path(tmp) / "store.db")
            store.upsert_leaf_segment(
                segment_id="construction|kr|seoul|",
                industry_path="construction",
                country_iso_two_code="kr",
                region_name="seoul",
                city_name="",
                expected_count=100,
            )
            store.upsert_leaf_segment(
                segment_id="construction|kr|busan|",
                industry_path="construction",
                country_iso_two_code="kr",
                region_name="busan",
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
        from korea_crawler.dnb.store import DnbKoreaStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbKoreaStore(Path(tmp) / "store.db")
            store.upsert_leaf_segment(
                segment_id="construction|kr|seoul|",
                industry_path="construction",
                country_iso_two_code="kr",
                region_name="seoul",
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

    def test_ensure_discovery_seeds_is_idempotent_for_full_catalog(self) -> None:
        from korea_crawler.dnb.catalog import build_country_industry_segments
        from korea_crawler.dnb.store import DnbKoreaStore

        with tempfile.TemporaryDirectory() as tmp:
            store = DnbKoreaStore(Path(tmp) / "store.db")
            try:
                rows = [
                    {
                        "segment_id": segment.segment_id,
                        "industry_path": segment.industry_path,
                        "country_iso_two_code": segment.country_iso_two_code,
                        "region_name": segment.region_name,
                        "city_name": segment.city_name,
                        "expected_count": 0,
                    }
                    for segment in build_country_industry_segments("kr")
                ]
                store.ensure_discovery_seeds(rows)
                store.ensure_discovery_seeds(rows)

                queued = store._scalar("SELECT COUNT(*) FROM dnb_discovery_queue")
                self.assertEqual(327, queued)
            finally:
                store.close()

    def test_store_reopen_prunes_invalid_korean_segments(self) -> None:
        from korea_crawler.dnb.store import DnbKoreaStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "store.db"
            store = DnbKoreaStore(db_path)
            store.upsert_leaf_segment(
                segment_id="construction|kr|nairobi|",
                industry_path="construction",
                country_iso_two_code="kr",
                region_name="nairobi",
                city_name="",
                expected_count=10,
            )
            store.upsert_leaf_segment(
                segment_id="construction|kr|gyeonggi|",
                industry_path="construction",
                country_iso_two_code="kr",
                region_name="gyeonggi",
                city_name="",
                expected_count=100,
            )
            store.enqueue_discovery_node(
                segment_id="construction|kr|nairobi|",
                industry_path="construction",
                country_iso_two_code="kr",
                region_name="nairobi",
                city_name="",
                expected_count=10,
            )
            store.close()

            reopened = DnbKoreaStore(db_path)
            try:
                stats = reopened.get_stats()
                self.assertEqual(1, stats["segments_total"])
                claimed = reopened.claim_segment(50)
                self.assertIsNotNone(claimed)
                self.assertEqual("construction|kr|gyeonggi|", claimed.segment_id)
                self.assertTrue(reopened.discovery_done() or reopened.has_discovery_work() is False)
            finally:
                reopened.close()

    def test_export_snapshots_filters_dirty_gmap_domains(self) -> None:
        from korea_crawler.dnb.store import DnbKoreaStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = DnbKoreaStore(root / "store.db")
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
        from korea_crawler.dnb.store import DnbKoreaStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = DnbKoreaStore(root / "store.db")
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
            finally:
                store.close()

    def test_store_reopen_cleans_dirty_gmap_name_back_to_english(self) -> None:
        from korea_crawler.dnb.store import DnbKoreaStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "store.db"
            store = DnbKoreaStore(db_path)
            try:
                store._upsert_company(
                    duns="D3",
                    company_name_en_dnb="Clean English Name Co., Ltd.",
                    key_principal="홍길동",
                    detail_done=True,
                )
                store._conn.execute(
                    """
                    UPDATE companies
                    SET company_name_en_gmap = ?, company_name_resolved = ?
                    WHERE duns = ?
                    """,
                    ("현재 게시가 사용 중지됨", "현재 게시가 사용 중지됨", "D3"),
                )
                store._conn.commit()
            finally:
                store.close()

            reopened = DnbKoreaStore(db_path)
            try:
                row = reopened.get_company("D3")
                self.assertEqual("", row["company_name_en_gmap"])
                self.assertEqual("Clean English Name Co., Ltd.", row["company_name_resolved"])
            finally:
                reopened.close()


if __name__ == "__main__":
    unittest.main()
