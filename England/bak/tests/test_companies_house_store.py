import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class CompaniesHouseStoreTests(unittest.TestCase):
    def test_snapshot_writer_tolerates_invalid_unicode(self) -> None:
        from england_crawler.companies_house.snapshot_export import _write_jsonl

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rows.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "company_name": "Bad\ud800Name",
                        "ceo": "Alice",
                        "homepage": "https://example.com",
                        "emails": ["boss@example.com"],
                    }
                ],
            )

            text = path.read_text(encoding="utf-8")
            self.assertIn("Bad", text)
            self.assertIn("boss@example.com", text)

    def test_claim_query_uses_queue_index(self) -> None:
        from england_crawler.companies_house.store import CompaniesHouseStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CompaniesHouseStore(Path(tmp) / "store.db")
            try:
                store.import_company_names(
                    [f"COMPANY {index} LTD" for index in range(50)]
                )
                rows = store._conn.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT q.comp_id, q.retries, c.company_name
                    FROM ch_queue q
                    JOIN companies c ON c.comp_id = q.comp_id
                    WHERE q.status = 'pending' AND q.next_run_at <= ?
                    ORDER BY q.next_run_at ASC, q.updated_at ASC
                    LIMIT 1
                    """,
                    ("9999-12-31T23:59:59Z",),
                ).fetchall()
                details = " | ".join(str(row[3]) for row in rows)

                self.assertNotIn("SCAN q", details)
                self.assertNotIn("USE TEMP B-TREE FOR ORDER BY", details)
            finally:
                store.close()

    def test_source_loaded_scope_distinguishes_limited_and_full_import(self) -> None:
        from england_crawler.companies_house.store import CompaniesHouseStore

        with tempfile.TemporaryDirectory() as tmp:
            source_path = Path(tmp) / "companies.xlsx"
            source_path.write_text("stub", encoding="utf-8")
            store = CompaniesHouseStore(Path(tmp) / "store.db")
            try:
                store.mark_source_loaded(
                    source_path,
                    fingerprint="abc",
                    total_rows=1,
                    scope="limit:1",
                )

                self.assertTrue(
                    store.source_is_loaded(source_path, "abc", scope="limit:1")
                )
                self.assertFalse(
                    store.source_is_loaded(source_path, "abc", scope="full")
                )
            finally:
                store.close()

    def test_mark_ch_and_gmap_done_enqueues_snov(self) -> None:
        from england_crawler.companies_house.store import CompaniesHouseStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CompaniesHouseStore(Path(tmp) / "store.db")
            try:
                store.import_company_names(["ZZZ DEVELOPMENTS LTD"])
                task = store.claim_ch_task()
                self.assertIsNotNone(task)
                store.mark_ch_done(
                    comp_id=task.comp_id,
                    company_number="00000002",
                    company_status="00000002 - Incorporated on 20 April 2020",
                    ceo="CHARRO, Jorge Manrique",
                )
                self.assertIsNone(store.claim_snov_task())

                gmap_task = store.claim_gmap_task()
                self.assertIsNotNone(gmap_task)
                store.mark_gmap_done(
                    comp_id=gmap_task.comp_id,
                    homepage="https://zzzdevelopments.example.com",
                    phone="+44 20 1234 5678",
                )

                snov_task = store.claim_snov_task()
                self.assertIsNotNone(snov_task)
                self.assertEqual("zzzdevelopments.example.com", snov_task.domain)
            finally:
                store.close()

    def test_export_final_companies_requires_company_ceo_homepage_and_emails(self) -> None:
        from england_crawler.companies_house.store import CompaniesHouseStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = CompaniesHouseStore(root / "store.db")
            try:
                store.import_company_names(["ZZZ DEVELOPMENTS LTD", "TPL SERVICES LTD"])
                first = store.claim_ch_task()
                second = store.claim_ch_task()
                self.assertIsNotNone(first)
                self.assertIsNotNone(second)
                store.mark_ch_done(
                    comp_id=first.comp_id,
                    company_number="00000002",
                    company_status="00000002 - Incorporated on 20 April 2020",
                    ceo="CHARRO, Jorge Manrique",
                )
                store.mark_ch_done(
                    comp_id=second.comp_id,
                    company_number="00000003",
                    company_status="00000003 - Incorporated on 20 April 2021",
                    ceo="",
                )
                first_gmap = store.claim_gmap_task()
                second_gmap = store.claim_gmap_task()
                self.assertIsNotNone(first_gmap)
                self.assertIsNotNone(second_gmap)
                store.mark_gmap_done(
                    comp_id=first_gmap.comp_id,
                    homepage="https://zzzdevelopments.example.com",
                    phone="",
                )
                store.mark_gmap_done(
                    comp_id=second_gmap.comp_id,
                    homepage="https://tplservices.example.com",
                    phone="",
                )
                first_snov = store.claim_snov_task()
                self.assertIsNotNone(first_snov)
                store.mark_snov_done(comp_id=first_snov.comp_id, emails=["boss@example.com"])
                store.export_jsonl_snapshots(root)

                rows = [
                    json.loads(line)
                    for line in (root / "final_companies.jsonl").read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]
                self.assertEqual(1, len(rows))
                self.assertEqual("ZZZ DEVELOPMENTS LTD", rows[0]["company_name"])
                self.assertEqual(["boss@example.com"], rows[0]["emails"])
            finally:
                store.close()

    def test_get_stats_returns_done_and_total_counts(self) -> None:
        from england_crawler.companies_house.store import CompaniesHouseStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CompaniesHouseStore(Path(tmp) / "store.db")
            try:
                store.import_company_names(["AAA LTD", "BBB LTD"])
                first = store.claim_ch_task()
                self.assertIsNotNone(first)
                store.mark_ch_done(
                    comp_id=first.comp_id,
                    company_number="12345678",
                    company_status="12345678 - Incorporated on 1 January 2020",
                    ceo="ALICE",
                )

                stats = store.get_stats()

                self.assertEqual(2, stats["ch_total"])
                self.assertEqual(1, stats["ch_done"])
                self.assertEqual(1, stats["ch_pending"])
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
