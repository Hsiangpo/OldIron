"""DNB 存储重试测试。"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from brazil_crawler.sites.dnb.store import DnbBrStore
from brazil_crawler.sites.dnb.store import _clean_site_emails


class DnbStoreTests(unittest.TestCase):
    def test_clean_site_emails_keeps_email_but_strips_encoded_prefix(self) -> None:
        cleaned = _clean_site_emails(["05%7c02%7cdkelly@pretium.com"])
        self.assertEqual(["dkelly@pretium.com"], cleaned)

    def test_detail_task_becomes_failed_after_three_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            store.upsert_companies(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme",
                        "detail_url": "https://example.com/detail",
                        "address": "x",
                        "region": "y",
                        "city": "z",
                        "postal_code": "",
                        "industry_path": "construction",
                    }
                ]
            )
            store.enqueue_detail_tasks(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme",
                        "detail_url": "https://example.com/detail",
                    }
                ]
            )
            for _ in range(3):
                store.fail_detail_task("1")
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            row = conn.execute("SELECT status, retries FROM detail_queue WHERE duns = '1'").fetchone()
            conn.close()
            self.assertEqual("failed", row[0])
            self.assertEqual(3, row[1])

    def test_site_result_keeps_p1_representative_when_names_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            store.upsert_companies(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Inc",
                        "representative": "Jane Doe",
                        "website": "https://acme.example",
                        "detail_url": "https://example.com/detail",
                        "address": "Main St",
                        "region": "CA",
                        "city": "LA",
                        "postal_code": "1",
                        "industry_path": "construction",
                    }
                ]
            )
            store.complete_detail_task("1", "Jane Doe", "https://acme.example", "")
            store.complete_site_task(
                "1",
                "ACME, INC.",
                "",
                ["sales@acme.example"],
                "https://acme.example",
                "",
                "",
                "https://acme.example",
            )
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            row = conn.execute(
                "SELECT company_name, representative FROM final_companies WHERE duns = '1'"
            ).fetchone()
            conn.close()
            self.assertEqual("ACME, INC.", row[0])
            self.assertEqual("Jane Doe", row[1])

    def test_site_result_drops_p1_representative_when_names_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            store.upsert_companies(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Inc",
                        "representative": "Jane Doe",
                        "website": "https://acme.example",
                        "detail_url": "https://example.com/detail",
                        "address": "Main St",
                        "region": "CA",
                        "city": "LA",
                        "postal_code": "1",
                        "industry_path": "construction",
                    }
                ]
            )
            store.complete_detail_task("1", "Jane Doe", "https://acme.example", "")
            store.complete_site_task(
                "1",
                "Beta Holdings",
                "",
                ["ops@beta.example"],
                "https://beta.example",
                "",
                "",
                "https://beta.example",
            )
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            row = conn.execute(
                "SELECT company_name, representative, website FROM final_companies WHERE duns = '1'"
            ).fetchone()
            conn.close()
            self.assertIsNone(row)

    def test_requeue_empty_detail_tasks_reopens_old_empty_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            store.upsert_companies(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Inc",
                        "detail_url": "https://example.com/detail",
                        "address": "Main St",
                        "region": "CA",
                        "city": "LA",
                        "postal_code": "1",
                        "industry_path": "construction",
                    }
                ]
            )
            store.enqueue_detail_tasks(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Inc",
                        "detail_url": "https://example.com/detail",
                    }
                ]
            )
            store.complete_detail_task("1", "", "", "")
            repaired = store.requeue_empty_detail_tasks()
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            row = conn.execute(
                "SELECT detail_status FROM companies WHERE duns = '1'"
            ).fetchone()
            queue = conn.execute(
                "SELECT status, retries FROM detail_queue WHERE duns = '1'"
            ).fetchone()
            conn.close()
            self.assertEqual(1, repaired)
            self.assertEqual("pending", row[0])
            self.assertEqual("pending", queue[0])
            self.assertEqual(0, queue[1])

    def test_site_result_without_emails_does_not_delete_existing_final(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            store.upsert_companies(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Inc",
                        "representative": "Jane Doe",
                        "website": "https://acme.example",
                        "detail_url": "https://example.com/detail",
                        "address": "Main St",
                        "region": "CA",
                        "city": "LA",
                        "postal_code": "1",
                        "industry_path": "construction",
                    }
                ]
            )
            store.complete_detail_task("1", "Jane Doe", "https://acme.example", "")
            store.complete_site_task(
                "1",
                "Acme Inc",
                "Jane Doe",
                ["sales@acme.example"],
                "https://acme.example",
                "",
                "",
                "https://acme.example",
            )
            store.complete_detail_task("1", "Jane Doe", "https://acme.example", "")
            store.complete_site_task(
                "1",
                "Acme Inc",
                "Jane Doe",
                [],
                "https://acme.example",
                "",
                "",
                "https://acme.example",
            )
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            row = conn.execute(
                "SELECT company_name, representative, emails FROM final_companies WHERE duns = '1'"
            ).fetchone()
            conn.close()
            self.assertEqual(("Acme Inc", "Jane Doe", "sales@acme.example"), row)

    def test_site_result_merges_new_emails_into_existing_final(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            store.upsert_companies(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Inc",
                        "representative": "Jane Doe",
                        "website": "https://acme.example",
                        "detail_url": "https://example.com/detail",
                        "address": "Main St",
                        "region": "CA",
                        "city": "LA",
                        "postal_code": "1",
                        "industry_path": "construction",
                    }
                ]
            )
            store.complete_detail_task("1", "Jane Doe", "https://acme.example", "")
            store.complete_site_task(
                "1",
                "Acme Inc",
                "Jane Doe",
                ["sales@acme.example"],
                "https://acme.example",
                "",
                "",
                "https://acme.example",
            )
            store.complete_site_task(
                "1",
                "Acme Inc",
                "Jane Doe",
                ["info@acme.example"],
                "https://acme.example",
                "",
                "",
                "https://acme.example",
            )
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            row = conn.execute("SELECT emails FROM final_companies WHERE duns = '1'").fetchone()
            conn.close()
            self.assertEqual("sales@acme.example; info@acme.example", row[0])

    def test_requeue_stale_running_tasks_only_recovers_old_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            conn.executescript(
                """
                INSERT INTO dnb_segments (
                    segment_id, industry_path, country_iso_two_code, region_name, city_name,
                    expected_count, next_page, status, updated_at
                ) VALUES
                    ('old-seg', 'industry', 'br', '', '', 0, 1, 'running', '2026-03-31 00:00:00'),
                    ('new-seg', 'industry', 'br', '', '', 0, 1, 'running', '2099-03-31 00:00:00');
                INSERT INTO detail_queue (duns, detail_url, company_name, status, retries, updated_at) VALUES
                    ('old-detail', 'https://example.com/1', 'Old Detail', 'running', 0, '2026-03-31 00:00:00'),
                    ('new-detail', 'https://example.com/2', 'New Detail', 'running', 0, '2099-03-31 00:00:00');
                INSERT INTO gmap_queue (duns, company_name, address, region, city, status, retries, updated_at) VALUES
                    ('old-gmap', 'Old Gmap', '', '', '', 'running', 0, '2026-03-31 00:00:00'),
                    ('new-gmap', 'New Gmap', '', '', '', 'running', 0, '2099-03-31 00:00:00');
                INSERT INTO site_queue (duns, company_name, website, status, retries, updated_at) VALUES
                    ('old-site', 'Old Site', 'https://old.example', 'running', 0, '2026-03-31 00:00:00'),
                    ('new-site', 'New Site', 'https://new.example', 'running', 0, '2099-03-31 00:00:00');
                """
            )
            conn.commit()
            conn.close()

            recovered = store.requeue_stale_running_tasks(max_age_seconds=60.0)

            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            statuses = conn.execute(
                """
                SELECT 'detail', duns, status FROM detail_queue
                UNION ALL
                SELECT 'gmap', duns, status FROM gmap_queue
                UNION ALL
                SELECT 'site', duns, status FROM site_queue
                ORDER BY 1, 2
                """
            ).fetchall()
            segments = conn.execute(
                "SELECT segment_id, status FROM dnb_segments ORDER BY segment_id"
            ).fetchall()
            conn.close()
            self.assertEqual(4, recovered)
            self.assertEqual(
                [('new-seg', 'running'), ('old-seg', 'pending')],
                segments,
            )
            self.assertEqual(
                [
                    ('detail', 'new-detail', 'running'),
                    ('detail', 'old-detail', 'pending'),
                    ('gmap', 'new-gmap', 'running'),
                    ('gmap', 'old-gmap', 'pending'),
                    ('site', 'new-site', 'running'),
                    ('site', 'old-site', 'pending'),
                ],
                statuses,
            )

    def test_claim_site_task_allows_rep_missing_while_detail_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            conn.executescript(
                """
                INSERT INTO companies (
                    duns, company_name, representative, website, phone, address, region, city,
                    postal_code, detail_url, industry_path, detail_status, gmap_status, site_status, updated_at
                ) VALUES
                    ('1', 'Pending Detail Co', '', 'https://pending.example', '', 'A', 'B', 'C', '1', 'https://example.com/1', 'construction', 'pending', 'done', 'pending', '2026-03-31 00:00:00'),
                    ('2', 'Ready Detail Co', '', 'https://ready.example', '', 'A', 'B', 'C', '1', 'https://example.com/2', 'construction', 'done', 'done', 'pending', '2026-03-31 00:00:00');
                INSERT INTO site_queue (duns, company_name, website, status, retries, updated_at) VALUES
                    ('1', 'Pending Detail Co', 'https://pending.example', 'pending', 0, '2026-03-31 00:00:00'),
                    ('2', 'Ready Detail Co', 'https://ready.example', 'pending', 0, '2026-03-31 00:00:00');
                """
            )
            conn.commit()
            conn.close()

            task = store.claim_site_task()

            self.assertIsNotNone(task)
            assert task is not None
            self.assertEqual("1", task.duns)

    def test_claim_gmap_task_prioritizes_company_like_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            conn.executescript(
                """
                INSERT INTO companies (
                    duns, company_name, representative, website, phone, address, region, city,
                    postal_code, detail_url, industry_path, detail_status, gmap_status, site_status, updated_at
                ) VALUES
                    ('1', 'JOSE DA SILVA', '', '', '', 'Rua A', 'Bahia', 'Salvador', '1', 'https://example.com/1', 'construction', 'pending', 'pending', 'pending', '2026-03-31 00:00:00'),
                    ('2', 'POUSADA SOL NASCENTE LTDA', '', '', '', 'Rua B', 'Bahia', 'Salvador', '1', 'https://example.com/2', 'construction', 'pending', 'pending', 'pending', '2026-03-31 00:00:00');
                INSERT INTO gmap_queue (duns, company_name, address, region, city, status, retries, updated_at) VALUES
                    ('1', 'JOSE DA SILVA', 'Rua A', 'Bahia', 'Salvador', 'pending', 0, '2026-03-31 00:00:00'),
                    ('2', 'POUSADA SOL NASCENTE LTDA', 'Rua B', 'Bahia', 'Salvador', 'pending', 0, '2026-03-31 00:00:00');
                """
            )
            conn.commit()
            conn.close()

            task = store.claim_gmap_task()

            self.assertIsNotNone(task)
            assert task is not None
            self.assertEqual("2", task.duns)

    def test_complete_gmap_task_enqueues_site_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            store.upsert_companies(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Hotel Ltda",
                        "detail_url": "https://example.com/detail",
                        "address": "Rua X",
                        "region": "Bahia",
                        "city": "Salvador",
                        "postal_code": "1",
                        "industry_path": "accommodation_and_food_services",
                    }
                ]
            )
            store.complete_gmap_task("1", "https://acmehotel.com.br", "")
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            queue = conn.execute("SELECT duns, website, status FROM site_queue").fetchone()
            company = conn.execute("SELECT website, site_status FROM companies WHERE duns='1'").fetchone()
            conn.close()
            self.assertEqual(("1", "https://acmehotel.com.br", "pending"), queue)
            self.assertEqual(("https://acmehotel.com.br", "pending"), company)

    def test_fail_detail_task_enqueues_site_when_website_already_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            store.upsert_companies(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Hotel Ltda",
                        "detail_url": "https://example.com/detail",
                        "website": "https://acmehotel.com.br",
                        "address": "Rua X",
                        "region": "Bahia",
                        "city": "Salvador",
                        "postal_code": "1",
                        "industry_path": "accommodation_and_food_services",
                    }
                ]
            )
            store.enqueue_detail_tasks(
                [
                    {
                        "duns": "1",
                        "company_name": "Acme Hotel Ltda",
                        "detail_url": "https://example.com/detail",
                    }
                ]
            )
            for _ in range(3):
                store.fail_detail_task("1")
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            queue = conn.execute("SELECT duns, website, status FROM site_queue").fetchone()
            company = conn.execute("SELECT detail_status, site_status FROM companies WHERE duns='1'").fetchone()
            conn.close()
            self.assertEqual(("1", "https://acmehotel.com.br", "pending"), queue)
            self.assertEqual(("failed", "pending"), company)

    def test_purge_bad_websites_clears_dirty_gmap_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = DnbBrStore(Path(tmpdir) / "store.db")
            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            conn.executescript(
                """
                INSERT INTO companies (
                    duns, company_name, representative, website, phone, address, region, city,
                    postal_code, detail_url, industry_path, detail_status, gmap_status, site_status, updated_at
                ) VALUES
                    ('1', 'Hotel One', 'A', 'https://Booking.com', '', 'A', 'B', 'C', '1', 'https://example.com/detail1', 'accommodation_and_food_services', 'done', 'done', 'running', '2026-03-31 00:00:00'),
                    ('2', 'Hotel Two', 'B', 'https://media.staticontent.com/media/pictures/x', '', 'A', 'B', 'C', '1', 'https://example.com/detail2', 'accommodation_and_food_services', 'done', 'done', 'running', '2026-03-31 00:00:00'),
                    ('3', 'Normal Co', 'C', 'https://normal.example.com', '', 'A', 'B', 'C', '1', 'https://example.com/detail3', 'construction', 'done', 'done', 'done', '2026-03-31 00:00:00');
                INSERT INTO site_queue (duns, company_name, website, status, retries, updated_at) VALUES
                    ('1', 'Hotel One', 'https://Booking.com', 'running', 0, '2026-03-31 00:00:00'),
                    ('2', 'Hotel Two', 'https://media.staticontent.com/media/pictures/x', 'running', 0, '2026-03-31 00:00:00'),
                    ('3', 'Normal Co', 'https://normal.example.com', 'done', 0, '2026-03-31 00:00:00');
                INSERT INTO final_companies (duns, company_name, representative, emails, website, phone, address, evidence_url, updated_at) VALUES
                    ('1', 'Hotel One', 'A', 'a@hotel.com', 'https://Booking.com', '', '', 'https://Booking.com', '2026-03-31 00:00:00'),
                    ('2', 'Hotel Two', 'B', 'b@hotel.com', 'https://media.staticontent.com/media/pictures/x', '', '', 'https://media.staticontent.com/media/pictures/x', '2026-03-31 00:00:00'),
                    ('3', 'Normal Co', 'C', 'c@normal.com', 'https://normal.example.com', '', '', 'https://normal.example.com', '2026-03-31 00:00:00');
                """
            )
            conn.commit()
            conn.close()

            cleaned = store.purge_bad_websites()

            conn = sqlite3.connect(str(Path(tmpdir) / "store.db"))
            companies = conn.execute(
                "SELECT duns, website, gmap_status FROM companies ORDER BY duns"
            ).fetchall()
            final_rows = conn.execute(
                "SELECT duns, website FROM final_companies ORDER BY duns"
            ).fetchall()
            conn.close()

            self.assertEqual(2, cleaned)
            self.assertEqual([("1", "", "pending"), ("2", "", "pending"), ("3", "https://normal.example.com", "done")], companies)
            self.assertEqual([("3", "https://normal.example.com")], final_rows)


if __name__ == "__main__":
    unittest.main()
