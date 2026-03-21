import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbEnglandDetailQueueTests(unittest.TestCase):
    def test_detail_queue_claim_query_uses_index(self) -> None:
        from england_crawler.dnb.runtime.detail_queue import DetailQueueStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "store.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE companies (
                    duns TEXT PRIMARY KEY,
                    company_name_en_dnb TEXT NOT NULL DEFAULT '',
                    company_name_url TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    region TEXT NOT NULL DEFAULT '',
                    country TEXT NOT NULL DEFAULT '',
                    postal_code TEXT NOT NULL DEFAULT '',
                    sales_revenue TEXT NOT NULL DEFAULT '',
                    detail_done INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO companies(duns, company_name_en_dnb, company_name_url, country, detail_done)
                VALUES ('D1', 'Foo Co., Ltd.', 'foo.1', 'United Kingdom', 0);
                """
            )
            conn.commit()
            conn.close()

            queue = DetailQueueStore(db_path)
            try:
                queue.enqueue("D1")
                rows = queue._conn.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT q.duns, q.retries, c.company_name_en_dnb
                    FROM detail_queue q
                    JOIN companies c ON c.duns = q.duns
                    WHERE q.status = 'pending' AND q.next_run_at <= ? AND c.detail_done = 0
                    ORDER BY q.next_run_at ASC, q.updated_at ASC
                    LIMIT 1
                    """,
                    ("9999-12-31T23:59:59Z",),
                ).fetchall()
                details = " | ".join(str(row[3]) for row in rows)
                self.assertNotIn("SCAN q", details)
                self.assertNotIn("USE TEMP B-TREE FOR ORDER BY", details)
            finally:
                queue.close()

    def test_sync_from_companies_enqueues_pending_details(self) -> None:
        from england_crawler.dnb.runtime.detail_queue import DetailQueueStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "store.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE companies (
                    duns TEXT PRIMARY KEY,
                    company_name_en_dnb TEXT NOT NULL DEFAULT '',
                    company_name_url TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    region TEXT NOT NULL DEFAULT '',
                    country TEXT NOT NULL DEFAULT '',
                    postal_code TEXT NOT NULL DEFAULT '',
                    sales_revenue TEXT NOT NULL DEFAULT '',
                    detail_done INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO companies(duns, company_name_en_dnb, company_name_url, country, detail_done)
                VALUES ('D1', 'Foo Co., Ltd.', 'foo.1', 'United Kingdom', 0);
                """
            )
            conn.commit()
            conn.close()

            queue = DetailQueueStore(db_path)
            try:
                queue.sync_from_companies()
                task = queue.claim()
                self.assertIsNotNone(task)
                self.assertEqual("D1", task.duns)
                self.assertEqual("foo.1", task.company_name_url)
            finally:
                queue.close()

    def test_detail_queue_and_store_writes_do_not_raise_lock_errors(self) -> None:
        from england_crawler.dnb.runtime.detail_queue import DetailQueueStore
        from england_crawler.dnb.store import DnbEnglandStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "store.db"
            store = DnbEnglandStore(db_path)
            queue = DetailQueueStore(db_path)
            errors: list[Exception] = []
            start = threading.Barrier(3)

            def _write_store() -> None:
                try:
                    start.wait()
                    for idx in range(20):
                        store.upsert_company_listing(
                            {
                                "duns": f"D{idx}",
                                "company_name_en_dnb": f"Foo {idx}",
                                "company_name_url": f"foo.{idx}",
                                "address": "",
                                "city": "Seoul",
                                "region": "Seoul",
                                "country": "United Kingdom",
                                "postal_code": "",
                                "sales_revenue": "",
                            }
                        )
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

            def _write_queue() -> None:
                try:
                    start.wait()
                    for idx in range(20):
                        queue.enqueue(f"D{idx}")
                        queue.sync_from_companies()
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

            t1 = threading.Thread(target=_write_store)
            t2 = threading.Thread(target=_write_queue)
            t1.start()
            t2.start()
            start.wait()
            t1.join()
            t2.join()
            try:
                self.assertEqual([], errors)
            finally:
                queue.close()
                store.close()

    def test_mark_failed_sets_failed_status_and_retry_count(self) -> None:
        from england_crawler.dnb.runtime.detail_queue import DetailQueueStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "store.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE companies (
                    duns TEXT PRIMARY KEY,
                    company_name_en_dnb TEXT NOT NULL DEFAULT '',
                    company_name_url TEXT NOT NULL DEFAULT '',
                    address TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    region TEXT NOT NULL DEFAULT '',
                    country TEXT NOT NULL DEFAULT '',
                    postal_code TEXT NOT NULL DEFAULT '',
                    sales_revenue TEXT NOT NULL DEFAULT '',
                    detail_done INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO companies(duns, company_name_en_dnb, company_name_url, country, detail_done)
                VALUES ('D1', 'Foo Co., Ltd.', 'foo.1', 'United Kingdom', 0);
                """
            )
            conn.commit()
            conn.close()

            queue = DetailQueueStore(db_path)
            try:
                queue.enqueue("D1")
                queue.mark_failed("D1", retries=8, error_text="boom")
                row = queue._conn.execute(
                    "SELECT status, retries, last_error FROM detail_queue WHERE duns = 'D1'"
                ).fetchone()
                self.assertEqual("failed", row["status"])
                self.assertEqual(8, row["retries"])
                self.assertEqual("boom", row["last_error"])
            finally:
                queue.close()


if __name__ == "__main__":
    unittest.main()
