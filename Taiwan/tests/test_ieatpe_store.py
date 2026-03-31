from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from taiwan_crawler.sites.ieatpe.store import IeatpeStore  # noqa: E402


class IeatpeStoreTests(unittest.TestCase):
    def test_seed_claim_and_complete_letter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = IeatpeStore(Path(tmp) / "store.db")
            try:
                store.seed_letters(["A", "B"])
                task = store.claim_letter_task()
                assert task is not None
                self.assertIn(task["letter"], {"A", "B"})
                store.mark_letter_done(task["letter"], result_count=10)
                progress = store.get_progress()
                self.assertEqual(1, progress["letters_done"])
                self.assertEqual(1, progress["letters_pending"])
            finally:
                store.close()

    def test_upsert_company_schedules_detail_and_merges_duplicate_member(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = IeatpeStore(Path(tmp) / "store.db")
            try:
                store.upsert_company_summary(
                    {
                        "member_id": "00007",
                        "company_name": "合發貿易股份有限公司",
                        "representative": "徐季安",
                        "address": "臺北市大安區",
                        "capital": "19,000,000",
                    },
                    source_letter="A",
                )
                store.upsert_company_summary(
                    {
                        "member_id": "00007",
                        "company_name": "合發貿易股份有限公司",
                        "representative": "徐季安",
                        "address": "臺北市大安區忠孝東路",
                        "capital": "19,000,000",
                    },
                    source_letter="B",
                )
                task = store.claim_detail_task()
                assert task is not None
                self.assertEqual("00007", task["member_id"])
                self.assertEqual(1, store.get_progress()["companies_total"])
            finally:
                store.close()

    def test_save_detail_result_updates_contact_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = IeatpeStore(Path(tmp) / "store.db")
            try:
                store.upsert_company_summary(
                    {
                        "member_id": "00007",
                        "company_name": "合發貿易股份有限公司",
                        "representative": "徐季安",
                        "address": "臺北市大安區",
                        "capital": "19,000,000",
                    },
                    source_letter="A",
                )
                store.save_detail_result(
                    "00007",
                    {
                        "company_name": "合發貿易股份有限公司",
                        "representative": "徐季安",
                        "website": "",
                        "phone": "(02)27407278",
                        "address": "臺北市大安區忠孝東路4段223巷49弄2號1樓",
                        "emails": "prgrtp@yahoo.com.tw",
                        "detail_url": "https://www.ieatpe.org.tw/qry/query.aspx",
                    },
                )
                row = store.get_company("00007")
                assert row is not None
                self.assertEqual("prgrtp@yahoo.com.tw", row["emails"])
                self.assertEqual("done", row["detail_status"])
            finally:
                store.close()

    def test_requeue_stale_running_tasks_resets_running_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = IeatpeStore(Path(tmp) / "store.db")
            try:
                store.seed_letters(["A"])
                task = store.claim_letter_task()
                assert task is not None
                store.upsert_company_summary(
                    {
                        "member_id": "00007",
                        "company_name": "合發貿易股份有限公司",
                        "representative": "徐季安",
                        "address": "臺北市大安區",
                        "capital": "19,000,000",
                    },
                    source_letter="A",
                )
                detail = store.claim_detail_task()
                assert detail is not None
                conn = store._conn()
                conn.execute("UPDATE letters SET updated_at = 1 WHERE letter = 'A'")
                conn.execute("UPDATE companies SET updated_at = 1 WHERE member_id = '00007'")
                conn.commit()
                recovered = store.requeue_stale_running_tasks(older_than_seconds=0)
                self.assertEqual(1, recovered["letters"])
                self.assertEqual(1, recovered["details"])
                progress = store.get_progress()
                self.assertEqual(1, progress["letters_pending"])
                self.assertEqual(1, progress["details_pending"])
            finally:
                store.close()

    def test_requeue_failed_detail_tasks_resets_failed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = IeatpeStore(Path(tmp) / "store.db")
            try:
                store.upsert_company_summary(
                    {
                        "member_id": "17263",
                        "company_name": "台灣阿爾卑斯電子股份有限公司",
                        "representative": "小熊貴博",
                        "address": "臺北市中山區",
                        "capital": "8,000,000",
                    },
                    source_letter="A",
                )
                detail = store.claim_detail_task()
                assert detail is not None
                store.mark_detail_failed("17263")
                count = store.requeue_failed_detail_tasks()
                self.assertEqual(1, count)
                row = store.get_company("17263")
                assert row is not None
                self.assertEqual("pending", row["detail_status"])
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
