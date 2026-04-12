from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_DIR = ROOT.parent / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from england_crawler.sites.companyname.store import CompanyNameStore


class EnglandStoreGuardrailTests(unittest.TestCase):
    def test_complete_gmap_task_blocks_portal_homepage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "store.db"
            store = CompanyNameStore(db_path)
            try:
                store.seed_companies(["Acme Limited"])
                task = store.claim_gmap_task()
                assert task is not None

                store.complete_gmap_task(
                    task.orgnr,
                    "https://getyourguide.com",
                    "",
                    "",
                    "https://getyourguide.com",
                )

                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT homepage, domain, evidence_url, gmap_status, firecrawl_status FROM companies WHERE orgnr = ?",
                    (task.orgnr,),
                ).fetchone()
                conn.close()

                self.assertEqual("", row[0])
                self.assertEqual("", row[1])
                self.assertEqual("", row[2])
                self.assertEqual("done", row[3])
                self.assertEqual("skip", row[4])
            finally:
                store.close()

    def test_complete_gmap_task_blocks_companies_house_homepage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "store.db"
            store = CompanyNameStore(db_path)
            try:
                store.seed_companies(["Registry Limited"])
                task = store.claim_gmap_task()
                assert task is not None

                store.complete_gmap_task(
                    task.orgnr,
                    "https://find-and-update.company-information.service.gov.uk/company/01234567",
                    "",
                    "",
                    "https://find-and-update.company-information.service.gov.uk/company/01234567",
                )

                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT homepage, domain, evidence_url, gmap_status, firecrawl_status FROM companies WHERE orgnr = ?",
                    (task.orgnr,),
                ).fetchone()
                conn.close()

                self.assertEqual("", row[0])
                self.assertEqual("", row[1])
                self.assertEqual("", row[2])
                self.assertEqual("done", row[3])
                self.assertEqual("skip", row[4])
            finally:
                store.close()

    def test_repair_dirty_homepages_requeues_gmap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "store.db"
            store = CompanyNameStore(db_path)
            try:
                store.seed_companies(["Dirty Limited"])
                conn = sqlite3.connect(db_path)
                orgnr = conn.execute("SELECT orgnr FROM companies WHERE company_name = 'Dirty Limited'").fetchone()[0]
                conn.execute(
                    """
                    UPDATE companies
                    SET homepage = ?, domain = ?, gmap_status = 'done', firecrawl_status = 'pending', evidence_url = ?
                    WHERE orgnr = ?
                    """,
                    ("https://i.giatamedia.com/m.php?m=abc", "i.giatamedia.com", "https://i.giatamedia.com/m.php?m=abc", orgnr),
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO firecrawl_queue(orgnr, status, retries, next_run_at, last_error, updated_at)
                    VALUES(?, 'pending', 0, '2026-04-12T00:00:00Z', '', '2026-04-12T00:00:00Z')
                    """,
                    (orgnr,),
                )
                conn.commit()
                conn.close()

                repaired = store.repair_dirty_homepages()

                conn = sqlite3.connect(db_path)
                company_row = conn.execute(
                    "SELECT homepage, domain, evidence_url, gmap_status, firecrawl_status FROM companies WHERE orgnr = ?",
                    (orgnr,),
                ).fetchone()
                queue_row = conn.execute(
                    "SELECT status, retries FROM gmap_queue WHERE orgnr = ?",
                    (orgnr,),
                ).fetchone()
                firecrawl_row = conn.execute(
                    "SELECT COUNT(*) FROM firecrawl_queue WHERE orgnr = ?",
                    (orgnr,),
                ).fetchone()
                conn.close()

                self.assertEqual(1, repaired)
                self.assertEqual("", company_row[0])
                self.assertEqual("", company_row[1])
                self.assertEqual("", company_row[2])
                self.assertEqual("pending", company_row[3])
                self.assertEqual("", company_row[4])
                self.assertEqual("pending", queue_row[0])
                self.assertEqual(0, queue_row[1])
                self.assertEqual(0, firecrawl_row[0])
            finally:
                store.close()

    def test_repair_companies_house_homepages_requeues_gmap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "store.db"
            store = CompanyNameStore(db_path)
            try:
                store.seed_companies(["Registry Dirty Limited"])
                conn = sqlite3.connect(db_path)
                orgnr = conn.execute(
                    "SELECT orgnr FROM companies WHERE company_name = 'Registry Dirty Limited'"
                ).fetchone()[0]
                dirty_url = "https://find-and-update.company-information.service.gov.uk/company/01234567"
                conn.execute(
                    """
                    UPDATE companies
                    SET homepage = ?, domain = ?, gmap_status = 'done', firecrawl_status = 'pending', evidence_url = ?
                    WHERE orgnr = ?
                    """,
                    (dirty_url, "find-and-update.company-information.service.gov.uk", dirty_url, orgnr),
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO firecrawl_queue(orgnr, status, retries, next_run_at, last_error, updated_at)
                    VALUES(?, 'pending', 0, '2026-04-12T00:00:00Z', '', '2026-04-12T00:00:00Z')
                    """,
                    (orgnr,),
                )
                conn.commit()
                conn.close()

                repaired = store.repair_dirty_homepages()

                conn = sqlite3.connect(db_path)
                company_row = conn.execute(
                    "SELECT homepage, domain, evidence_url, gmap_status, firecrawl_status FROM companies WHERE orgnr = ?",
                    (orgnr,),
                ).fetchone()
                queue_row = conn.execute(
                    "SELECT status, retries FROM gmap_queue WHERE orgnr = ?",
                    (orgnr,),
                ).fetchone()
                firecrawl_row = conn.execute(
                    "SELECT COUNT(*) FROM firecrawl_queue WHERE orgnr = ?",
                    (orgnr,),
                ).fetchone()
                conn.close()

                self.assertEqual(1, repaired)
                self.assertEqual("", company_row[0])
                self.assertEqual("", company_row[1])
                self.assertEqual("", company_row[2])
                self.assertEqual("pending", company_row[3])
                self.assertEqual("", company_row[4])
                self.assertEqual("pending", queue_row[0])
                self.assertEqual(0, queue_row[1])
                self.assertEqual(0, firecrawl_row[0])
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
