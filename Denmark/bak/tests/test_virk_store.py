from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class VirkStoreTests(unittest.TestCase):
    def test_detail_with_email_skips_gmap_and_firecrawl(self) -> None:
        from denmark_crawler.virk.models import VirkCompanyRecord
        from denmark_crawler.virk.models import VirkSearchCompany
        from denmark_crawler.virk.store import VirkDenmarkStore

        with tempfile.TemporaryDirectory() as tmp:
            store = VirkDenmarkStore(Path(tmp) / "store.db")
            try:
                store.ensure_search_seed()
                store.upsert_search_company(
                    VirkSearchCompany(
                        cvr="25297288",
                        company_name="Fjordvejen ApS",
                        emails=["finn@munkebokro.dk"],
                    )
                )
                store.upsert_detail_company(
                    VirkCompanyRecord(
                        cvr="25297288",
                        company_name="Fjordvejen ApS",
                        representative="Finn Egebjerg Rasmussen",
                        emails=["finn@munkebokro.dk"],
                    )
                )
                conn = sqlite3.connect(Path(tmp) / "store.db")
                cur = conn.cursor()
                final_count = cur.execute("select count(*) from final_companies").fetchone()[0]
                gmap_count = cur.execute("select count(*) from gmap_queue").fetchone()[0]
                firecrawl_count = cur.execute("select count(*) from firecrawl_queue").fetchone()[0]
                conn.close()
                self.assertEqual(1, final_count)
                self.assertEqual(0, gmap_count)
                self.assertEqual(0, firecrawl_count)
            finally:
                store.close()

    def test_expand_search_pages_from_known_total_restores_missing_pages(self) -> None:
        from denmark_crawler.virk.store import VirkDenmarkStore

        with tempfile.TemporaryDirectory() as tmp:
            store = VirkDenmarkStore(Path(tmp) / "store.db")
            try:
                store.ensure_search_seed()
                conn = sqlite3.connect(Path(tmp) / "store.db")
                cur = conn.cursor()
                cur.execute("DELETE FROM search_pages")
                for page_index in range(31):
                    cur.execute(
                        "INSERT INTO search_pages(page_index, total_hits, status, updated_at) VALUES(?, ?, 'done', '2026-03-19T00:00:00Z')",
                        (page_index, 853589),
                    )
                conn.commit()
                conn.close()

                inserted = store.expand_search_pages_from_known_total(page_size=100, max_pages=None)

                conn = sqlite3.connect(Path(tmp) / "store.db")
                cur = conn.cursor()
                total_pages = cur.execute("select count(*) from search_pages").fetchone()[0]
                pending_pages = cur.execute("select count(*) from search_pages where status = 'pending'").fetchone()[0]
                conn.close()
                self.assertEqual(8505, inserted)
                self.assertEqual(8536, total_pages)
                self.assertEqual(8505, pending_pages)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
