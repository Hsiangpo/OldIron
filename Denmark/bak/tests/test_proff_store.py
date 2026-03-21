from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from denmark_crawler.sites.proff.models import ProffCompany  # noqa: E402
from denmark_crawler.sites.proff.store import ProffStore  # noqa: E402


class ProffStoreTests(unittest.TestCase):
    def test_store_seeds_claims_and_exports_final_company(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ProffStore(root / "store.db")
            try:
                store.ensure_search_seed(["ApS"])
                task = store.claim_search_task()
                self.assertIsNotNone(task)
                assert task is not None
                self.assertEqual("ApS", task.query)
                self.assertEqual(1, task.page)

                store.upsert_company(
                    ProffCompany(
                        orgnr="41900598",
                        company_name="Nordic Sales Force ApS",
                        representative="Mik Meisen Lokdam",
                        representative_role="Direktør",
                        homepage="https://nordicsalesforce.dk/",
                        email="info@nordicsalesforce.com",
                        phone="88 63 88 00",
                        source_query="ApS",
                        source_page=1,
                        source_url="https://www.proff.dk/branches%C3%B8g?q=ApS&page=1",
                        raw_payload={"name": "Nordic Sales Force ApS"},
                    )
                )
                store.mark_search_done(query="ApS", page=1, total_pages=3, max_pages_per_query=2)
                store.export_jsonl_snapshots(root / "output")

                progress = store.get_progress()
                self.assertEqual(2, progress.search_total)
                self.assertEqual(1, progress.search_done)
                self.assertEqual(1, progress.final_total)

                csv_like = (root / "output" / "final_companies.jsonl").read_text(encoding="utf-8")
                rows = [json.loads(line) for line in csv_like.splitlines() if line.strip()]
                self.assertEqual(1, len(rows))
                self.assertEqual("Nordic Sales Force ApS", rows[0]["company_name"])
            finally:
                store.close()

    def test_store_schedules_gmap_then_firecrawl_for_no_email_company(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ProffStore(root / "store.db")
            try:
                store.upsert_company(
                    ProffCompany(
                        orgnr="555",
                        company_name="No Mail ApS",
                        representative="Jane Doe",
                        representative_role="Direktør",
                        homepage="",
                        email="",
                        phone="1234",
                        source_query="ApS",
                        source_page=1,
                        source_url="https://www.proff.dk/branches%C3%B8g?q=ApS&page=1",
                    )
                )
                gmap_task = store.claim_gmap_task()
                self.assertIsNotNone(gmap_task)
                assert gmap_task is not None
                self.assertEqual("555", gmap_task.orgnr)

                store.mark_gmap_done(orgnr="555", website="https://nomail.dk/", source="gmap", phone="1234")
                firecrawl_task = store.claim_firecrawl_task()
                self.assertIsNotNone(firecrawl_task)
                assert firecrawl_task is not None
                self.assertEqual("https://nomail.dk/", firecrawl_task.website)

                store.mark_firecrawl_done(orgnr="555", emails=["hello@nomail.dk"])
                progress = store.get_progress()
                self.assertEqual(1, progress.final_total)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
