from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
SHARED_DIR = SHARED_PARENT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from england_crawler.sites.kompass.client import build_list_url
from england_crawler.sites.kompass.pipeline import parse_companies_from_html
from england_crawler.sites.kompass.pipeline import run_pipeline_list
from england_crawler.sites.kompass.store import EnglandKompassStore


SAMPLE_HTML = """
<section class="results">
  <article class="card">
    <a class="title" href="/c/acme-industrial/gb123456/"><strong>Acme Industrial Ltd</strong></a>
    <a href="https://www.linkedin.com/company/acme-industrial/">LinkedIn</a>
    <a class="website" href="https://www.acme.co.uk/">www.acme.co.uk</a>
  </article>
  <article class="card">
    <a class="title" href="/c/northern-fabrication/gb654321/">Northern Fabrication Ltd</a>
    <span>Website</span>
    <a class="website" href="https://northernfab.co.uk">northernfab.co.uk</a>
  </article>
</section>
"""


class EnglandKompassTests(unittest.TestCase):
    def test_build_list_url_uses_page_suffix_after_first_page(self) -> None:
        self.assertEqual(build_list_url(1), "https://us.kompass.com/businessplace/z/gb/")
        self.assertEqual(build_list_url(2), "https://us.kompass.com/businessplace/z/gb/page-2/")

    def test_parse_companies_from_html_extracts_company_and_website(self) -> None:
        records = parse_companies_from_html(SAMPLE_HTML)
        self.assertEqual(
            records,
            [
                {"company_name": "Acme Industrial Ltd", "website": "https://www.acme.co.uk/"},
                {"company_name": "Northern Fabrication Ltd", "website": "https://northernfab.co.uk"},
            ],
        )

    def test_run_pipeline_list_done_checkpoint_exports_unique_websites(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = EnglandKompassStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {"company_name": "Acme Industrial Ltd", "website": "https://www.acme.co.uk/"},
                    {"company_name": "Acme Industrial Holdings", "website": "https://www.acme.co.uk/"},
                    {"company_name": "Northern Fabrication Ltd", "website": "https://northernfab.co.uk"},
                ]
            )
            (output_dir / "list_checkpoint.json").write_text(
                '{"page": 2, "status": "done"}',
                encoding="utf-8",
            )

            result = run_pipeline_list(output_dir=output_dir, request_delay=0, proxy="", max_pages=0)

            lines = (output_dir / "websites.txt").read_text(encoding="utf-8").splitlines()

        self.assertEqual(result["pages"], 0)
        self.assertEqual(result["total_companies"], 3)
        self.assertEqual(lines, ["https://northernfab.co.uk", "https://www.acme.co.uk/"])


if __name__ == "__main__":
    unittest.main()
