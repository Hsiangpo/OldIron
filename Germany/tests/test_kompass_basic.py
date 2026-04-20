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

from germany_crawler.sites.kompass.client import _looks_like_challenge_response
from germany_crawler.sites.kompass.client import build_list_url
from germany_crawler.sites.kompass.pipeline import parse_companies_from_html
from germany_crawler.sites.kompass.pipeline import run_pipeline_list
from germany_crawler.sites.kompass.store import GermanyKompassStore


SAMPLE_HTML = """
<section class="results">
  <article class="card">
    <a class="title" href="/c/berlin-tools/de123456/">Berlin Tools GmbH</a>
    <a href="/c/p/berlin-tools/de123456/">See the 11 products</a>
    <a href="https://www.facebook.com/berlin-tools">Facebook</a>
    <a class="website" href="https://www.berlin-tools.de/">www.berlin-tools.de</a>
  </article>
  <article class="card">
    <a class="title" href="/c/hamburg-industrie/de654321/">Hamburg Industrie GmbH</a>
    <div class="meta">Website</div>
    <a class="website" href="https://hamburg-industrie.de">hamburg-industrie.de</a>
  </article>
  <article class="card">
    <a class="title" href="/c/placeholder-industrie/de999999/">Placeholder Industrie GmbH</a>
    <a class="website" href="http://mise-en-relation.svaplus.fr/">redirect</a>
  </article>
</section>
"""


class GermanyKompassTests(unittest.TestCase):
    def test_build_list_url_uses_page_suffix_after_first_page(self) -> None:
        self.assertEqual(build_list_url(1), "https://us.kompass.com/businessplace/z/de/")
        self.assertEqual(build_list_url(2), "https://us.kompass.com/businessplace/z/de/page-2/")

    def test_parse_companies_from_html_extracts_company_and_website(self) -> None:
        records = parse_companies_from_html(SAMPLE_HTML)
        self.assertEqual(
            records,
            [
                {"company_name": "Berlin Tools GmbH", "website": "https://www.berlin-tools.de/"},
                {"company_name": "Hamburg Industrie GmbH", "website": "https://hamburg-industrie.de"},
            ],
        )

    def test_challenge_detection_keeps_valid_datadome_page(self) -> None:
        valid_page = """
        <html>
          <head><script src="https://js.datadome.co/tags.js"></script></head>
          <body><a href="/c/berlin-tools/de123456/">Berlin Tools GmbH</a></body>
        </html>
        """
        self.assertFalse(_looks_like_challenge_response(200, valid_page))
        self.assertTrue(_looks_like_challenge_response(403, "Please enable JS and disable any ad blocker"))

    def test_run_pipeline_list_done_checkpoint_exports_unique_websites(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = GermanyKompassStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {"company_name": "Berlin Tools GmbH", "website": "https://www.berlin-tools.de/"},
                    {"company_name": "Berlin Tools Holding", "website": "https://www.berlin-tools.de/"},
                    {"company_name": "Hamburg Industrie GmbH", "website": "https://hamburg-industrie.de"},
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
        self.assertEqual(lines, ["https://hamburg-industrie.de", "https://www.berlin-tools.de/"])


if __name__ == "__main__":
    unittest.main()
