from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


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

from england_crawler.sites.kompass.client import _looks_like_challenge_response
from england_crawler.sites.kompass.client import build_list_url
from england_crawler.sites.kompass.client import build_seed_page_url
from england_crawler.sites.kompass.pipeline import extract_seed_urls
from england_crawler.sites.kompass.pipeline import parse_companies_from_html
from england_crawler.sites.kompass.pipeline import run_pipeline_list
from england_crawler.sites.kompass.store import EnglandKompassStore


SAMPLE_HTML = """
<section class="results">
  <article class="card">
    <a class="title" href="/c/acme-industrial/gb123456/"><strong>Acme Industrial Ltd</strong></a>
    <a href="/c/p/acme-industrial/gb123456/">See the 23 products</a>
    <a href="https://www.linkedin.com/company/acme-industrial/">LinkedIn</a>
    <a class="website" href="https://www.acme.co.uk/">www.acme.co.uk</a>
  </article>
  <article class="card">
    <a class="title" href="/c/northern-fabrication/gb654321/">Northern Fabrication Ltd</a>
    <span>Website</span>
    <a class="website" href="https://northernfab.co.uk">northernfab.co.uk</a>
  </article>
  <article class="card">
    <a class="title" href="/c/placeholder-industrial/gb999999/">Placeholder Industrial Ltd</a>
    <a class="website" href="http://mise-en-relation.svaplus.fr/">redirect</a>
  </article>
</section>
"""

ROOT_SEED_HTML = """
<div>
  <a href="/z/gb/r/england-united-kingdom/gb_gbr09/">England</a>
  <a href="/z/gb/r/scotland/gb_gbr11/">Scotland</a>
  <a href="/z/gb/r/wales/gb_gbr12/">Wales</a>
  <a href="/z/gb/r/northern-ireland/gb_gbr10/">Northern Ireland</a>
</div>
"""

ENGLAND_REGION_HTML = """
<div>
  <a href="/z/gb/d/greater-london/gb_gbr09_gl/">Greater London</a>
  <a href="/z/gb/d/lancashire/gb_gbr09_la/">Lancashire</a>
  <a href="/z/gb/d/west-midlands/gb_gbr09_wm/">West Midlands</a>
</div>
"""

CONCURRENT_HTML_BY_URL = {
    "seed-a": """
    <section class="results">
      <article><a href="/c/acme-industrial/gb123456/">Acme Industrial Ltd</a><a href="https://www.acme.co.uk/">www.acme.co.uk</a></article>
    </section>
    """,
    "seed-b": """
    <section class="results">
      <article><a href="/c/northern-fabrication/gb654321/">Northern Fabrication Ltd</a><a href="https://northernfab.co.uk">northernfab.co.uk</a></article>
    </section>
    """,
}


class EnglandKompassTests(unittest.TestCase):
    def test_build_list_url_uses_page_suffix_after_first_page(self) -> None:
        self.assertEqual(build_list_url(1), "https://us.kompass.com/businessplace/z/gb/")
        self.assertEqual(build_list_url(2), "https://us.kompass.com/businessplace/z/gb/page-2/")
        self.assertEqual(
            build_seed_page_url("https://us.kompass.com/z/gb/d/greater-london/gb_gbr09_gl/", 2),
            "https://us.kompass.com/z/gb/d/greater-london/gb_gbr09_gl/page-2/",
        )

    def test_extract_seed_urls_collects_root_and_england_counties(self) -> None:
        seeds = extract_seed_urls(ROOT_SEED_HTML, country_code="gb")
        england_url = "https://us.kompass.com/z/gb/r/england-united-kingdom/gb_gbr09/"
        self.assertEqual(
            seeds,
            [
                england_url,
                "https://us.kompass.com/z/gb/r/scotland/gb_gbr11/",
                "https://us.kompass.com/z/gb/r/wales/gb_gbr12/",
                "https://us.kompass.com/z/gb/r/northern-ireland/gb_gbr10/",
            ],
        )
        county_seeds = extract_seed_urls(ENGLAND_REGION_HTML, country_code="gb", kind="county")
        self.assertEqual(
            county_seeds,
            [
                "https://us.kompass.com/z/gb/d/greater-london/gb_gbr09_gl/",
                "https://us.kompass.com/z/gb/d/lancashire/gb_gbr09_la/",
                "https://us.kompass.com/z/gb/d/west-midlands/gb_gbr09_wm/",
            ],
        )

    def test_parse_companies_from_html_extracts_company_and_website(self) -> None:
        records = parse_companies_from_html(SAMPLE_HTML)
        self.assertEqual(
            records,
            [
                {"company_name": "Acme Industrial Ltd", "website": "https://www.acme.co.uk/"},
                {"company_name": "Northern Fabrication Ltd", "website": "https://northernfab.co.uk"},
            ],
        )

    def test_challenge_detection_keeps_valid_datadome_page(self) -> None:
        valid_page = """
        <html>
          <head><script src="https://js.datadome.co/tags.js"></script></head>
          <body><a href="/c/acme-industrial/gb123456/">Acme Industrial Ltd</a></body>
        </html>
        """
        self.assertFalse(_looks_like_challenge_response(200, valid_page))
        self.assertTrue(_looks_like_challenge_response(403, "Please enable JS and disable any ad blocker"))
        self.assertTrue(_looks_like_challenge_response(405, "Please contact your local Kompass or support.bip@kompass.com"))

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
                '{"status": "done", "seeds": [{"url": "https://us.kompass.com/z/gb/r/england-united-kingdom/gb_gbr09/", "label": "england-united-kingdom", "page": 2, "status": "done"}]}',
                encoding="utf-8",
            )

            result = run_pipeline_list(output_dir=output_dir, request_delay=0, proxy="", max_pages=0)

            lines = (output_dir / "websites.txt").read_text(encoding="utf-8").splitlines()

        self.assertEqual(result["pages"], 0)
        self.assertEqual(result["total_companies"], 3)
        self.assertEqual(lines, ["https://northernfab.co.uk", "https://www.acme.co.uk/"])

    def test_run_pipeline_list_stops_when_page_repeats_without_new_companies(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            store = EnglandKompassStore(output_dir / "companies.db")
            store.upsert_companies(
                [
                    {"company_name": "Acme Industrial Ltd", "website": "https://www.acme.co.uk/"},
                    {"company_name": "Northern Fabrication Ltd", "website": "https://northernfab.co.uk"},
                ]
            )

            class FakeClient:
                def __init__(self, output_dir: Path, proxy: str) -> None:
                    del output_dir, proxy

                def fetch_page(self, url: str, *, referer: str = "") -> str:
                    del url, referer
                    return SAMPLE_HTML

                def close(self) -> None:
                    return None

            checkpoint_payload = {
                "status": "running",
                "seeds": [
                    {
                        "url": "https://us.kompass.com/z/gb/r/england-united-kingdom/gb_gbr09/",
                        "label": "england-united-kingdom",
                        "page": 0,
                        "status": "pending",
                    }
                ],
            }
            (output_dir / "list_checkpoint.json").write_text(
                __import__("json").dumps(checkpoint_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            with patch("england_crawler.sites.kompass.pipeline.KompassClient", FakeClient):
                result = run_pipeline_list(output_dir=output_dir, request_delay=0, proxy="", max_pages=0)

            checkpoint = (output_dir / "list_checkpoint.json").read_text(encoding="utf-8")

        self.assertEqual(result["pages"], 0)
        self.assertEqual(result["new_companies"], 0)
        self.assertIn('"status": "done"', checkpoint)

    def test_run_pipeline_list_supports_concurrent_seed_workers(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)

            class FakeClient:
                _calls: dict[str, int] = {}

                def __init__(self, output_dir: Path, proxy: str) -> None:
                    del output_dir, proxy

                def fetch_page(self, url: str, *, referer: str = "") -> str:
                    del referer
                    count = self._calls.get(url, 0)
                    self._calls[url] = count + 1
                    if count > 0:
                        return ""
                    for key, html in CONCURRENT_HTML_BY_URL.items():
                        if key in url:
                            return html
                    return ""

                def close(self) -> None:
                    return None

            checkpoint_payload = {
                "status": "running",
                "seeds": [
                    {"url": "https://us.kompass.com/z/gb/r/seed-a/", "label": "seed-a", "page": 0, "status": "pending"},
                    {"url": "https://us.kompass.com/z/gb/r/seed-b/", "label": "seed-b", "page": 0, "status": "pending"},
                ],
            }
            (output_dir / "list_checkpoint.json").write_text(
                __import__("json").dumps(checkpoint_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            with patch("england_crawler.sites.kompass.pipeline.KompassClient", FakeClient):
                result = run_pipeline_list(output_dir=output_dir, request_delay=0, proxy="", max_pages=0, concurrency=2)

            lines = (output_dir / "websites.txt").read_text(encoding="utf-8").splitlines()

        self.assertEqual(result["pages"], 2)
        self.assertEqual(result["new_companies"], 2)
        self.assertEqual(result["total_companies"], 2)
        self.assertEqual(lines, ["https://northernfab.co.uk", "https://www.acme.co.uk/"])


if __name__ == "__main__":
    unittest.main()
