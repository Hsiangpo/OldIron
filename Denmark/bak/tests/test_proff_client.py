from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from denmark_crawler.sites.proff.client import build_task_key  # noqa: E402
from denmark_crawler.sites.proff.client import parse_search_page  # noqa: E402
from denmark_crawler.sites.proff.client import parse_task_key  # noqa: E402


class ProffClientTests(unittest.TestCase):
    def test_task_key_roundtrip(self) -> None:
        task_key = build_task_key("ApS", "municipality:Københavns Kommune", industry="Finans")
        search_term, filter_text, industry = parse_task_key(task_key)
        self.assertEqual("ApS", search_term)
        self.assertEqual("municipality:Københavns Kommune", filter_text)
        self.assertEqual("Finans", industry)

    def test_parse_search_page_reads_company_rows_from_next_data(self) -> None:
        html = """
        <html>
          <head></head>
          <body>
            <script id="__NEXT_DATA__" type="application/json">
            {
              "props": {
                "pageProps": {
                  "hydrationData": {
                    "searchStore": {
                      "companies": {
                        "hits": 438356,
                        "pages": 400,
                        "currentPage": 1,
                        "companies": [
                          {
                            "name": "Nordic Sales Force ApS",
                            "orgnr": "41900598",
                            "email": "info@nordicsalesforce.com",
                            "homePage": "https://nordicsalesforce.dk/",
                            "phone": "88 63 88 00",
                            "contactPerson": {
                              "name": "Mik Meisen Lokdam",
                              "role": "Direktør"
                            }
                          }
                        ]
                      }
                    }
                  }
                }
              }
            }
            </script>
          </body>
        </html>
        """

        rows, hits, pages = parse_search_page(
            html,
            query="ApS",
            page=1,
            source_url="https://www.proff.dk/branches%C3%B8g?q=ApS&page=1",
        )

        self.assertEqual(438356, hits)
        self.assertEqual(400, pages)
        self.assertEqual(1, len(rows))
        self.assertEqual("41900598", rows[0].orgnr)
        self.assertEqual("Nordic Sales Force ApS", rows[0].company_name)
        self.assertEqual("Mik Meisen Lokdam", rows[0].representative)
        self.assertEqual("Direktør", rows[0].representative_role)
        self.assertEqual("info@nordicsalesforce.com", rows[0].email)


if __name__ == "__main__":
    unittest.main()
