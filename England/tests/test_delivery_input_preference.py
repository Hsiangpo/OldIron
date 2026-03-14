import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from england_crawler.delivery import _load_all_records  # noqa: E402


class DeliveryInputPreferenceTests(unittest.TestCase):
    def test_load_all_records_prefers_final_companies_over_companies_with_emails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_root = Path(tmp) / "output"
            site_dir = data_root / "dnb"
            site_dir.mkdir(parents=True)
            (site_dir / "companies_with_emails.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "脏数据",
                        "ceo": "甲",
                        "homepage": "https://ko.wikipedia.org/wiki/test",
                        "emails": ["bad@example.com"],
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            (site_dir / "final_companies.jsonl").write_text(
                json.dumps(
                    {
                        "company_name": "干净数据",
                        "ceo": "乙",
                        "homepage": "https://example.com",
                        "emails": ["ok@example.com"],
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            records = _load_all_records(data_root)

            self.assertEqual(1, len(records))
            self.assertEqual("干净数据", records[0]["company_name"])


if __name__ == "__main__":
    unittest.main()
