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


class DeliveryTests(unittest.TestCase):
    def test_delivery_only_outputs_records_with_company_representative_and_email(self) -> None:
        from taiwan_crawler.delivery import build_delivery_bundle

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            site_dir = root / "output" / "ieatpe"
            site_dir.mkdir(parents=True, exist_ok=True)
            db_path = site_dir / "ieatpe_store.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE companies (
                    member_id TEXT PRIMARY KEY,
                    company_name TEXT,
                    representative TEXT,
                    website TEXT,
                    phone TEXT,
                    address TEXT,
                    emails TEXT,
                    detail_url TEXT
                );
                INSERT INTO companies VALUES
                    ('00007', '合發貿易股份有限公司', '徐季安', '', '(02)27407278', '臺北市', 'prgrtp@yahoo.com.tw', 'https://www.ieatpe.org.tw/qry/query.aspx'),
                    ('00008', '缺邮箱公司', '王小明', '', '(02)00000000', '臺北市', '', 'https://www.ieatpe.org.tw/qry/query.aspx');
                """
            )
            conn.commit()
            conn.close()

            summary = build_delivery_bundle(root / "output", root / "output" / "delivery", "day1")
            csv_path = root / "output" / "delivery" / "Taiwan_day001" / "companies.csv"
            self.assertEqual(1, summary["delta_companies"])
            rows = csv_path.read_text(encoding="utf-8-sig").splitlines()
            self.assertEqual(2, len(rows))
            self.assertIn("合發貿易股份有限公司,徐季安,prgrtp@yahoo.com.tw", rows[1])


if __name__ == "__main__":
    unittest.main()
