"""巴西 DNB 交付测试。"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from brazil_crawler.delivery import build_delivery_bundle


class DeliveryTests(unittest.TestCase):
    def test_day1_outputs_only_qualified_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_root = root / "output" / "dnb"
            data_root.mkdir(parents=True)
            conn = sqlite3.connect(str(data_root / "dnb_store.db"))
            conn.executescript(
                """
                CREATE TABLE final_companies (
                    duns TEXT PRIMARY KEY,
                    company_name TEXT,
                    representative TEXT,
                    emails TEXT,
                    website TEXT,
                    phone TEXT,
                    address TEXT,
                    evidence_url TEXT
                );
                INSERT INTO final_companies VALUES
                    ('1', 'Acme Inc', 'Jane Doe', 'sales@acme.com', 'https://acme.com', '1', 'x', 'https://acme.com'),
                    ('2', 'No Email Inc', 'Jane Doe', '', 'https://bad.com', '1', 'x', 'https://bad.com');
                """
            )
            conn.commit()
            conn.close()

            summary = build_delivery_bundle(root / "output", root / "delivery", "day1")
            self.assertEqual(1, summary["delta_companies"])


if __name__ == "__main__":
    unittest.main()
