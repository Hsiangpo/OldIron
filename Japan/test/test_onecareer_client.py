"""OneCareer client 测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

from japan_crawler.sites.onecareer.client import _normalize_category_path


class OnecareerClientTests(unittest.TestCase):
    def test_normalize_category_path_accepts_full_path(self) -> None:
        self.assertEqual(
            "/companies/business_categories/5",
            _normalize_category_path("/companies/business_categories/5"),
        )

    def test_normalize_category_path_accepts_plain_id(self) -> None:
        self.assertEqual("/companies/business_categories/5", _normalize_category_path("5"))


if __name__ == "__main__":
    unittest.main()
