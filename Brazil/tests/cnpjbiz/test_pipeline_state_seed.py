"""CNPJ Biz 州级种子测试。"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from brazil_crawler.sites.cnpjbiz.pipeline import build_initial_seed_urls
from brazil_crawler.sites.cnpjbiz.store import CnpjBizStore


class StateSeedTests(unittest.TestCase):
    def test_build_initial_seed_urls_for_states(self) -> None:
        urls = build_initial_seed_urls("states")
        self.assertEqual(27, len(urls))
        self.assertIn("https://cnpj.biz/empresas/estado/SP", urls)
        self.assertIn("https://cnpj.biz/empresas/estado/RJ", urls)

    def test_store_seed_start_pages_inserts_multiple_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CnpjBizStore(Path(tmpdir) / "store.db")
            store.seed_start_pages(
                [
                    "https://cnpj.biz/empresas/estado/SP",
                    "https://cnpj.biz/empresas/estado/RJ",
                ]
            )
            progress = store.progress()
            self.assertEqual(2, progress.list_pending)
            store.close()
