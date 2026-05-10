"""CNPJ Biz 存储测试。"""

from __future__ import annotations

import sqlite3
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

from brazil_crawler.sites.cnpjbiz.store import CnpjBizStore


class CnpjBizStoreTests(unittest.TestCase):
    def test_complete_list_task_seeds_next_page_and_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CnpjBizStore(Path(tmpdir) / "store.db")
            store.seed_start_page("https://cnpj.biz/empresas")
            task = store.claim_list_task()
            self.assertIsNotNone(task)
            assert task is not None
            store.complete_list_task(
                task.page_url,
                task.depth,
                [
                    {
                        "cnpj": "1",
                        "company_name": "Acme",
                        "detail_url": "https://cnpj.biz/1",
                        "city": "Sao Paulo",
                        "region": "SP",
                        "status_text": "ATIVA",
                        "opened_at": "01/01/2020",
                    }
                ],
                "https://cnpj.biz/empresas?id=2",
            )
            progress = store.progress()
            self.assertEqual(1, progress.list_pending)
            self.assertEqual(1, progress.companies_total)
            detail = store.claim_detail_task()
            self.assertIsNotNone(detail)
            assert detail is not None
            self.assertEqual("1", detail.cnpj)
            store.close()

    def test_complete_detail_task_upserts_final_company(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "store.db"
            store = CnpjBizStore(db_path)
            store.seed_start_page("https://cnpj.biz/empresas")
            task = store.claim_list_task()
            assert task is not None
            store.complete_list_task(
                task.page_url,
                task.depth,
                [
                    {
                        "cnpj": "1",
                        "company_name": "Acme Brasil LTDA",
                        "detail_url": "https://cnpj.biz/1",
                        "city": "Sao Paulo",
                        "region": "SP",
                        "status_text": "ATIVA",
                        "opened_at": "01/01/2020",
                    }
                ],
                "",
            )
            store.complete_detail_task(
                cnpj="1",
                company_name="Acme Brasil LTDA",
                trade_name="Acme",
                representative="Joao Souza",
                representative_candidates_json='[{"name":"Joao Souza","role":"Sócio-Administrador"}]',
                emails="contato@acme.com.br",
                phone="(11) 99999-0000",
                address="Rua A, Sao Paulo, SP",
                city="Sao Paulo",
                region="SP",
                status_text="ATIVA",
                opened_at="01/01/2020",
                evidence_url="https://cnpj.biz/1",
            )
            conn = sqlite3.connect(str(db_path))
            row = conn.execute(
                "SELECT company_name, representative, emails FROM final_companies WHERE cnpj = '1'"
            ).fetchone()
            conn.close()
            self.assertEqual(("Acme Brasil LTDA", "Joao Souza", "contato@acme.com.br"), row)
            store.close()


if __name__ == "__main__":
    unittest.main()
