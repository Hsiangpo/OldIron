"""CNPJ Biz supervisor 测试。"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


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
from brazil_crawler.sites.cnpjbiz.supervisor import _choose_run_mode
from brazil_crawler.sites.cnpjbiz.supervisor import _is_fully_drained
from brazil_crawler.sites.cnpjbiz.supervisor import CnpjBizSupervisor
from brazil_crawler.sites.cnpjbiz.supervisor import SupervisorSettings


class SupervisorTests(unittest.TestCase):
    def test_choose_run_mode_prefers_detail_when_backlog_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CnpjBizStore(Path(tmpdir) / "store.db")
            store.seed_start_page("https://cnpj.biz/empresas")
            task = store.claim_list_task()
            assert task is not None
            store.complete_list_task(
                task.page_url,
                task.depth,
                [
                    {
                        "cnpj": "1",
                        "company_name": "Acme",
                        "detail_url": "https://cnpj.biz/1",
                        "city": "SP",
                        "region": "SP",
                        "status_text": "ATIVA",
                        "opened_at": "01/01/2020",
                    }
                ],
                "",
            )
            progress = store.progress()
            self.assertEqual("detail", _choose_run_mode(progress))
            self.assertFalse(_is_fully_drained(progress))
            store.close()

    def test_drained_state_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CnpjBizStore(Path(tmpdir) / "store.db")
            store.seed_start_page("https://cnpj.biz/empresas")
            task = store.claim_list_task()
            assert task is not None
            store.complete_list_task(task.page_url, task.depth, [], "")
            progress = store.progress()
            self.assertTrue(_is_fully_drained(progress))
            self.assertEqual("all", _choose_run_mode(progress))
            store.close()

    def test_rotate_clash_if_enabled_updates_last_choice(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SupervisorSettings(
                project_root=Path(tmpdir),
                output_dir=Path(tmpdir),
            )
            supervisor = CnpjBizSupervisor(settings)
            with patch(
                "brazil_crawler.sites.cnpjbiz.supervisor.CnpjBizConfig.from_env",
                return_value=type(
                    "Cfg",
                    (),
                    {
                        "clash_rotate_enabled": True,
                        "clash_unix_socket_path": "/tmp/verge.sock",
                        "clash_selector_name": "PROXY",
                    },
                )(),
            ), patch(
                "brazil_crawler.sites.cnpjbiz.supervisor.ClashUnixController.cycle_selector",
                return_value="🇭🇰 香港商宽",
            ):
                supervisor._rotate_clash_if_enabled()  # noqa: SLF001
            self.assertEqual("🇭🇰 香港商宽", supervisor._last_clash_choice)  # noqa: SLF001
