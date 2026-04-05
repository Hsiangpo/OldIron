"""mynavi 分组并发测试。"""

from __future__ import annotations

import sys
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

from japan_crawler.sites.mynavi.pipeline import _fetch_company_detail
from japan_crawler.sites.mynavi.pipeline import _run_group_jobs
from japan_crawler.sites.mynavi.cli import _parent_process_is_alive
from japan_crawler.sites.mynavi.cli import _prepare_processes_for_start
from japan_crawler.sites.mynavi.cli import _shutdown_processes
from japan_crawler.sites.mynavi.cli import _wait_for_next_round


class MynaviConcurrencyTests(unittest.TestCase):
    def test_group_jobs_parallelize_multiple_groups(self) -> None:
        groups = [
            {"group_code": "na"},
            {"group_code": "nk"},
            {"group_code": "ns"},
        ]
        active = 0
        max_active = 0
        lock = threading.Lock()

        def _worker(group: dict[str, str]) -> tuple[int, int]:
            nonlocal active, max_active
            _ = group
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            with lock:
                active -= 1
            return 1, 2

        groups_done, new_total = _run_group_jobs(groups=groups, max_workers=3, worker_fn=_worker)
        self.assertEqual(3, groups_done)
        self.assertEqual(6, new_total)
        self.assertGreaterEqual(max_active, 2)

    def test_fetch_company_detail_falls_back_when_detail_html_missing(self) -> None:
        class _MissingDetailClient:
            def fetch_detail_page(self, detail_url: str) -> str | None:
                _ = detail_url
                return None

        card = {
            "company_id": "413161",
            "company_name": "株式会社メイツ",
            "address": "東京都中央区",
            "industry": "教育",
            "detail_url": "/company/413161/",
        }
        company = _fetch_company_detail(_MissingDetailClient(), card)
        self.assertEqual("413161", company["company_id"])
        self.assertEqual("株式会社メイツ", company["company_name"])
        self.assertEqual("", company["representative"])
        self.assertEqual("", company["website"])
        self.assertEqual("東京都中央区", company["address"])

    def test_prepare_processes_marks_children_daemon(self) -> None:
        class _FakeProcess:
            def __init__(self) -> None:
                self.daemon = False

        processes = [_FakeProcess(), _FakeProcess()]
        _prepare_processes_for_start(processes)
        self.assertTrue(all(process.daemon for process in processes))

    def test_shutdown_processes_terminates_alive_children(self) -> None:
        class _FakeProcess:
            def __init__(self, alive: bool) -> None:
                self._alive = alive
                self.join_calls = 0
                self.terminate_calls = 0

            def join(self, timeout: float | None = None) -> None:  # noqa: ARG002
                self.join_calls += 1

            def is_alive(self) -> bool:
                return self._alive

            def terminate(self) -> None:
                self.terminate_calls += 1
                self._alive = False

        class _FakeEvent:
            def __init__(self) -> None:
                self.set_calls = 0

            def set(self) -> None:
                self.set_calls += 1

        class _FakeQueue:
            def __init__(self) -> None:
                self.close_calls = 0
                self.cancel_calls = 0

            def close(self) -> None:
                self.close_calls += 1

            def cancel_join_thread(self) -> None:
                self.cancel_calls += 1

        event = _FakeEvent()
        queue = _FakeQueue()
        processes = [_FakeProcess(alive=False), _FakeProcess(alive=True)]
        _shutdown_processes(processes, event, queue)
        self.assertEqual(1, event.set_calls)
        self.assertEqual(0, processes[0].terminate_calls)
        self.assertEqual(1, processes[1].terminate_calls)
        self.assertEqual(1, queue.close_calls)
        self.assertEqual(1, queue.cancel_calls)

    def test_parent_process_is_alive_uses_multiprocessing_parent(self) -> None:
        class _FakeParent:
            def __init__(self, alive: bool) -> None:
                self._alive = alive

            def is_alive(self) -> bool:
                return self._alive

        with mock.patch("japan_crawler.sites.mynavi.cli.mp.parent_process", return_value=_FakeParent(True)):
            self.assertTrue(_parent_process_is_alive())
        with mock.patch("japan_crawler.sites.mynavi.cli.mp.parent_process", return_value=_FakeParent(False)):
            self.assertFalse(_parent_process_is_alive())

    def test_wait_for_next_round_exits_when_parent_is_dead(self) -> None:
        class _FakeEvent:
            def is_set(self) -> bool:
                return False

        with mock.patch("japan_crawler.sites.mynavi.cli._parent_process_is_alive", return_value=False):
            self.assertFalse(_wait_for_next_round(_FakeEvent(), 5))


if __name__ == "__main__":
    unittest.main()
