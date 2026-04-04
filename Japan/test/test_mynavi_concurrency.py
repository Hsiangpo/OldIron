"""mynavi 分组并发测试。"""

from __future__ import annotations

import sys
import threading
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))

from japan_crawler.sites.mynavi.pipeline import _run_group_jobs


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


if __name__ == "__main__":
    unittest.main()
