"""bizmaps 错误都道府県自动重试测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))


class _FakeBizmapsStore:
    def __init__(self, db_path):  # noqa: D401
        self._db_path = db_path
        self._checkpoints: list[tuple[str, int, int, str, str]] = []

    def get_all_prefs(self):
        return [{"pref_code": "12", "name": "千葉県", "total": 100}, {"pref_code": "11", "name": "埼玉県", "total": 100}]

    def get_pending_prefs(self):
        return [{"pref_code": "12", "name": "千葉県", "total": 100, "last_page": 5, "total_pages": 10, "status": "running"}]

    def get_prefs_by_status(self, status: str):
        if status == "error":
            return [{"pref_code": "11", "name": "埼玉県", "total": 100, "last_page": 3, "total_pages": 8, "status": "error"}]
        return []

    def get_company_count(self):
        return 123

    def update_checkpoint(self, pref_code, last_page, total_pages, status="running", last_ph=""):
        self._checkpoints.append((pref_code, last_page, total_pages, status, last_ph))

    def upsert_prefs(self, prefs):
        return len(prefs)


class _FakeBizmapsClient:
    def __init__(self, *args, **kwargs):  # noqa: D401
        self.stats = {"requests": 0, "errors": 0}


class BizmapsRetryTests(unittest.TestCase):
    def test_run_pipeline_list_retries_error_prefecture_once(self) -> None:
        from japan_crawler.sites.bizmaps import pipeline

        calls: list[tuple[str, bool]] = []

        def fake_run_prefecture(client, store, pref, *, force_restart=False):
            calls.append((pref["pref_code"], force_restart))
            return {"new": 1, "completed": True}

        with patch.object(pipeline, "BizmapsStore", _FakeBizmapsStore), \
             patch.object(pipeline, "BizmapsClient", _FakeBizmapsClient), \
             patch.object(pipeline, "_run_prefecture", side_effect=fake_run_prefecture):
            result = pipeline.run_pipeline_list(output_dir=ROOT / "output" / "bizmaps-test")

        self.assertEqual([("12", False), ("11", True)], calls)
        self.assertEqual(2, result["prefs_done"])


if __name__ == "__main__":
    unittest.main()
