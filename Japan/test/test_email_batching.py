"""日本邮箱阶段批处理测试。"""

from __future__ import annotations

import sys
import time
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SHARED_PARENT = ROOT.parent
SHARED_DIR = SHARED_PARENT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SHARED_PARENT) not in sys.path:
    sys.path.insert(0, str(SHARED_PARENT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))


class EmailBatchingTests(unittest.TestCase):
    def test_bizmaps_email_batch_limit_matches_other_sites(self) -> None:
        from japan_crawler.sites.bizmaps.pipeline3_email import _email_batch_limit

        self.assertEqual(64, _email_batch_limit(0, 128))
        self.assertEqual(5, _email_batch_limit(5, 128))

    def test_hellowork_batch_size_scales_with_concurrency(self) -> None:
        from japan_crawler.sites.hellowork.pipeline2_email import _iter_batches
        from japan_crawler.sites.hellowork.pipeline2_email import _resolve_batch_size

        self.assertEqual(512, _resolve_batch_size(128))
        batches = list(_iter_batches(list(range(1025)), 512))
        self.assertEqual([512, 512, 1], [len(batch) for batch in batches])

    def test_onecareer_run_with_timeout_fails_fast(self) -> None:
        from japan_crawler.sites.onecareer.pipeline3_email import _run_with_timeout

        start = time.perf_counter()
        with self.assertRaises(TimeoutError):
            _run_with_timeout(
                lambda: (time.sleep(0.2), ("company-1", [], ""))[1],
                timeout_seconds=0.05,
                timeout_label="timeout-test",
            )
        self.assertLess(time.perf_counter() - start, 0.2)

    def test_onecareer_pipeline3_uses_real_concurrency(self) -> None:
        from japan_crawler.sites.onecareer.pipeline3_email import run_pipeline_email
        from japan_crawler.sites.onecareer.store import OnecareerStore

        class _DummySettings:
            def validate(self) -> None:
                return None

        state = {"current": 0, "max": 0}
        lock = threading.Lock()

        def _fake_worker(company: dict[str, str], settings: object, timeout_seconds: float):
            with lock:
                state["current"] += 1
                state["max"] = max(state["max"], state["current"])
            time.sleep(0.15)
            with lock:
                state["current"] -= 1
            return company["company_id"], ["a@example.com"], "代表人"

        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            store = OnecareerStore(output_dir / "onecareer_store.db")
            try:
                store.upsert_companies(
                    [
                        {
                            "company_id": f"cid-{idx}",
                            "company_name": f"company-{idx}",
                            "website": f"https://example-{idx}.com",
                            "representative": "",
                        }
                        for idx in range(4)
                    ]
                )
            finally:
                store.close()

            start = time.perf_counter()
            with patch("japan_crawler.sites.onecareer.pipeline3_email._build_settings", return_value=_DummySettings()):
                with patch("japan_crawler.sites.onecareer.pipeline3_email._run_company_process_with_timeout", side_effect=_fake_worker):
                    result = run_pipeline_email(output_dir=output_dir, concurrency=128)
            elapsed = time.perf_counter() - start

            self.assertEqual({"processed": 4, "found": 4}, result)
            self.assertGreaterEqual(state["max"], 2)
            self.assertLess(elapsed, 0.5)


if __name__ == "__main__":
    unittest.main()
