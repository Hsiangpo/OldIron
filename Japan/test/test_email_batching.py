"""日本邮箱阶段批处理测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


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

        self.assertEqual(24, _email_batch_limit(0, 128))
        self.assertEqual(5, _email_batch_limit(5, 128))

    def test_hellowork_batch_size_scales_with_concurrency(self) -> None:
        from japan_crawler.sites.hellowork.pipeline2_email import _iter_batches
        from japan_crawler.sites.hellowork.pipeline2_email import _resolve_batch_size

        self.assertEqual(512, _resolve_batch_size(128))
        batches = list(_iter_batches(list(range(1025)), 512))
        self.assertEqual([512, 512, 1], [len(batch) for batch in batches])


if __name__ == "__main__":
    unittest.main()
