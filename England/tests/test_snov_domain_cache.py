import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class SnovDomainCacheTests(unittest.TestCase):
    def test_prepare_lookup_claim_done_and_wait_flow(self) -> None:
        from england_crawler.snov.domain_cache import SnovDomainCache

        with tempfile.TemporaryDirectory() as tmp:
            cache = SnovDomainCache(Path(tmp) / "snov_cache.db")
            try:
                first = cache.prepare_lookup("example.com")
                second = cache.prepare_lookup("example.com")
                cache.mark_done("example.com", ["hello@example.com"])
                third = cache.prepare_lookup("example.com")

                self.assertEqual("claimed", first.status)
                self.assertEqual("wait", second.status)
                self.assertEqual("done", third.status)
                self.assertEqual(["hello@example.com"], third.emails)
            finally:
                cache.close()

    def test_defer_lookup_blocks_until_retry_window(self) -> None:
        from england_crawler.snov.domain_cache import SnovDomainCache

        with tempfile.TemporaryDirectory() as tmp:
            cache = SnovDomainCache(Path(tmp) / "snov_cache.db")
            try:
                cache.prepare_lookup("example.com")
                cache.defer("example.com", delay_seconds=60, error_text="429")

                decision = cache.prepare_lookup("example.com")

                self.assertEqual("wait", decision.status)
                self.assertGreaterEqual(decision.wait_seconds, 50)
            finally:
                cache.close()

    def test_running_lookup_uses_longer_recheck_window(self) -> None:
        from england_crawler.snov.domain_cache import RUNNING_RECHECK_SECONDS
        from england_crawler.snov.domain_cache import SnovDomainCache

        with tempfile.TemporaryDirectory() as tmp:
            cache = SnovDomainCache(Path(tmp) / "snov_cache.db")
            try:
                first = cache.prepare_lookup("example.com")
                second = cache.prepare_lookup("example.com")

                self.assertEqual("claimed", first.status)
                self.assertEqual("wait", second.status)
                self.assertGreaterEqual(second.wait_seconds, RUNNING_RECHECK_SECONDS)
            finally:
                cache.close()

    def test_seed_done_makes_domain_immediately_cacheable(self) -> None:
        from england_crawler.snov.domain_cache import SnovDomainCache

        with tempfile.TemporaryDirectory() as tmp:
            cache = SnovDomainCache(Path(tmp) / "snov_cache.db")
            try:
                cache.seed_done([("example.com", ["a@example.com"])])

                decision = cache.prepare_lookup("example.com")

                self.assertEqual("done", decision.status)
                self.assertEqual(["a@example.com"], decision.emails)
            finally:
                cache.close()
