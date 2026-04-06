"""邮箱服务生命周期测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.oldiron_core.fc_email.email_service import FirecrawlEmailService
from shared.oldiron_core.fc_email.email_service import FirecrawlEmailSettings
from shared.oldiron_core.fc_email.email_service import extract_domain


class _DummyCrawler:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class _DummyKeyPool:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class FirecrawlEmailServiceLifecycleTests(unittest.TestCase):
    def test_close_does_not_close_injected_crawler_or_key_pool(self) -> None:
        crawler = _DummyCrawler()
        key_pool = _DummyKeyPool()
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=key_pool,
            firecrawl_client=crawler,
        )
        service.close()
        self.assertEqual(0, crawler.closed)
        self.assertEqual(0, key_pool.closed)

    def test_extract_domain_uses_registrable_domain_for_subdomain(self) -> None:
        self.assertEqual("aig.com", extract_domain("http://www-154.aig.com/"))
        self.assertEqual("abc.co.jp", extract_domain("https://recruit.abc.co.jp/about"))

    def test_filter_same_domain_emails_matches_registrable_domain(self) -> None:
        service = FirecrawlEmailService(
            FirecrawlEmailSettings(
                llm_api_key="x",
                llm_model="gpt-5.4-mini",
            ),
            key_pool=_DummyKeyPool(),
            firecrawl_client=_DummyCrawler(),
        )
        emails = service._filter_same_domain_emails(
            "http://www-154.aig.com/",
            [
                "aviationworklist@aig.com",
                "paul.smith@talbotuw.com",
            ],
        )
        self.assertEqual(["aviationworklist@aig.com"], emails)


if __name__ == "__main__":
    unittest.main()
