"""巴西 P3：规则优先，LLM 兜底邮箱测试。"""

from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from brazil_crawler.sites.dnb.pipeline3_email import _site_worker


class _FakeTask:
    def __init__(self) -> None:
        self.duns = "1"
        self.company_name = "POUSADA VILA DO COCO BEACH LTDA"
        self.representative = ""
        self.website = "https://example.com"
        self.status = "pending"
        self.retries = 0
        self.updated_at = "2026-03-31 00:00:00"


class _FakeStore:
    def __init__(self) -> None:
        self.task = _FakeTask()
        self.completed = None
        self.calls = 0

    def claim_site_task(self):
        self.calls += 1
        if self.calls == 1:
            return self.task
        return None

    def complete_site_task(self, *args):
        self.completed = args

    def fail_site_task(self, duns: str) -> None:
        raise AssertionError(f"不应失败: {duns}")


class _FakeCrawler:
    def __init__(self, *args, **kwargs) -> None:
        _ = args, kwargs

    def close(self) -> None:
        return None


class _FakeEmailService:
    last_kwargs = None

    def __init__(self, *args, **kwargs) -> None:
        _ = args, kwargs

    def discover_emails(self, **kwargs):
        _FakeEmailService.last_kwargs = kwargs
        from oldiron_core.fc_email import EmailDiscoveryResult

        return EmailDiscoveryResult(
            company_name=kwargs["company_name"],
            representative="LUIS GUSTAVO DOS SANTOS",
            emails=["gustavo.contabil@hotmail.com"],
            evidence_url="https://example.com/about",
        )

    def close(self) -> None:
        return None


class Pipeline3EmailFlowTests(unittest.TestCase):
    def test_brazil_p3_uses_plain_llm_fallback_when_rules_miss_email(self) -> None:
        from oldiron_core.fc_email import FirecrawlEmailSettings

        stop_event = threading.Event()
        store = _FakeStore()
        settings = FirecrawlEmailSettings(
            llm_api_key="dummy",
            llm_model="dummy",
            llm_api_style="auto",
            crawl_backend="protocol",
        )

        with patch("brazil_crawler.sites.dnb.pipeline3_email.SiteCrawlClient", _FakeCrawler), \
             patch("brazil_crawler.sites.dnb.pipeline3_email.FirecrawlEmailService", _FakeEmailService):
            _FakeEmailService.last_kwargs = None
            worker = threading.Thread(
                target=_site_worker,
                args=(store, settings, stop_event),
                daemon=True,
            )
            worker.start()
            for _ in range(20):
                if store.completed is not None:
                    break
                threading.Event().wait(0.05)
            stop_event.set()
            worker.join(timeout=2)

        self.assertIsNotNone(store.completed)
        assert store.completed is not None
        self.assertIsNotNone(_FakeEmailService.last_kwargs)
        self.assertEqual("https://example.com", _FakeEmailService.last_kwargs["homepage"])
        self.assertEqual("POUSADA VILA DO COCO BEACH LTDA", _FakeEmailService.last_kwargs["company_name"])
        self.assertEqual(["gustavo.contabil@hotmail.com"], list(store.completed[3]))
        self.assertEqual("LUIS GUSTAVO DOS SANTOS", store.completed[2])


if __name__ == "__main__":
    unittest.main()
