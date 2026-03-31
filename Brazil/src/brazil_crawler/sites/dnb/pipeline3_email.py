"""DNB 巴西 Pipeline 3：官网抓取并覆盖公司名/代表人。"""

from __future__ import annotations

import logging
import os
import threading
import time

from oldiron_core.fc_email import FirecrawlEmailService
from oldiron_core.fc_email import FirecrawlEmailSettings
from oldiron_core.protocol_crawler import SiteCrawlClient
from oldiron_core.protocol_crawler import SiteCrawlConfig

from .store import DnbBrStore


LOGGER = logging.getLogger(__name__)


def run_pipeline_email(
    *,
    store: DnbBrStore,
    settings: FirecrawlEmailSettings,
    workers: int,
    stop_event,
) -> None:
    threads = [
        threading.Thread(
            target=_site_worker,
            args=(store, settings, stop_event),
            name=f"dnb-site-{index + 1}",
            daemon=True,
        )
        for index in range(max(int(workers or 1), 1))
    ]
    for thread in threads:
        thread.start()
    try:
        while not stop_event.is_set():
            time.sleep(1.0)
    finally:
        for thread in threads:
            thread.join(timeout=2)


def _site_worker(store: DnbBrStore, settings: FirecrawlEmailSettings, stop_event) -> None:
    crawler = SiteCrawlClient(
        SiteCrawlConfig(
            proxy_url=os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
            timeout_seconds=20.0,
        )
    )
    service = FirecrawlEmailService(settings, firecrawl_client=crawler)
    try:
        while not stop_event.is_set():
            task = store.claim_site_task()
            if task is None:
                time.sleep(1.0)
                continue
            try:
                result = service.discover_emails(
                    company_name=task.company_name,
                    homepage=task.website,
                    existing_representative=task.representative,
                )
                company_name = (result.company_name or "").strip() or task.company_name
                representative = (result.representative or "").strip()
                emails = list(result.emails or [])
                store.complete_site_task(
                    task.duns,
                    company_name,
                    representative,
                    emails,
                    task.website,
                    "",
                    "",
                    result.evidence_url or task.website,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("DNB 官网邮箱抓取失败：%s | %s", task.duns, exc)
                try:
                    store.fail_site_task(task.duns)
                except Exception as fail_exc:  # noqa: BLE001
                    LOGGER.error("DNB 官网 fail_site_task 也失败了：%s | %s", task.duns, fail_exc)
    finally:
        service.close()
