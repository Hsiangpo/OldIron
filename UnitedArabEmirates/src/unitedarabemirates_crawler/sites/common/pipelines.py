"""阿联酋通用 P2/P3。"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

from oldiron_core.fc_email.email_service import FirecrawlEmailService
from oldiron_core.google_maps import GoogleMapsClient
from oldiron_core.google_maps import GoogleMapsConfig
from oldiron_core.protocol_crawler import SiteCrawlClient
from oldiron_core.protocol_crawler import SiteCrawlConfig

from .enrich import build_email_settings
from .enrich import merge_representatives
from .enrich import normalize_person_name
from .store import UaeCompanyStore


LOGGER = logging.getLogger("uae.common.pipelines")
DEFAULT_BATCH_SIZE = 64


def run_pipeline_gmap(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = 64,
) -> dict[str, int]:
    """补全缺失官网。"""
    store = UaeCompanyStore(output_dir / "companies.db")
    pending = store.get_gmap_pending(max_items or max(int(concurrency or 1), 1))
    if not pending:
        LOGGER.info("没有需要 GMap 补官网的公司")
        return {"processed": 0, "found": 0}
    thread_local = threading.local()

    def _get_client() -> GoogleMapsClient:
        client = getattr(thread_local, "client", None)
        if client is not None:
            return client
        thread_local.client = GoogleMapsClient(GoogleMapsConfig(hl="en", gl="ae"))
        return thread_local.client

    def _worker(company: dict[str, str]) -> tuple[str, str, str]:
        query = f"{company['company_name']} {company.get('address', '')} United Arab Emirates".strip()
        result = _get_client().search_company_profile(query, company["company_name"])
        website = result.website if result else ""
        phone = result.phone if result else ""
        return company["record_id"], website, phone

    processed = 0
    found = 0
    with ThreadPoolExecutor(max_workers=max(int(concurrency or 1), 1)) as executor:
        futures = {executor.submit(_worker, item): item for item in pending}
        for future in as_completed(futures):
            record_id, website, phone = future.result()
            processed += 1
            if website:
                found += 1
                store.save_gmap_result(record_id, website, phone)
                continue
            store.mark_gmap_done(record_id)
    return {"processed": processed, "found": found}


def run_pipeline_email(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = 64,
) -> dict[str, int]:
    """补官网邮箱并强制追加官网代表人。"""
    store = UaeCompanyStore(output_dir / "companies.db")
    batch_limit = max_items or min(max(int(concurrency or 1), 1), DEFAULT_BATCH_SIZE)
    pending = store.get_email_pending(batch_limit)
    if not pending:
        LOGGER.info("没有需要官网补充的公司")
        return {"processed": 0, "found": 0}
    settings = build_email_settings(output_dir)
    settings.validate(require_llm=True)
    thread_local = threading.local()
    cleanup_lock = threading.Lock()
    resources: list[tuple[FirecrawlEmailService, SiteCrawlClient]] = []

    def _get_service() -> FirecrawlEmailService:
        service = getattr(thread_local, "service", None)
        if service is not None:
            return service
        crawler = SiteCrawlClient(
            SiteCrawlConfig(
                proxy_url=os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
                timeout_seconds=20.0,
            )
        )
        service = FirecrawlEmailService(settings, firecrawl_client=crawler)
        thread_local.service = service
        with cleanup_lock:
            resources.append((service, crawler))
        return service

    def _worker(company: dict[str, str]) -> tuple[str, list[str], str, str]:
        result = _get_service().discover_emails(
            company_name=company["company_name"],
            homepage=company["website"],
            existing_representative="",
        )
        p3_rep = normalize_person_name(str(result.representative or ""), company["company_name"])
        final_rep = merge_representatives(
            str(company.get("representative_p1", "")),
            p3_rep,
            company["company_name"],
        )
        return (
            company["record_id"],
            list(result.emails or []),
            p3_rep,
            final_rep,
            str(result.evidence_url or "").strip(),
        )

    processed = 0
    found = 0
    try:
        with ThreadPoolExecutor(max_workers=max(int(concurrency or 1), 1)) as executor:
            futures = {executor.submit(_worker, item): item for item in pending}
            for future in as_completed(futures):
                company = futures[future]
                try:
                    record_id, emails, p3_rep, final_rep, evidence_url = future.result()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("官网补充失败：%s | %s", company["company_name"], exc)
                    record_id = company["record_id"]
                    emails = []
                    p3_rep = ""
                    final_rep = normalize_person_name(
                        str(company.get("representative_p1", "")),
                        company["company_name"],
                    )
                    evidence_url = ""
                store.save_email_result(record_id, emails, p3_rep, final_rep, evidence_url)
                processed += 1
                if emails:
                    found += 1
    finally:
        for service, crawler in resources:
            try:
                service.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                crawler.close()
            except Exception:  # noqa: BLE001
                pass
    return {"processed": processed, "found": found}
