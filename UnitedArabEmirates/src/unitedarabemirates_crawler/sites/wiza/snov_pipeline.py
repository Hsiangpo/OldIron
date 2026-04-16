"""Wiza Snov P3。"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

from oldiron_core.snov import SnovAuthError
from oldiron_core.snov import SnovClient
from oldiron_core.snov import SnovPermissionError
from oldiron_core.snov import SnovQuotaError
from oldiron_core.snov import SnovService
from oldiron_core.snov import SnovServiceSettings

from ..common.store import UaeCompanyStore


LOGGER = logging.getLogger("uae.wiza.snov_pipeline")
DEFAULT_BATCH_SIZE = 8


def run_pipeline_snov(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = 8,
) -> dict[str, int]:
    """用 Snov 直接补域名邮箱和关键联系人。"""
    store = UaeCompanyStore(output_dir / "companies.db")
    batch_limit = max_items or min(max(int(concurrency or 1), 1), DEFAULT_BATCH_SIZE)
    pending = store.get_email_pending(batch_limit, require_website=False)
    if not pending:
        LOGGER.info("没有需要 Snov 补充的公司")
        return {"processed": 0, "found": 0}
    settings = SnovServiceSettings.from_env()
    settings.validate(require_llm=True)
    shared_client = SnovClient(settings.client_config)
    thread_local = threading.local()
    cleanup_lock = threading.Lock()
    services: list[SnovService] = []

    def _get_service() -> SnovService:
        service = getattr(thread_local, "service", None)
        if service is not None:
            return service
        service = SnovService(settings, client=shared_client)
        thread_local.service = service
        with cleanup_lock:
            services.append(service)
        return service

    def _worker(company: dict[str, str]) -> tuple[str, object]:
        result = _get_service().discover_company(
            company_name=company["company_name"],
            homepage=company["website"],
        )
        return company["record_id"], result

    processed = 0
    found = 0
    try:
        with ThreadPoolExecutor(max_workers=max(int(concurrency or 1), 1)) as executor:
            futures = {executor.submit(_worker, item): item for item in pending}
            for future in as_completed(futures):
                company = futures[future]
                try:
                    record_id, result = future.result()
                except (SnovAuthError, SnovQuotaError, SnovPermissionError):
                    raise
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Wiza Snov 补充失败，保留 pending：%s | %s", company["company_name"], exc)
                    continue
                store.save_email_result(
                    record_id,
                    list(result.domain_emails),
                    result.representative_names,
                    result.representative_names,
                    result.website,
                    people_json=result.people_json,
                    website=result.website,
                    mark_done=True,
                )
                processed += 1
                if not result.domain_emails or not result.people:
                    LOGGER.info(
                        "Wiza Snov 无有效结果，已收口：company=%s website=%s emails=%d people=%d",
                        company["company_name"],
                        result.website,
                        len(result.domain_emails),
                        len(result.people),
                    )
                    continue
                found += 1
    finally:
        for service in services:
            try:
                service.close()
            except Exception:  # noqa: BLE001
                pass
        try:
            shared_client.close()
        except Exception:  # noqa: BLE001
            pass
    return {"processed": processed, "found": found}
