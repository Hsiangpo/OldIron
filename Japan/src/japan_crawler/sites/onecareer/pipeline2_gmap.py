"""OneCareer Pipeline 2 — GMap 补官网。"""

from __future__ import annotations

import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from oldiron_core.google_maps import GoogleMapsClient, GoogleMapsConfig

from .store import OnecareerStore


LOGGER = logging.getLogger("onecareer.pipeline2")


def run_pipeline_gmap(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = 16,
) -> dict[str, int]:
    store = OnecareerStore(output_dir / "onecareer_store.db")
    pending = store.get_gmap_pending(max_items)
    if not pending:
        LOGGER.info("没有需要 GMap 补官网的公司")
        return {"processed": 0, "found": 0}
    LOGGER.info("OneCareer GMap：待处理 %d 家，并发=%d", len(pending), concurrency)

    thread_local = threading.local()

    def _get_client() -> GoogleMapsClient:
        if not hasattr(thread_local, "client"):
            thread_local.client = GoogleMapsClient(GoogleMapsConfig(hl="ja", gl="jp"))
        return thread_local.client

    def _worker(company: dict[str, str]) -> tuple[str, str]:
        query = _build_query(company["company_name"], company.get("address", ""))
        try:
            result = _get_client().search_company_profile(query, company["company_name"])
            return company["company_id"], result.website if result else ""
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("GMap 查询失败: %s", exc)
            return company["company_id"], ""

    processed = 0
    found = 0
    with ThreadPoolExecutor(max_workers=max(int(concurrency or 1), 1)) as executor:
        futures = {executor.submit(_worker, item): item for item in pending}
        for future in as_completed(futures):
            company_id, website = future.result()
            processed += 1
            if website:
                store.update_website(company_id, website)
                found += 1
            else:
                store.mark_gmap_done(company_id)
    return {"processed": processed, "found": found, "total": store.get_company_count()}


def _build_query(company_name: str, address: str) -> str:
    location = _extract_location_prefix(address)
    return f"{company_name} {location}".strip()


def _extract_location_prefix(address: str) -> str:
    text = str(address or "").strip()
    matched = re.match(r"(.+?[都道府県].+?[市区郡町村])", text)
    if matched is not None:
        return str(matched.group(1) or "").strip()
    matched = re.match(r"(.+?[都道府県])", text)
    return str(matched.group(1) or "").strip() if matched is not None else text

