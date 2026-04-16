"""Wiza Snov Pipeline 1。"""

from __future__ import annotations

import json
import logging
import math
import time
from pathlib import Path
from typing import Any

from ..common.enrich import normalize_website_url
from ..common.store import UaeCompanyStore
from .client import WizaClient


LOGGER = logging.getLogger("uae.wizasnov.pipeline")
CHECKPOINT_NAME = "list_checkpoint.json"
PAGE_SIZE = 100


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str = "",
    max_pages: int = 0,
    concurrency: int = 8,
) -> dict[str, int]:
    """抓取 Wiza UAE 公司列表，不再进入站内详情页。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    store = UaeCompanyStore(output_dir / "companies.db")
    finalized = _finalize_legacy_pending_p1(store)
    checkpoint = _load_checkpoint(output_dir)
    if checkpoint.get("status") == "done" and max_pages <= 0:
        if finalized:
            LOGGER.info("Wiza Snov 历史 P1 已停用并收口：%d 家", finalized)
        return {"pages": 0, "new_companies": 0, "total_companies": store.get_company_count()}
    client = WizaClient(output_dir, proxy)
    page_number = int(checkpoint.get("page") or 0) + 1
    search_after = checkpoint.get("search_after")
    processed_pages = 0
    new_companies = 0
    try:
        while True:
            page = client.search_companies(search_after=search_after, page_size=PAGE_SIZE)
            if not page.items:
                _save_checkpoint(output_dir, page_number - 1, [], "done")
                store.update_checkpoint("list", page_number - 1, "done")
                break
            companies = _build_company_records(page.items)
            new_companies += store.upsert_companies(companies)
            processed_pages += 1
            total_pages = _estimate_total_pages(page.total, page.total_relation, page.page_size)
            _save_checkpoint(output_dir, page_number, page.last_sort, "running")
            store.update_checkpoint("list", page_number, "running")
            LOGGER.info("Wiza Snov 页 %d/%s：解析 %d 家", page_number, total_pages or "?", len(companies))
            if max_pages > 0 and processed_pages >= max_pages:
                break
            if not page.last_sort:
                _save_checkpoint(output_dir, page_number, [], "done")
                store.update_checkpoint("list", page_number, "done")
                break
            search_after = page.last_sort
            page_number += 1
            time.sleep(max(request_delay, 0.0))
    finally:
        client.close()
    return {
        "pages": processed_pages,
        "new_companies": new_companies,
        "total_companies": store.get_company_count(),
    }


def _finalize_legacy_pending_p1(store: UaeCompanyStore) -> int:
    """把旧库里遗留的站内详情任务统一收口为 done。"""
    return store.finalize_pending_p1()


def _build_company_records(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    """列表页只保留公司基础字段与官网，不再抓站内联系人详情。"""
    results: list[dict[str, str]] = []
    for item in items:
        record = _build_company_record_without_contacts(item)
        if record["company_name"]:
            results.append(record)
    return results


def _normalize_company_website(value: str) -> str:
    text = str(value or "").strip()
    if text and "://" not in text:
        text = f"https://{text}"
    return normalize_website_url(text)


def _normalize_linkedin_url(value: str) -> str:
    text = str(value or "").strip()
    if text and "://" not in text:
        text = f"https://{text}"
    return text


def _build_summary(item: dict[str, Any]) -> str:
    parts = [
        str(item.get("industry") or "").strip(),
        str(item.get("size") or "").strip(),
        str(item.get("inferred_revenue") or "").strip(),
    ]
    return " | ".join(part for part in parts if part)


def _build_company_record_without_contacts(item: dict[str, Any]) -> dict[str, str]:
    company_name = str(item.get("name") or "").strip()
    website = _normalize_company_website(str(item.get("website") or "").strip())
    linkedin_url = _normalize_linkedin_url(str(item.get("linkedin_url") or "").strip())
    return {
        "company_name": company_name,
        "source_pdl_id": str(item.get("pdl_id") or "").strip(),
        "p1_status": "done",
        "representative_p1": "",
        "representative_final": "",
        "website": website,
        "address": str(item.get("location_name") or "").strip(),
        "phone": "",
        "emails": "",
        "detail_url": linkedin_url or "https://wiza.co/app/prospect",
        "summary": _build_summary(item),
        "evidence_url": linkedin_url or "https://wiza.co/app/prospect",
    }


def _estimate_total_pages(total: int, total_relation: str, page_size: int) -> int:
    if total <= 0 or page_size <= 0:
        return 0
    if str(total_relation or "").lower() != "eq":
        return 0
    pages = max(math.ceil(total / page_size), 1)
    return pages


def _load_checkpoint(output_dir: Path) -> dict[str, Any]:
    checkpoint_path = output_dir / CHECKPOINT_NAME
    if not checkpoint_path.exists():
        return {}
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.warning("Wiza Snov checkpoint 解析失败：%s", checkpoint_path)
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_checkpoint(output_dir: Path, page: int, search_after: list[Any], status: str) -> None:
    payload = {
        "page": int(page),
        "search_after": list(search_after or []),
        "status": str(status or "running"),
    }
    checkpoint_path = output_dir / CHECKPOINT_NAME
    checkpoint_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

