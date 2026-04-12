"""OneCareer Pipeline 3 — 官网补邮箱与法人。"""

from __future__ import annotations

import logging
import multiprocessing
import os
import threading
import traceback
from queue import Empty
from pathlib import Path
from typing import Callable

from oldiron_core.fc_email.email_service import (
    DEFAULT_LLM_API_STYLE,
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
    DEFAULT_LLM_REASONING_EFFORT,
    FirecrawlEmailService,
    FirecrawlEmailSettings,
)
from oldiron_core.protocol_crawler import SiteCrawlClient, SiteCrawlConfig

from .store import OnecareerStore


LOGGER = logging.getLogger("onecareer.pipeline3")
_DEFAULT_BATCH_SIZE = 24
_MAX_SAFE_CONCURRENCY = 4
_DEFAULT_TASK_TIMEOUT_SECONDS = 90.0


def run_pipeline_email(
    *,
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = 128,
) -> dict[str, int]:
    store = OnecareerStore(output_dir / "onecareer_store.db")
    worker_count = _effective_email_concurrency(concurrency)
    if worker_count != max(int(concurrency or 1), 1):
        LOGGER.info("OneCareer 邮箱提取并发已收敛：请求=%d 实际=%d", int(concurrency or 1), worker_count)
    batch_limit = _email_batch_limit(max_items, worker_count)
    pending = store.get_email_pending(batch_limit)
    if not pending:
        LOGGER.info("没有需要提取邮箱的公司")
        return {"processed": 0, "found": 0}

    settings = _build_settings(output_dir)
    settings.validate()
    LOGGER.info("OneCareer 邮箱提取：待处理 %d 家，并发=%d，批量=%d", len(pending), 1, batch_limit)
    task_timeout_seconds = _email_task_timeout_seconds()

    def _worker(company: dict[str, str]) -> tuple[str, list[str], str]:
        return _run_company_process_with_timeout(company, settings, task_timeout_seconds)

    processed = 0
    found = 0
    for company in pending:
        try:
            company_id, emails, representative = _worker(company)
        except Exception:  # noqa: BLE001
            LOGGER.warning("邮箱提取失败：%s", company["company_name"], exc_info=True)
            processed += 1
            continue
        store.save_email_result(company_id, emails, representative)
        processed += 1
        if emails:
            found += 1
    return {"processed": processed, "found": found}


def _email_batch_limit(max_items: int, concurrency: int) -> int:
    if max_items > 0:
        return max_items
    configured = int(os.getenv("ONECAREER_EMAIL_BATCH_SIZE", str(_DEFAULT_BATCH_SIZE)) or _DEFAULT_BATCH_SIZE)
    return min(max(int(concurrency or 1), 1), max(configured, 1))


def _effective_email_concurrency(concurrency: int) -> int:
    requested = max(int(concurrency or 1), 1)
    configured_cap = int(os.getenv("ONECAREER_EMAIL_MAX_CONCURRENCY", str(_MAX_SAFE_CONCURRENCY)) or _MAX_SAFE_CONCURRENCY)
    return min(requested, max(configured_cap, 1))


def _email_task_timeout_seconds() -> float:
    raw = str(os.getenv("ONECAREER_EMAIL_TASK_TIMEOUT_SECONDS", str(_DEFAULT_TASK_TIMEOUT_SECONDS)) or _DEFAULT_TASK_TIMEOUT_SECONDS).strip()
    try:
        return max(float(raw), 10.0)
    except ValueError:
        return _DEFAULT_TASK_TIMEOUT_SECONDS


def _run_with_timeout(action: Callable[[], tuple[str, list[str], str]], *, timeout_seconds: float, timeout_label: str) -> tuple[str, list[str], str]:
    result_holder: dict[str, tuple[str, list[str], str]] = {}
    error_holder: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result_holder["value"] = action()
        except BaseException as exc:  # noqa: BLE001
            error_holder["error"] = exc

    worker = threading.Thread(target=_runner, name="onecareer-email-task", daemon=True)
    worker.start()
    worker.join(timeout_seconds)
    if worker.is_alive():
        raise TimeoutError(f"{timeout_label}，{timeout_seconds:.0f}s 内未完成")
    if "error" in error_holder:
        raise error_holder["error"]
    return result_holder["value"]


def _run_company_process_with_timeout(
    company: dict[str, str],
    settings: FirecrawlEmailSettings,
    timeout_seconds: float,
) -> tuple[str, list[str], str]:
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(
        target=_discover_company_contacts_process_entry,
        args=(result_queue, company, settings),
        daemon=True,
    )
    process.start()
    process.join(timeout_seconds)
    if process.is_alive():
        process.terminate()
        process.join(5.0)
        raise TimeoutError(f"OneCareer P3 单任务超时：{company['company_name']}，{timeout_seconds:.0f}s 内未完成")
    try:
        status, payload = result_queue.get_nowait()
    except Empty as exc:
        raise RuntimeError(f"OneCareer P3 子进程未返回结果：{company['company_name']} exit={process.exitcode}") from exc
    if status == "error":
        raise RuntimeError(str(payload))
    company_id, emails, representative = payload
    return str(company_id), list(emails or []), str(representative or "").strip()


def _discover_company_contacts(company: dict[str, str], settings: FirecrawlEmailSettings) -> tuple[str, list[str], str]:
    crawler = SiteCrawlClient(
        SiteCrawlConfig(
            proxy_url=os.getenv("HTTP_PROXY", "http://127.0.0.1:7897"),
            timeout_seconds=20.0,
        )
    )
    service = FirecrawlEmailService(settings, firecrawl_client=crawler)
    try:
        result = service.discover_emails(
            company_name=company["company_name"],
            homepage=company["website"],
            existing_representative=company.get("representative", ""),
        )
        return company["company_id"], list(result.emails or []), str(result.representative or "").strip()
    finally:
        try:
            service.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            crawler.close()
        except Exception:  # noqa: BLE001
            pass


def _discover_company_contacts_process_entry(
    result_queue,
    company: dict[str, str],
    settings: FirecrawlEmailSettings,
) -> None:
    try:
        result = _discover_company_contacts(company, settings)
    except BaseException as exc:  # noqa: BLE001
        result_queue.put(("error", f"{exc}\n{traceback.format_exc()}"))
        return
    result_queue.put(("ok", result))


def _build_settings(output_dir: Path) -> FirecrawlEmailSettings:
    return FirecrawlEmailSettings(
        project_root=output_dir.parent,
        crawl_backend="protocol",
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", DEFAULT_LLM_BASE_URL),
        llm_model=os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL),
        llm_reasoning_effort=os.getenv("LLM_REASONING_EFFORT", DEFAULT_LLM_REASONING_EFFORT),
        llm_api_style=os.getenv("LLM_API_STYLE", DEFAULT_LLM_API_STYLE),
        llm_timeout_seconds=float(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        prefilter_limit=int(os.getenv("FIRECRAWL_PREFILTER_LIMIT", "12")),
        llm_pick_count=int(os.getenv("FIRECRAWL_LLM_PICK_COUNT", "5")),
        extract_max_urls=int(os.getenv("FIRECRAWL_EXTRACT_MAX_URLS", "5")),
    )
