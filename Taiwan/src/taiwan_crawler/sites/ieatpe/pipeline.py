"""IEATPE Pipeline 1：A-Z 列表 + 详情。"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .client import IeatpeClient
from .config import IeatpeConfig
from .store import IeatpeStore


logger = logging.getLogger("ieatpe.pipeline")


def run_pipeline(
    *,
    config: IeatpeConfig,
    only_list: bool = False,
    only_detail: bool = False,
) -> dict[str, int]:
    """执行 IEATPE 单站点主流程。"""
    config.validate()
    config.output_dir.mkdir(parents=True, exist_ok=True)
    store = IeatpeStore(config.output_dir / "ieatpe_store.db")
    recovered = store.requeue_stale_running_tasks(
        older_than_seconds=config.stale_running_requeue_seconds,
    )
    if recovered["letters"] or recovered["details"]:
        logger.info("回收陈旧 running 任务：%s", recovered)
    retried_failed = store.requeue_failed_detail_tasks()
    if retried_failed:
        logger.info("重置 failed 详情任务：%d", retried_failed)
    store.seed_letters(list(config.letters))

    list_done = threading.Event()
    if only_detail:
        list_done.set()

    results = {"letters_done": 0, "companies_total": 0, "details_done": 0}

    def _list_worker() -> None:
        client = IeatpeClient(
            timeout_seconds=config.timeout_seconds,
            request_delay=config.request_delay,
            proxy_url=config.proxy_url,
        )
        while True:
            task = store.claim_letter_task()
            if task is None:
                break
            letter = task["letter"]
            try:
                rows = client.fetch_company_list(letter=letter, flow=config.flow)
                for row in rows:
                    store.upsert_company_summary(row, source_letter=letter)
                store.mark_letter_done(letter, result_count=len(rows))
                logger.info("字母 %s 完成，新增列表 %d 条", letter, len(rows))
            except Exception as exc:
                logger.warning("字母 %s 抓取失败: %s", letter, exc)
                store.mark_letter_failed(letter)
        list_done.set()

    def _detail_worker() -> None:
        client = IeatpeClient(
            timeout_seconds=config.timeout_seconds,
            request_delay=config.request_delay,
            proxy_url=config.proxy_url,
        )
        while True:
            task = store.claim_detail_task()
            if task is None:
                progress = store.get_progress()
                if list_done.is_set() and progress["details_pending"] == 0 and progress["details_running"] == 0:
                    return
                time.sleep(0.5)
                continue
            member_id = task["member_id"]
            try:
                detail = client.fetch_company_detail(member_id=member_id, flow=task["flow"])
                store.save_detail_result(member_id, detail)
                logger.info("详情完成：%s %s", member_id, detail.get("company_name", ""))
            except Exception as exc:
                logger.warning("详情 %s 抓取失败: %s", member_id, exc)
                store.mark_detail_failed(member_id)

    threads: list[threading.Thread] = []
    if not only_detail:
        for index in range(config.list_workers):
            threads.append(threading.Thread(target=_list_worker, name=f"ieatpe-list-{index+1}", daemon=True))
    if not only_list:
        for index in range(config.detail_workers):
            threads.append(threading.Thread(target=_detail_worker, name=f"ieatpe-detail-{index+1}", daemon=True))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    progress = store.get_progress()
    results["letters_done"] = progress["letters_done"]
    results["companies_total"] = progress["companies_total"]
    results["details_done"] = store.count_details_done()
    store.close()
    logger.info("IEATPE 完成：%s", results)
    return results
