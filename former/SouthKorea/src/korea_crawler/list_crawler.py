"""列表API爬虫 — 分页遍历所有公司ID和基础信息，支持并发。"""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import CatchClient, RateLimitConfig
from .models import CompanyRecord

logger = logging.getLogger(__name__)

LIST_API_PATH = "/api/v1.0/comp/compMajor/getMainCompanyListV2"

LIST_PAYLOAD_TEMPLATE = {
    "NowPage": 1,
    "PageSize": 30,
    "Sort": "popularity",
    "IsInHiring": 0,
    "IsRecommendCompany": 0,
    "CName": "",
    "AreaSido": "",
    "JCode": "",
    "Size": "",
    "ThemeName": "",
    "Culture": "",
    "Salary": "0,0",
    "AverageSalary": "0,0",
    "IsNewList": 1,
    "GroupName": "",
    "CategoryName": "",
}

PAGE_SIZE = 30
MAX_EMPTY_PAGES = 3
CONCURRENCY = 8

_thread_local = threading.local()


def _get_client() -> CatchClient:
    """每个线程一个独立的 CatchClient。"""
    if not hasattr(_thread_local, "client"):
        rate_config = RateLimitConfig(
            min_delay=0.3, max_delay=1.0,
            long_rest_interval=200, long_rest_seconds=10.0,
        )
        _thread_local.client = CatchClient(rate_config=rate_config)
    return _thread_local.client


def _fetch_page(page_num: int) -> tuple[int, list[dict], int]:
    """抓取单页，返回 (页码, 公司列表, 总数)。"""
    client = _get_client()
    payload = {**LIST_PAYLOAD_TEMPLATE, "NowPage": page_num}
    data = client.post_json(LIST_API_PATH, payload)
    company_list = data.get("companyList", [])
    total_count = data.get("totalCount", 0)
    return page_num, company_list, total_count


def load_checkpoint(checkpoint_path: Path) -> int:
    if checkpoint_path.exists():
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return int(data.get("last_page", 0))
    return 0


def save_checkpoint(checkpoint_path: Path, page: int) -> None:
    checkpoint_path.write_text(
        json.dumps({"last_page": page}, ensure_ascii=False),
        encoding="utf-8",
    )


def crawl_list(
    output_dir: Path,
    max_pages: int = 0,
    start_page: int = 0,
    concurrency: int = CONCURRENCY,
) -> int:
    """
    并发爬取列表API，输出 company_ids.jsonl。

    返回实际写入的记录数。
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "company_ids.jsonl"
    checkpoint_file = output_dir / "checkpoint_list.json"

    if start_page > 0:
        last_page = start_page - 1
    else:
        last_page = load_checkpoint(checkpoint_file)

    current_page = last_page + 1
    total_written = 0
    mode = "a" if last_page > 0 else "w"
    write_lock = threading.Lock()

    logger.info("列表爬虫启动: 从第 %d 页开始, 并发=%d", current_page, concurrency)

    try:
        with (
            output_file.open(mode, encoding="utf-8") as fp,
            ThreadPoolExecutor(max_workers=concurrency) as executor,
        ):
            stop = False
            empty_streak = 0
            pages_done = 0

            while not stop:
                # 提交一批页面请求
                batch_end = current_page + concurrency
                if max_pages > 0:
                    batch_end = min(batch_end, last_page + 1 + max_pages)

                futures = {}
                for pg in range(current_page, batch_end):
                    fut = executor.submit(_fetch_page, pg)
                    futures[fut] = pg

                if not futures:
                    break

                # 按完成顺序处理结果
                batch_results: dict[int, tuple[list[dict], int]] = {}
                for fut in as_completed(futures):
                    pg = futures[fut]
                    try:
                        page_num, company_list, total_count = fut.result()
                        batch_results[page_num] = (company_list, total_count)
                    except RuntimeError as exc:
                        logger.error("第 %d 页请求失败: %s", pg, exc)
                        stop = True
                        break

                # 按页码顺序写入，保证顺序一致
                for pg in sorted(batch_results.keys()):
                    company_list, total_count = batch_results[pg]

                    if not company_list:
                        empty_streak += 1
                        if empty_streak >= MAX_EMPTY_PAGES:
                            logger.info("连续 %d 页无数据，遍历完毕", MAX_EMPTY_PAGES)
                            stop = True
                            break
                        continue

                    empty_streak = 0

                    with write_lock:
                        for item in company_list:
                            comp_id = str(item.get("CompID", ""))
                            comp_name = str(item.get("CompName", ""))
                            if comp_id:
                                record = CompanyRecord(
                                    comp_id=comp_id,
                                    company_name=comp_name,
                                )
                                fp.write(record.to_json_line() + "\n")
                                total_written += 1
                        fp.flush()

                    save_checkpoint(checkpoint_file, pg)
                    pages_done += 1

                    pct = min(pg * PAGE_SIZE / total_count * 100, 100) if total_count else 0
                    if pages_done <= 3 or pages_done % 50 == 0:
                        logger.info("第 %d 页 [%d条] | 进度 ~%.1f%%", pg, len(company_list), pct)

                current_page = batch_end

                if max_pages > 0 and pages_done >= max_pages:
                    logger.info("已达最大页数限制 %d, 停止", max_pages)
                    stop = True

    except Exception:
        save_checkpoint(checkpoint_file, current_page - 1)
        raise

    logger.info("列表爬取完成: %d 条记录", total_written)
    return total_written
