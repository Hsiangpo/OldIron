"""indonesiayp 列表与详情抓取。"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from indonesia_crawler.models import CompanyRecord

from .client import IndonesiaYpClient
from .parser import extract_total_pages, parse_detail_page, parse_list_page

logger = logging.getLogger(__name__)

MAX_EMPTY_PAGES = 3
LIST_CHECKPOINT = "checkpoint_list.json"
DETAIL_CHECKPOINT = "checkpoint_detail.json"


def _load_last_page(checkpoint_file: Path) -> int:
    """读取列表阶段最后完成页码。"""
    if not checkpoint_file.exists():
        return 0
    data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    return int(data.get("last_page", 0))


def _save_last_page(checkpoint_file: Path, page: int, total_pages: int) -> None:
    """写入列表阶段断点。"""
    payload = {"last_page": page, "total_pages": total_pages}
    checkpoint_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _load_processed_ids(checkpoint_file: Path, output_file: Path) -> set[str]:
    """合并 checkpoint 与输出文件，避免重复写入。"""
    processed: set[str] = set()
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        processed = set(data.get("processed_ids", []))

    if output_file.exists():
        with output_file.open("r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                comp_id = str(record.get("comp_id", "")).strip()
                if comp_id:
                    processed.add(comp_id)
    return processed


def crawl_company_list(output_dir: Path, max_pages: int = 0) -> int:
    """抓取列表页，输出 `company_ids.jsonl`。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "company_ids.jsonl"
    checkpoint_file = output_dir / LIST_CHECKPOINT

    last_page = _load_last_page(checkpoint_file)
    current_page = last_page + 1
    mode = "a" if last_page > 0 else "w"
    written = 0
    empty_streak = 0
    total_pages = 0

    client = IndonesiaYpClient()
    try:
        with output_file.open(mode, encoding="utf-8") as fp:
            while True:
                if max_pages > 0 and (current_page - last_page - 1) >= max_pages:
                    logger.info("列表阶段达到最大页数限制 %d，停止", max_pages)
                    break

                path = "/category/general_business" if current_page == 1 else f"/category/general_business/{current_page}"
                logger.info("列表请求第 %d 页: %s", current_page, path)
                html = client.get_html(path)

                if total_pages == 0:
                    total_pages = extract_total_pages(html)
                    logger.info("列表总页数: %d", total_pages)

                items = parse_list_page(html)
                if not items:
                    empty_streak += 1
                    logger.warning("第 %d 页无数据（连续 %d 次）", current_page, empty_streak)
                    if empty_streak >= MAX_EMPTY_PAGES:
                        logger.info("连续空页达到阈值，停止列表抓取")
                        break
                    current_page += 1
                    continue

                empty_streak = 0
                for record in items:
                    fp.write(record.to_json_line() + "\n")
                    written += 1
                fp.flush()

                _save_last_page(checkpoint_file, current_page, total_pages)
                if current_page <= 3 or current_page % 50 == 0:
                    logger.info("列表进度: 第 %d 页，新增 %d 条，累计 %d 条", current_page, len(items), written)

                current_page += 1
                if total_pages > 0 and current_page > total_pages:
                    logger.info("列表阶段完成，已达末页")
                    break
    finally:
        client.close()
    return written


def crawl_company_details(output_dir: Path, max_items: int = 0) -> int:
    """抓取详情页，输出 `companies.jsonl`。"""
    ids_file = output_dir / "company_ids.jsonl"
    output_file = output_dir / "companies.jsonl"
    checkpoint_file = output_dir / DETAIL_CHECKPOINT
    if not ids_file.exists():
        logger.warning("未找到 company_ids.jsonl，跳过详情阶段")
        return 0

    records: list[CompanyRecord] = []
    with ids_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(CompanyRecord.from_dict(json.loads(line)))

    processed = _load_processed_ids(checkpoint_file, output_file)
    pending = [record for record in records if record.comp_id not in processed]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        logger.info("详情阶段无待处理记录")
        return 0

    written = 0
    client = IndonesiaYpClient()
    try:
        with output_file.open("a", encoding="utf-8") as fp:
            for record in pending:
                try:
                    html = client.get_html(record.detail_path)
                    parsed = parse_detail_page(html, detail_path=record.detail_path)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("详情抓取失败 %s: %s", record.comp_id, exc)
                    continue

                parsed.comp_id = record.comp_id
                if not parsed.company_name:
                    parsed.company_name = record.company_name
                if not parsed.detail_path:
                    parsed.detail_path = record.detail_path

                fp.write(parsed.to_json_line() + "\n")
                fp.flush()
                processed.add(parsed.comp_id)
                written += 1

                if written % 20 == 0:
                    checkpoint_file.write_text(
                        json.dumps({"processed_ids": sorted(processed)}, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    logger.info("详情进度: %d/%d", written, len(pending))
    finally:
        client.close()

    checkpoint_file.write_text(
        json.dumps({"processed_ids": sorted(processed)}, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("详情阶段完成，新增 %d 条", written)
    return written

