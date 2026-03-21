"""详情页解析器 — 逐个公司页面提取代表者和官网，支持并发。"""

from __future__ import annotations

import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from lxml import html as lxml_html

from .client import CatchClient, RateLimitConfig
from .models import CompanyRecord

logger = logging.getLogger(__name__)

CONCURRENCY = 8
_thread_local = threading.local()


def _get_client() -> CatchClient:
    """每个线程一个独立的 CatchClient。"""
    if not hasattr(_thread_local, "client"):
        rate_config = RateLimitConfig(
            min_delay=0.3, max_delay=1.0,
            long_rest_interval=200, long_rest_seconds=15.0,
        )
        _thread_local.client = CatchClient(rate_config=rate_config)
    return _thread_local.client


def _extract_first_ceo(raw_ceo: str) -> str:
    """从代表者字段提取第一个人名。"""
    if not raw_ceo:
        return ""
    parts = re.split(r"[/,·、]", raw_ceo)
    return parts[0].strip()


def _parse_detail_html(raw_html: str, comp_id: str) -> dict:
    """从详情页 HTML 提取代表者和官网。"""
    result = {"ceo": "", "homepage": ""}

    try:
        tree = lxml_html.fromstring(raw_html)
    except Exception as exc:
        logger.warning("HTML 解析失败 (CompID=%s): %s", comp_id, exc)
        return result

    try:
        ths = tree.xpath("//th[contains(text(),'대표자')]")
        for th in ths:
            td = th.getnext()
            if td is not None and td.tag == "td":
                raw_ceo = (td.text_content() or "").strip()
                if raw_ceo:
                    result["ceo"] = _extract_first_ceo(raw_ceo)
                    break
    except Exception as exc:
        logger.debug("CEO 提取失败 (CompID=%s): %s", comp_id, exc)

    try:
        jsonld_scripts = tree.xpath('//script[@type="application/ld+json"]/text()')
        for script_text in jsonld_scripts:
            try:
                ld_data = json.loads(script_text)
                same_as = ld_data.get("sameAs", [])
                if isinstance(same_as, list):
                    for url in same_as:
                        if url and "catch.co.kr" not in url:
                            result["homepage"] = url
                            break
            except (json.JSONDecodeError, TypeError):
                pass
    except Exception as exc:
        logger.debug("JSON-LD 解析失败 (CompID=%s): %s", comp_id, exc)

    return result


def _fetch_and_parse(record: CompanyRecord) -> CompanyRecord:
    """Worker: 请求详情页并解析。"""
    client = _get_client()
    raw = client.get_html(f"/Comp/CompSummary/{record.comp_id}")
    detail = _parse_detail_html(raw, record.comp_id)
    record.ceo = detail["ceo"]
    record.homepage = detail["homepage"]
    return record


def load_processed_ids(checkpoint_path: Path) -> set[str]:
    if checkpoint_path.exists():
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return set(data.get("processed_ids", []))
    return set()


def save_processed_ids(checkpoint_path: Path, ids: set[str]) -> None:
    checkpoint_path.write_text(
        json.dumps({"processed_ids": sorted(ids)}, ensure_ascii=False),
        encoding="utf-8",
    )


def crawl_details(
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = CONCURRENCY,
) -> int:
    """
    并发爬取详情页，输出 companies.jsonl。

    返回实际处理的记录数。
    """
    ids_file = output_dir / "company_ids.jsonl"
    output_file = output_dir / "companies.jsonl"
    checkpoint_file = output_dir / "checkpoint_detail.json"

    if not ids_file.exists():
        return 0

    records: list[CompanyRecord] = []
    with ids_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(CompanyRecord.from_dict(json.loads(line)))

    processed_ids = load_processed_ids(checkpoint_file)

    # 过滤待处理的
    pending = [r for r in records if r.comp_id not in processed_ids]
    if max_items > 0:
        pending = pending[:max_items]

    if not pending:
        return 0

    logger.info("详情爬虫: 待处理 %d 条, 并发=%d", len(pending), concurrency)

    write_lock = threading.Lock()
    written = 0

    try:
        with (
            output_file.open("a", encoding="utf-8") as fp,
            ThreadPoolExecutor(max_workers=concurrency) as executor,
        ):
            futures = {executor.submit(_fetch_and_parse, r): r for r in pending}

            for fut in as_completed(futures):
                original = futures[fut]
                try:
                    result = fut.result()
                    with write_lock:
                        fp.write(result.to_json_line() + "\n")
                        fp.flush()
                        processed_ids.add(result.comp_id)
                        written += 1

                        if written % 50 == 0:
                            save_processed_ids(checkpoint_file, processed_ids)

                        if written <= 5 or written % 100 == 0:
                            logger.info(
                                "[%d] %s | CEO=%s | HP=%s",
                                written,
                                result.company_name,
                                result.ceo or "-",
                                result.homepage[:40] if result.homepage else "-",
                            )

                except RuntimeError as exc:
                    logger.warning("详情页失败 (%s): %s", original.comp_id, exc)

    finally:
        save_processed_ids(checkpoint_file, processed_ids)

    logger.info("详情爬取完成: %d 条", written)
    return written
