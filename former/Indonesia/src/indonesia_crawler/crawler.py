"""GAPENSI 分页爬虫 — 遍历所有页面，解析公司数据，支持断点续跑。"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from .client import GapensiClient, RateLimitConfig
from .parser import parse_page, extract_total_count
from .models import CompanyRecord

logger = logging.getLogger(__name__)

# 每页记录数（使用最大值 200 减少请求数）
PAGE_LIMIT = 200
# 连续空页阈值
MAX_EMPTY_PAGES = 3


def _build_page_url(page: int, limit: int = PAGE_LIMIT) -> str:
    """构建分页请求路径。"""
    return (
        f"/anggota?"
        f"limit={limit}&keyword=&idkual=&subkla=&kab=&char="
        f"&tahun=&provinsi=&page={page}"
    )


def load_checkpoint(checkpoint_path: Path) -> int:
    """读取断点（已完成的最后一页）。"""
    if checkpoint_path.exists():
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return int(data.get("last_page", 0))
    return 0


def save_checkpoint(checkpoint_path: Path, page: int, total: int) -> None:
    """保存断点。"""
    checkpoint_path.write_text(
        json.dumps({
            "last_page": page,
            "total_records_expected": total,
        }, ensure_ascii=False),
        encoding="utf-8",
    )


def crawl_gapensi(
    output_dir: Path,
    max_pages: int = 0,
) -> int:
    """
    爬取 GAPENSI 全部公司数据。

    参数:
        output_dir: 输出目录
        max_pages: 最大页数（0=全量）

    返回:
        实际写入的记录数
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "companies.jsonl"
    checkpoint_file = output_dir / "checkpoint.json"

    last_page = load_checkpoint(checkpoint_file)
    current_page = last_page + 1
    total_written = 0
    empty_streak = 0
    total_expected = 0

    # 断点续跑时，统计已有记录数
    if last_page > 0 and output_file.exists():
        with output_file.open("r", encoding="utf-8") as fp:
            total_written = sum(1 for line in fp if line.strip())
        logger.info("断点续跑: 从第 %d 页继续, 已有 %d 条记录", current_page, total_written)

    mode = "a" if last_page > 0 else "w"

    client = GapensiClient(rate_config=RateLimitConfig(
        min_delay=1.0, max_delay=2.5,
        long_rest_interval=30,
        long_rest_seconds=15.0,
    ))

    logger.info("GAPENSI 爬虫启动: 从第 %d 页开始 (limit=%d)", current_page, PAGE_LIMIT)

    try:
        with output_file.open(mode, encoding="utf-8") as fp:
            while True:
                if max_pages > 0 and (current_page - last_page - 1) >= max_pages:
                    logger.info("已达最大页数限制 %d, 停止", max_pages)
                    break

                path = _build_page_url(current_page, PAGE_LIMIT)
                logger.info("请求第 %d 页: %s", current_page, path)

                try:
                    html = client.get_html(path)
                except RuntimeError as exc:
                    logger.error("请求失败，保存断点: %s", exc)
                    save_checkpoint(checkpoint_file, current_page - 1, total_expected)
                    raise

                # 首页提取总数
                if total_expected == 0:
                    total_expected = extract_total_count(html)
                    if total_expected > 0:
                        total_pages = (total_expected + PAGE_LIMIT - 1) // PAGE_LIMIT
                        logger.info("总记录数: %d, 预计 %d 页", total_expected, total_pages)

                # 解析数据
                records = parse_page(html)

                if not records:
                    empty_streak += 1
                    logger.warning("第 %d 页无数据 (连续 %d 次)", current_page, empty_streak)
                    if empty_streak >= MAX_EMPTY_PAGES:
                        logger.info("连续 %d 页无数据，遍历完毕", MAX_EMPTY_PAGES)
                        break
                    current_page += 1
                    continue

                empty_streak = 0

                # 写入 JSONL
                for record in records:
                    fp.write(record.to_json_line() + "\n")
                    total_written += 1
                fp.flush()

                save_checkpoint(checkpoint_file, current_page, total_expected)

                # 进度日志
                pct = min(total_written / total_expected * 100, 100) if total_expected else 0
                logger.info(
                    "第 %d 页: %d 条 | 累计: %d/%d (%.1f%%)",
                    current_page, len(records), total_written, total_expected, pct,
                )

                current_page += 1

    except KeyboardInterrupt:
        logger.info("用户中断，保存断点")
        save_checkpoint(checkpoint_file, current_page - 1, total_expected)
    finally:
        client.close()

    logger.info("GAPENSI 爬取完成: %d 条记录", total_written)
    return total_written
