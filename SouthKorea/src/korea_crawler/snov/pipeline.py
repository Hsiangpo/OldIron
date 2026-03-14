"""Snov 邮箱管道 — 对有官网的公司查询邮箱，失败不标记已处理，支持续跑补齐。"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import SnovClient, SnovConfig, SnovCredentialPool, SnovNoCreditError, SnovRateLimitError, extract_domain, is_valid_domain, load_snov_credentials_from_env

logger = logging.getLogger(__name__)

# Snov API 官方限速: 60 请求/分钟
# 5 并发 + 1s 间隔，超限时 429 重试自动退避
CONCURRENCY = 5
REQUEST_DELAY = 1.0
TOKEN_PATTERN = re.compile(r"(access_token=)[^&\s]+", flags=re.I)


def _sanitize_error_text(text: str) -> str:
    """脱敏异常文本，避免日志打印 token。"""
    if not text:
        return ""
    return TOKEN_PATTERN.sub(r"\1***", text)


def run_snov_pipeline(
    output_dir: Path,
    max_items: int = 0,
    concurrency: int = CONCURRENCY,
    request_delay: float = REQUEST_DELAY,
    input_filename: str = "companies.jsonl",
    output_filename: str = "companies_with_emails.jsonl",
    checkpoint_filename: str = "checkpoint_snov.json",
) -> int:
    """
    查询 Snov 邮箱，输出 companies_with_emails.jsonl。

    429 失败的公司不标记为已处理，下次续跑会重试。

    返回实际查到邮箱的公司数。
    """
    input_file = output_dir / input_filename
    output_file = output_dir / output_filename
    checkpoint_file = output_dir / checkpoint_filename

    if not input_file.exists():
        return 0

    credentials = load_snov_credentials_from_env()
    if not credentials:
        logger.error("缺少 SNOV_CLIENT_ID 或 SNOV_CLIENT_SECRET")
        return 0

    config = SnovConfig(credentials=credentials)
    pool = SnovCredentialPool(credentials, no_credit_cooldown_seconds=3600.0)

    # 每个线程一个 SnovClient
    thread_local = threading.local()

    def _get_snov() -> SnovClient:
        if not hasattr(thread_local, "snov"):
            thread_local.snov = SnovClient(config, credential_pool=pool)
        return thread_local.snov

    # 断点恢复
    processed_ids: set[str] = set()
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        processed_ids = set(data.get("processed_ids", []))

    # 兼容异常中断场景：输出文件可能已写入，但 checkpoint 尚未来得及持久化。
    # 将输出文件中的 comp_id 一并视为已处理，避免重复写入。
    if output_file.exists():
        with output_file.open("r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    comp_id = json.loads(line).get("comp_id", "")
                except Exception:
                    continue
                if comp_id:
                    processed_ids.add(comp_id)

    records: list[dict] = []
    with input_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    # 过滤待处理的
    pending = [r for r in records if r.get("comp_id", "") not in processed_ids]
    if max_items > 0:
        pending = pending[:max_items]

    if not pending:
        return 0

    logger.info("Snov: 待处理 %d 条, 并发=%d", len(pending), concurrency)

    def _lookup_emails(record: dict) -> tuple[dict, list[str], bool, str]:
        """
        Worker: 查询单个公司的邮箱。

        返回 (record, emails, success, reason)。
        success=False 表示限流失败，不应标记为已处理。
        """
        homepage = record.get("homepage", "")
        emails: list[str] = []
        success = True
        reason = ""

        if homepage:
            domain = extract_domain(homepage)
            if is_valid_domain(domain):
                try:
                    snov = _get_snov()
                    emails = snov.get_domain_emails(domain)
                    # 查询成功后稍微休息，避免 429
                    if request_delay > 0:
                        time.sleep(request_delay)
                except (SnovRateLimitError, SnovNoCreditError):
                    # 429 限流 — 不标记为已处理，下次重试
                    logger.warning("Snov 额度/限流跳过 (%s)，下次续跑重试", domain)
                    success = False
                    reason = "snov_retry"
                except Exception as exc:
                    logger.warning("Snov 查询失败 (%s): %s", domain, _sanitize_error_text(str(exc)))
                    # 其他异常（网络抖动/临时错误）也不标记已处理，续跑继续补齐
                    success = False
                    reason = "error"

        return record, emails, success, reason

    found_count = 0
    processed_count = 0
    skipped_429 = 0
    skipped_error = 0
    write_lock = threading.Lock()

    try:
        with (
            output_file.open("a", encoding="utf-8") as fp,
            ThreadPoolExecutor(max_workers=concurrency) as executor,
        ):
            futures = {executor.submit(_lookup_emails, r): r for r in pending}

            for fut in as_completed(futures):
                try:
                    record, emails, success, reason = fut.result()
                    comp_id = record.get("comp_id", "")

                    if not success:
                        # 失败不写入、不标记 processed，留给下次续跑重试
                        if reason == "429":
                            skipped_429 += 1
                        else:
                            skipped_error += 1
                        continue

                    record["emails"] = emails

                    with write_lock:
                        fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                        fp.flush()

                        processed_ids.add(comp_id)
                        processed_count += 1

                        if emails:
                            found_count += 1
                            domain = extract_domain(record.get("homepage", ""))
                            logger.info(
                                "[%d] %s | %s → %d封邮箱",
                                processed_count,
                                record.get("company_name", ""),
                                domain, len(emails),
                            )

                        if processed_count % 20 == 0:
                            checkpoint_file.write_text(
                                json.dumps({"processed_ids": sorted(processed_ids)}, ensure_ascii=False),
                                encoding="utf-8",
                            )
                            logger.info("Snov 进度: %d/%d | 邮箱: %d | 429跳过: %d",
                                        processed_count, len(pending), found_count, skipped_429)

                except Exception as exc:
                    logger.warning("Snov worker 异常: %s", _sanitize_error_text(str(exc)))

    finally:
        checkpoint_file.write_text(
            json.dumps({"processed_ids": sorted(processed_ids)}, ensure_ascii=False),
            encoding="utf-8",
        )

    if skipped_429 > 0:
        logger.warning("Snov: %d 条因 429 限流跳过，下次续跑会重试", skipped_429)
    if skipped_error > 0:
        logger.warning("Snov: %d 条因临时异常跳过，下次续跑会重试", skipped_error)

    logger.info(
        "Snov 完成: 处理 %d, 找到邮箱 %d, 429跳过 %d, 异常跳过 %d",
        processed_count,
        found_count,
        skipped_429,
        skipped_error,
    )
    return found_count
