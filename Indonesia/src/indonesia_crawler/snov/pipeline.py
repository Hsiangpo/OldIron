"""Snov 邮箱补全流水线。"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from .client import SnovClient, SnovConfig, SnovRateLimitError, extract_domain

logger = logging.getLogger(__name__)


def _load_processed_ids(checkpoint_file: Path, output_file: Path) -> set[str]:
    """读取断点并合并输出文件中的已处理 ID。"""
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
                    comp_id = str(json.loads(line).get("comp_id", "")).strip()
                except json.JSONDecodeError:
                    continue
                if comp_id:
                    processed.add(comp_id)
    return processed


def _deduplicate_emails(emails: list[str]) -> list[str]:
    """邮箱去重并统一小写。"""
    seen: set[str] = set()
    output: list[str] = []
    for email in emails:
        normalized = email.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def run_snov_pipeline(output_dir: Path, max_items: int = 0, input_filename: str = "") -> int:
    """执行 Snov 补全，输出 `companies_with_emails.jsonl`。"""
    input_file = output_dir / input_filename if input_filename else output_dir / "companies_with_ceo.jsonl"
    if not input_file.exists() and not input_filename:
        input_file = output_dir / "companies.jsonl"
    output_file = output_dir / "companies_with_emails.jsonl"
    checkpoint_file = output_dir / "checkpoint_snov.json"

    if not input_file.exists():
        logger.warning("Snov 阶段缺少输入文件，已跳过")
        return 0

    client_id = os.getenv("SNOV_CLIENT_ID", "").strip()
    client_secret = os.getenv("SNOV_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        logger.warning("缺少 SNOV_CLIENT_ID/SNOV_CLIENT_SECRET，跳过 Snov 阶段")
        return 0

    config = SnovConfig(client_id=client_id, client_secret=client_secret)
    client = SnovClient(config)

    records: list[dict] = []
    with input_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    processed = _load_processed_ids(checkpoint_file, output_file)
    pending = [record for record in records if str(record.get("comp_id", "")).strip() not in processed]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        logger.info("Snov 阶段无待处理记录")
        client.close()
        return 0

    found_count = 0
    written = 0
    try:
        with output_file.open("a", encoding="utf-8") as fp:
            for record in pending:
                comp_id = str(record.get("comp_id", "")).strip()
                homepage = str(record.get("homepage", "")).strip()
                existing = [str(v) for v in record.get("emails", [])] if isinstance(record.get("emails"), list) else []

                snov_emails: list[str] = []
                if homepage:
                    domain = extract_domain(homepage)
                    if domain:
                        try:
                            snov_emails = client.get_domain_emails(domain)
                        except SnovRateLimitError as exc:
                            logger.warning("Snov 限流，保留断点后停止: %s", exc)
                            break
                        except Exception as exc:  # noqa: BLE001
                            logger.warning("Snov 查询失败 %s: %s", domain, exc)
                        time.sleep(1.0)

                merged = _deduplicate_emails(existing + snov_emails)
                record["emails"] = merged
                if snov_emails:
                    found_count += 1

                fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                fp.flush()
                if comp_id:
                    processed.add(comp_id)
                written += 1

                if written % 20 == 0:
                    checkpoint_file.write_text(
                        json.dumps({"processed_ids": sorted(processed)}, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    logger.info("Snov 进度: %d/%d（命中邮箱 %d）", written, len(pending), found_count)
    finally:
        client.close()
        checkpoint_file.write_text(
            json.dumps({"processed_ids": sorted(processed)}, ensure_ascii=False),
            encoding="utf-8",
        )

    logger.info("Snov 阶段完成: 写入 %d 条，命中 %d 条", written, found_count)
    return written
