"""公司名去重工具。"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


def _normalize_name(name: str) -> str:
    """标准化公司名用于去重比较。"""
    # 转小写，移除标点和多余空格
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


def deduplicate(input_file: Path) -> int:
    """
    对 JSONL 文件按公司名去重，输出 final_companies.jsonl。

    去重规则：
    - 按标准化公司名去重（不区分大小写、忽略标点）
    - 同名公司保留第一条

    返回去重后的记录数。
    """
    output_file = input_file.parent / "final_companies.jsonl"

    records: list[dict] = []
    with input_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    seen_names: set[str] = set()
    deduped: list[dict] = []

    for record in records:
        company_name = record.get("company_name", "")
        key = _normalize_name(company_name)

        if key and key in seen_names:
            logger.debug("去重跳过: %s", company_name)
            continue

        if key:
            seen_names.add(key)
        deduped.append(record)

    with output_file.open("w", encoding="utf-8") as fp:
        for record in deduped:
            fp.write(json.dumps(record, ensure_ascii=False) + "\n")

    removed = len(records) - len(deduped)
    if removed > 0:
        logger.info("公司名去重: %d → %d (去除 %d 条重复)", len(records), len(deduped), removed)
    else:
        logger.info("公司名去重: %d 条，无重复", len(deduped))

    return len(deduped)
