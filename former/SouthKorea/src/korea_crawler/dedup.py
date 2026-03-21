"""域名去重工具 — 根据公司官网域名去重，同域优先保留“邮箱更完整”的记录。"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def _extract_root_domain(url: str) -> str:
    """从 URL 提取根域名用于去重。"""
    if not url:
        return ""
    if "://" not in url:
        url = f"https://{url}"
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def _record_score(record: dict) -> tuple[int, int, int, int]:
    """同域冲突时的保留优先级：有邮箱 > 邮箱数量 > 有CEO > 有公司名。"""
    emails = record.get("emails", [])
    email_list: list[str] = []
    if isinstance(emails, list):
        email_list = [str(item).strip() for item in emails if str(item).strip()]
    has_email = 1 if email_list else 0
    has_ceo = 1 if str(record.get("ceo", "")).strip() else 0
    has_name = 1 if str(record.get("company_name", "")).strip() else 0
    return has_email, len(email_list), has_ceo, has_name


def deduplicate_by_domain(input_file: Path) -> int:
    """
    对 JSONL 文件按域名去重，输出 final_companies.jsonl。

    去重规则：
    - 有域名的公司按域名去重（同域名保留第一条）
    - 没有域名的公司全部保留（因为无法判断是否重复）

    返回去重后的记录数。
    """
    output_file = input_file.parent / "final_companies.jsonl"

    records: list[dict] = []
    with input_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    domain_best: dict[str, dict] = {}
    slots: list[tuple[str, str | dict]] = []

    for record in records:
        homepage = record.get("homepage", "")
        domain = _extract_root_domain(homepage)

        if domain:
            if domain not in domain_best:
                domain_best[domain] = record
                slots.append(("domain", domain))
                continue
            if _record_score(record) > _record_score(domain_best[domain]):
                domain_best[domain] = record
            logger.debug("同域替换/跳过: %s (%s)", record.get("company_name", ""), domain)
            continue
        slots.append(("record", record))

    deduped: list[dict] = []
    for kind, value in slots:
        if kind == "domain":
            deduped.append(domain_best[str(value)])
        else:
            deduped.append(value if isinstance(value, dict) else {})

    with output_file.open("w", encoding="utf-8") as fp:
        for record in deduped:
            fp.write(json.dumps(record, ensure_ascii=False) + "\n")

    removed = len(records) - len(deduped)
    if removed > 0:
        logger.info("域名去重: %d → %d (去除 %d 条重复)", len(records), len(deduped), removed)
    else:
        logger.info("域名去重: %d 条，无重复", len(deduped))

    return len(deduped)
