"""邮箱补全管道 — Phase 4: 对有官网但 Snov 无邮箱的公司用 Firecrawl+LLM 补全。"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from urllib.parse import urlparse

from .config import EmailAgentConfig
from .service import EmailAgentService

logger = logging.getLogger(__name__)


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    if "://" not in url:
        url = f"https://{url}"
    parsed = urlparse(url)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def run_email_agent_pipeline(
    output_dir: Path,
    project_root: Path,
    max_items: int = 0,
) -> int:
    """
    Phase 4: 对有官网但无邮箱的公司，用 Firecrawl+LLM 从官网抓邮箱。

    读取 companies_with_emails.jsonl，筛选无邮箱但有官网的，
    通过 email_agent 补全邮箱，结果追加到 companies_with_emails.jsonl 并更新原记录。

    返回成功找到邮箱的公司数。
    """
    input_file = output_dir / "companies_with_emails.jsonl"
    checkpoint_file = output_dir / "checkpoint_email_agent.json"

    if not input_file.exists():
        logger.warning("找不到 companies_with_emails.jsonl")
        return 0

    # 加载配置
    config = EmailAgentConfig.from_env(project_root)
    if not config.llm_api_key:
        logger.error("缺少 LLM_API_KEY 环境变量")
        return 0

    # 确保 keys 文件存在
    EmailAgentService.ensure_keys_file(
        config.firecrawl_keys_file,
        project_root / "firecrawl_keys.txt",
    )

    try:
        service = EmailAgentService.from_config(config)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("EmailAgent 初始化失败: %s", exc)
        return 0

    # 断点恢复
    processed_ids: set[str] = set()
    if checkpoint_file.exists():
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        processed_ids = set(data.get("processed_ids", []))

    # 读取所有记录并筛选：有官网但无邮箱
    records: list[dict] = []
    with input_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    pending: list[dict] = []
    # 排除招聘平台等非真实官网
    SKIP_DOMAINS = {"recruiter.co.kr", "jobkorea.co.kr", "saramin.co.kr", "catch.co.kr"}
    for r in records:
        comp_id = r.get("comp_id", "")
        if comp_id in processed_ids:
            continue
        homepage = r.get("homepage", "").strip()
        emails = r.get("emails", [])
        has_emails = bool(emails and any(str(e).strip() for e in emails)) if isinstance(emails, list) else bool(str(emails).strip())
        if homepage and not has_emails:
            # 过滤招聘平台
            domain = _extract_domain(homepage)
            if any(skip in domain for skip in SKIP_DOMAINS):
                continue
            pending.append(r)

    if max_items > 0:
        pending = pending[:max_items]

    if not pending:
        logger.info("EmailAgent: 无待处理记录")
        return 0

    logger.info("EmailAgent: 待处理 %d 条（有官网无邮箱）", len(pending))

    found_count = 0
    processed_count = 0
    # 补全结果写到单独文件，之后合并
    supplement_file = output_dir / "email_agent_results.jsonl"

    try:
        with supplement_file.open("a", encoding="utf-8") as fp:
            for record in pending:
                comp_id = record.get("comp_id", "")
                company_name = record.get("company_name", "")
                homepage = record.get("homepage", "")
                domain = _extract_domain(homepage)

                if not domain:
                    processed_ids.add(comp_id)
                    continue

                try:
                    result = service.enrich_emails(
                        company_name=company_name,
                        domain=domain,
                    )
                except Exception as exc:
                    logger.warning("EmailAgent 失败 (%s): %s", domain, exc)
                    processed_ids.add(comp_id)
                    processed_count += 1
                    continue

                processed_ids.add(comp_id)
                processed_count += 1

                if result.success and result.emails:
                    record["emails"] = result.emails
                    fp.write(json.dumps(record, ensure_ascii=False) + "\n")
                    fp.flush()
                    found_count += 1
                    logger.info(
                        "[%d] %s | %s → %d封邮箱 (Firecrawl+LLM)",
                        processed_count,
                        company_name,
                        domain,
                        len(result.emails),
                    )
                else:
                    logger.debug(
                        "[%d] %s | %s → 未找到邮箱 (%s)",
                        processed_count,
                        company_name,
                        domain,
                        result.error_code,
                    )

                if processed_count % 20 == 0:
                    checkpoint_file.write_text(
                        json.dumps({"processed_ids": sorted(processed_ids)}, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    logger.info("EmailAgent 进度: %d/%d | 找到邮箱: %d",
                                processed_count, len(pending), found_count)

    finally:
        checkpoint_file.write_text(
            json.dumps({"processed_ids": sorted(processed_ids)}, ensure_ascii=False),
            encoding="utf-8",
        )

    # 将补全的邮箱合并回 companies_with_emails.jsonl
    if supplement_file.exists() and found_count > 0:
        _merge_supplements(input_file, supplement_file)

    logger.info("EmailAgent 完成: 处理 %d, 找到邮箱 %d", processed_count, found_count)
    return found_count


def _merge_supplements(main_file: Path, supplement_file: Path) -> None:
    """将补全结果合并回主文件（按 comp_id 更新）。"""
    # 读取补全数据
    supplements: dict[str, dict] = {}
    with supplement_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                r = json.loads(line)
                supplements[r.get("comp_id", "")] = r

    if not supplements:
        return

    # 读取主文件并更新
    updated: list[str] = []
    with main_file.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            comp_id = r.get("comp_id", "")
            if comp_id in supplements:
                r["emails"] = supplements[comp_id]["emails"]
            updated.append(json.dumps(r, ensure_ascii=False))

    main_file.write_text("\n".join(updated) + "\n", encoding="utf-8")
    logger.info("已将 %d 条补全邮箱合并回主文件", len(supplements))
