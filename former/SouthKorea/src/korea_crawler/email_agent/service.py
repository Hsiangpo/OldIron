"""邮箱补全服务 — Firecrawl map + LLM 选链 + 抽取邮箱。"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .config import EmailAgentConfig
from .firecrawl_client import FirecrawlClient, FirecrawlClientConfig, FirecrawlError
from .key_pool import FirecrawlKeyPool, KeyPoolConfig
from .llm_client import EmailLlmClient, EmailExtractResult

logger = logging.getLogger(__name__)

# 关键词过滤：优先爬 contact/about/team 页面
_CONTACT_KEYWORDS = re.compile(
    r"(contact|about|team|company|support|inquiry|enquiry|문의|회사소개|연락)", re.I
)


@dataclass(slots=True)
class EmailAgentResult:
    success: bool
    emails: list[str]
    evidence_url: str
    tried_urls: list[str]
    error_code: str
    error_text: str


class EmailAgentService:
    """执行 map + LLM 选链 + 抽取邮箱。"""

    def __init__(
        self,
        *,
        config: EmailAgentConfig,
        firecrawl: FirecrawlClient,
        llm: EmailLlmClient,
    ) -> None:
        self._config = config
        self._firecrawl = firecrawl
        self._llm = llm

    @classmethod
    def from_config(cls, config: EmailAgentConfig) -> "EmailAgentService":
        keys_file = config.firecrawl_keys_file
        keys = FirecrawlKeyPool.load_keys(keys_file)

        key_pool = FirecrawlKeyPool(
            keys=keys,
            key_file=keys_file,
            db_path=config.firecrawl_pool_db,
            config=KeyPoolConfig(
                per_key_limit=config.firecrawl_key_per_limit,
                wait_seconds=config.firecrawl_key_wait_seconds,
                cooldown_seconds=config.firecrawl_key_cooldown_seconds,
                failure_threshold=config.firecrawl_key_failure_threshold,
            ),
        )

        firecrawl = FirecrawlClient(
            key_pool=key_pool,
            config=FirecrawlClientConfig(
                base_url=config.firecrawl_base_url,
                timeout_seconds=config.firecrawl_timeout_seconds,
                max_retries=config.firecrawl_max_retries,
                only_main_content=True,
                map_limit=config.map_limit,
            ),
        )

        llm = EmailLlmClient(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
            model=config.llm_model,
            reasoning_effort=config.llm_reasoning_effort,
            timeout_seconds=config.llm_timeout_seconds,
        )

        return cls(config=config, firecrawl=firecrawl, llm=llm)

    @staticmethod
    def ensure_keys_file(target_path: Path, seed_path: Path) -> None:
        if target_path.exists():
            return
        if seed_path.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(seed_path.read_text(encoding="utf-8"), encoding="utf-8")

    def enrich_emails(
        self,
        *,
        company_name: str,
        domain: str,
    ) -> EmailAgentResult:
        """对单个公司官网进行邮箱挖掘。"""
        tried_urls: list[str] = []
        candidate_pool: list[str] = []

        # Step 1: Firecrawl map 获取站点 URL
        base_url = f"https://{domain}" if "://" not in domain else domain
        try:
            all_urls = self._firecrawl.map_urls(
                base_url,
                include_subdomains=self._config.map_include_subdomains,
            )
        except FirecrawlError as exc:
            logger.debug("map 失败 (%s): %s，尝试直接 scrape", domain, exc)
            all_urls = []
        except Exception as exc:
            logger.debug("map 异常 (%s): %s，尝试直接 scrape", domain, exc)
            all_urls = []

        # 如果 map 没结果，构造常见路径作为候选
        if not all_urls:
            all_urls = [
                base_url,
                f"{base_url}/contact",
                f"{base_url}/contact-us",
                f"{base_url}/about",
                f"{base_url}/about-us",
                f"{base_url}/company",
            ]

        # 优先选 contact/about 页面
        priority_urls = [u for u in all_urls if _CONTACT_KEYWORDS.search(u)]
        other_urls = [u for u in all_urls if u not in set(priority_urls)]
        candidate_pool = priority_urls + other_urls

        # Step 2: 多轮 LLM 选链 + scrape + 抽取
        all_emails: list[str] = []
        seen_emails: set[str] = set()

        for round_idx in range(self._config.max_rounds):
            remaining = [u for u in candidate_pool if u not in set(tried_urls)]
            if not remaining:
                break

            # LLM 选 URL
            try:
                picked = self._llm.pick_urls(
                    company_name=company_name,
                    domain=domain,
                    candidate_urls=remaining[:100],
                    pick_count=self._config.pick_per_round,
                )
            except Exception as exc:
                logger.warning("LLM pick_urls 失败 (%s): %s", domain, exc)
                # 回退：取前几个 contact 页面
                picked = remaining[:self._config.pick_per_round]

            if not picked:
                picked = remaining[:self._config.pick_per_round]

            # Scrape 选中的页面
            pages: list[dict[str, str]] = []
            for url in picked:
                tried_urls.append(url)
                try:
                    page = self._firecrawl.scrape_page(url)
                    if page.get("markdown", "").strip():
                        pages.append(page)
                except FirecrawlError as exc:
                    logger.debug("scrape 失败 (%s): %s", url, exc)
                except Exception as exc:
                    logger.debug("scrape 异常 (%s): %s", url, exc)

            if not pages:
                continue

            # LLM 抽取邮箱
            try:
                result = self._llm.extract_emails(
                    company_name=company_name,
                    domain=domain,
                    pages=pages,
                )
                for email in result.emails:
                    if email not in seen_emails:
                        seen_emails.add(email)
                        all_emails.append(email)
            except Exception as exc:
                logger.warning("LLM extract_emails 失败 (%s): %s", domain, exc)

            # 如果已经找到邮箱就可以停了
            if all_emails:
                break

        if all_emails:
            return EmailAgentResult(
                success=True, emails=all_emails,
                evidence_url=tried_urls[0] if tried_urls else "",
                tried_urls=tried_urls, error_code="", error_text="",
            )

        return self._failed("no_emails_found", f"未从 {domain} 找到邮箱", tried_urls)

    @staticmethod
    def _failed(code: str, message: str, tried_urls: list[str]) -> EmailAgentResult:
        return EmailAgentResult(
            success=False, emails=[], evidence_url="",
            tried_urls=tried_urls, error_code=code, error_text=message,
        )
