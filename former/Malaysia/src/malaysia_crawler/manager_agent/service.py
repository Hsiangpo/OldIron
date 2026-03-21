"""管理人补全服务。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .config import ManagerAgentConfig
from .firecrawl_client import FirecrawlClient
from .firecrawl_client import FirecrawlClientConfig
from .firecrawl_client import FirecrawlError
from .key_pool import FirecrawlKeyPool
from .key_pool import KeyPoolConfig
from .llm_client import ManagerExtractResult
from .llm_client import ManagerLlmClient

_SKIP_EXTENSIONS = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".zip",
    ".rar",
    ".7z",
    ".mp4",
    ".mp3",
    ".avi",
    ".css",
    ".js",
)

_URL_HINTS = [
    "/about-us",
    "/aboutus",
    "/about",
    "/company",
    "/corporate",
    "/management",
    "/leadership",
    "/team",
    "/directors",
    "/board",
    "/who-we-are",
    "/profile",
    "/contact-us",
    "/contact",
]

_ROLE_RE = re.compile(
    r"\b("
    r"manager|managing\s*director|director|executive\s*director|"
    r"ceo|chief\s*executive\s*officer|"
    r"founder|co[-\s]*founder|owner|chair(man|person)?|president|"
    r"partner|principal|general\s*manager|head\s+of"
    r")\b",
    re.IGNORECASE,
)
_INVALID_NAME_RE = re.compile(
    r"(contact|support|email|enquiry|inquiry|manager|managing\s*director|privacy|policy)",
    re.IGNORECASE,
)


@dataclass(slots=True)
class ManagerAgentResult:
    success: bool
    manager_name: str
    manager_role: str
    evidence_url: str
    evidence_quote: str
    candidate_pool: list[str]
    tried_urls: list[str]
    error_code: str
    error_text: str
    retry_after: float


class ManagerAgentService:
    """执行 map + LLM 选链 + 抽取管理人。"""

    def __init__(
        self,
        *,
        config: ManagerAgentConfig,
        firecrawl: FirecrawlClient,
        llm: ManagerLlmClient,
    ) -> None:
        self.config = config
        self._firecrawl = firecrawl
        self._llm = llm

    @classmethod
    def from_config(cls, config: ManagerAgentConfig) -> "ManagerAgentService":
        keys = FirecrawlKeyPool.load_keys(config.firecrawl_keys_file)
        pool = FirecrawlKeyPool(
            keys=keys,
            key_file=config.firecrawl_keys_file,
            db_path=config.firecrawl_pool_db,
            config=KeyPoolConfig(
                per_key_limit=config.firecrawl_key_per_limit,
                wait_seconds=config.firecrawl_key_wait_seconds,
                cooldown_seconds=config.firecrawl_key_cooldown_seconds,
                failure_threshold=config.firecrawl_key_failure_threshold,
            ),
        )
        firecrawl = FirecrawlClient(
            key_pool=pool,
            config=FirecrawlClientConfig(
                base_url=config.firecrawl_base_url,
                timeout_seconds=config.firecrawl_timeout_seconds,
                max_retries=config.firecrawl_max_retries,
                map_limit=config.map_limit,
            ),
        )
        llm = ManagerLlmClient(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
            model=config.llm_model,
            reasoning_effort=config.llm_reasoning_effort,
            timeout_seconds=config.llm_timeout_seconds,
        )
        return cls(config=config, firecrawl=firecrawl, llm=llm)

    @staticmethod
    def ensure_keys_file(target_path: Path, seed_path: Path) -> None:
        if target_path.exists() and target_path.read_text(encoding="utf-8").strip():
            return
        if not seed_path.exists():
            raise FileNotFoundError(f"找不到 Firecrawl seed key 文件：{seed_path}")
        content = seed_path.read_text(encoding="utf-8").strip()
        if not content:
            raise ValueError(f"Firecrawl seed key 文件为空：{seed_path}")
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(content + "\n", encoding="utf-8")

    def enrich_manager(
        self,
        *,
        company_name: str,
        domain: str,
        candidate_pool: list[str],
        tried_urls: list[str],
    ) -> ManagerAgentResult:
        if not domain.strip():
            return self._failed("invalid_domain", "缺少可用域名。", candidate_pool, tried_urls)
        pool = self._prepare_pool(domain=domain, candidate_pool=candidate_pool)
        available = [url for url in pool if url not in set(tried_urls)]
        if not available:
            return self._failed("no_candidate", "没有可尝试的候选链接。", pool, tried_urls)
        picked = self._llm.pick_urls(
            company_name=company_name,
            domain=domain,
            round_index=max(1, len(tried_urls) // max(self.config.pick_per_round, 1) + 1),
            pick_count=self.config.pick_per_round,
            candidate_urls=available,
            tried_urls=tried_urls,
        )
        if not picked:
            picked = self._fallback_pick(available, self.config.pick_per_round)
        if not picked:
            return self._failed("pick_empty", "LLM 未返回可用链接。", pool, tried_urls)
        pages = []
        new_tried = list(tried_urls)
        for url in picked[: max(self.config.fetch_per_round, 1)]:
            if url not in new_tried:
                new_tried.append(url)
            try:
                page = self._firecrawl.scrape_page(url)
            except FirecrawlError as exc:
                retry_after = float(exc.retry_after or 0)
                if exc.code == "firecrawl_429":
                    return ManagerAgentResult(
                        success=False,
                        manager_name="",
                        manager_role="",
                        evidence_url="",
                        evidence_quote="",
                        candidate_pool=pool,
                        tried_urls=new_tried,
                        error_code=exc.code,
                        error_text=str(exc),
                        retry_after=retry_after,
                    )
                return self._failed(exc.code, str(exc), pool, new_tried)
            markdown = str(page.get("markdown", "") or "").strip()
            raw_html = str(page.get("raw_html", "") or "").strip()
            if not markdown and not raw_html:
                continue
            snippet = markdown[:12000] if markdown else raw_html[:12000]
            pages.append({"url": str(page.get("url", url)), "content": snippet})
        if not pages:
            return self._failed("empty_pages", "候选链接抓取后无有效文本。", pool, new_tried)
        extracted = self._llm.extract_manager(
            company_name=company_name,
            domain=domain,
            pages=pages,
        )
        if not self._is_valid_manager(extracted):
            return self._failed("manager_not_found", "未提取到有效 manager。", pool, new_tried)
        return ManagerAgentResult(
            success=True,
            manager_name=extracted.manager_name.strip(),
            manager_role=extracted.manager_role.strip(),
            evidence_url=extracted.evidence_url.strip(),
            evidence_quote=extracted.evidence_quote.strip(),
            candidate_pool=pool,
            tried_urls=new_tried,
            error_code="",
            error_text="",
            retry_after=0.0,
        )

    def _prepare_pool(self, *, domain: str, candidate_pool: list[str]) -> list[str]:
        cleaned = [self._normalize_url(item) for item in candidate_pool]
        cleaned = [item for item in cleaned if item]
        if cleaned:
            return list(dict.fromkeys(cleaned))
        mapped = self._firecrawl.map_urls(
            f"https://{domain}",
            include_subdomains=self.config.map_include_subdomains,
        )
        normalized = [self._normalize_url(item) for item in mapped]
        return list(dict.fromkeys([item for item in normalized if item]))

    def _normalize_url(self, url: str) -> str:
        value = str(url or "").strip()
        if not value:
            return ""
        if value.startswith("//"):
            value = "https:" + value
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"}:
            return ""
        if not parsed.netloc:
            return ""
        lower_path = parsed.path.lower()
        for suffix in _SKIP_EXTENSIONS:
            if lower_path.endswith(suffix):
                return ""
        return value

    def _fallback_pick(self, candidates: list[str], limit: int) -> list[str]:
        scored: list[tuple[int, str]] = []
        for url in candidates:
            score = 0
            lower = url.lower()
            for index, hint in enumerate(_URL_HINTS):
                if hint in lower:
                    score += 100 - index
            depth = lower.count("/")
            score -= min(depth, 12)
            scored.append((score, url))
        scored.sort(key=lambda item: item[0], reverse=True)
        picked = [url for _, url in scored[: max(limit, 1)]]
        return list(dict.fromkeys(picked))

    def _is_valid_manager(self, result: ManagerExtractResult) -> bool:
        name = result.manager_name.strip()
        role = result.manager_role.strip()
        if not name or not role:
            return False
        if not _ROLE_RE.search(role):
            return False
        if len(name) > 120:
            return False
        if _INVALID_NAME_RE.search(name):
            return False
        if "http://" in name.lower() or "https://" in name.lower():
            return False
        return True

    def _failed(
        self,
        code: str,
        message: str,
        candidate_pool: list[str],
        tried_urls: list[str],
    ) -> ManagerAgentResult:
        return ManagerAgentResult(
            success=False,
            manager_name="",
            manager_role="",
            evidence_url="",
            evidence_quote="",
            candidate_pool=candidate_pool,
            tried_urls=tried_urls,
            error_code=code,
            error_text=message,
            retry_after=0.0,
        )
