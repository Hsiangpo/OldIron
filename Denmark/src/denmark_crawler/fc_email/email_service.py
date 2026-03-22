"""Firecrawl + 外部 LLM 联系信息服务。"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .client import FirecrawlClient
from .client import FirecrawlClientConfig
from .client import FirecrawlError
from .client import HtmlPageResult
from .domain_cache import FirecrawlDomainCache
from .key_pool import FirecrawlKeyPool
from .key_pool import KeyPoolConfig
from .llm_client import EmailUrlLlmClient


_URL_KEYWORDS = {
    "contact": 100,
    "support": 95,
    "help": 90,
    "customer": 85,
    "about": 80,
    "team": 78,
    "leadership": 76,
    "management": 74,
    "director": 72,
    "board": 70,
    "privacy": 68,
    "legal": 66,
    "imprint": 64,
    "career": 62,
    "job": 60,
    "press": 58,
    "media": 56,
    "terms": 54,
    "policy": 52,
    ".pdf": 50,
}

_IGNORE_LOCAL_PARTS = {
    "x",
    "xx",
    "xxx",
    "test",
    "example",
    "sample",
    "yourname",
    "youremail",
    "email",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
}

_KEY_FILE_WRITE_LOCK = threading.Lock()


def extract_domain(website_url: str) -> str:
    """从 URL 提取域名。"""
    raw = str(website_url or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def _keys_file_has_content(keys_file: Path) -> bool:
    if not keys_file.exists():
        return False
    return bool(str(keys_file.read_text(encoding="utf-8", errors="replace")).strip())


@dataclass(slots=True)
class FirecrawlEmailSettings:
    project_root: Path = Path(".")
    keys_inline: list[str] | None = None
    keys_file: Path = Path("output/firecrawl_keys.txt")
    pool_db: Path = Path("output/cache/firecrawl_keys.db")
    domain_cache_db: Path = Path("output/firecrawl_cache.db")
    base_url: str = "https://api.firecrawl.dev/v2/"
    timeout_seconds: float = 45.0
    max_retries: int = 2
    key_per_limit: int = 2
    key_wait_seconds: int = 20
    key_cooldown_seconds: int = 90
    key_failure_threshold: int = 5
    llm_api_key: str = ""
    llm_base_url: str = "https://api.gpteamservices.com/v1"
    llm_model: str = "gpt-5.1-codex-mini"
    llm_reasoning_effort: str = "medium"
    llm_timeout_seconds: float = 120.0
    map_limit: int = 200
    prefilter_limit: int = 40
    llm_pick_count: int = 16
    extract_max_urls: int = 12
    zero_retry_seconds: float = 43200.0
    contact_form_retry_seconds: float = 259200.0
    per_key_limit: int = 0
    candidate_limit: int = 0
    llm_pick_limit: int = 0
    crawl_backend: str = "firecrawl"

    def __post_init__(self) -> None:
        self.keys_inline = list(self.keys_inline or [])
        if self.per_key_limit > 0:
            self.key_per_limit = self.per_key_limit
        if self.candidate_limit > 0:
            self.prefilter_limit = self.candidate_limit
        if self.llm_pick_limit > 0:
            self.llm_pick_count = self.llm_pick_limit

    def validate(self) -> None:
        # 协议爬虫不需要 Firecrawl key
        if self.crawl_backend != "protocol":
            if not self.keys_inline and not _keys_file_has_content(self.keys_file):
                raise RuntimeError("Firecrawl 阶段缺少 FIRECRAWL_KEYS，请检查根目录 .env。")
        if not self.llm_api_key or not self.llm_model:
            raise RuntimeError("Firecrawl 阶段缺少 LLM 配置，请检查 LLM_API_KEY / LLM_MODEL。")


FirecrawlEmailServiceConfig = FirecrawlEmailSettings


@dataclass(slots=True)
class EmailDiscoveryResult:
    emails: list[str]
    company_name: str = ""
    representative: str = ""
    evidence_url: str = ""
    evidence_quote: str = ""
    contact_form_only: bool = False
    selected_urls: list[str] | None = None
    retry_after_seconds: float = 0.0


@dataclass(slots=True)
class _EmailPassPlan:
    prefilter_limit: int
    llm_pick_count: int
    extract_max_urls: int


class FirecrawlEmailService:
    """基于 Firecrawl 与外部 LLM 的邮箱发现服务。"""

    def __init__(
        self,
        settings: FirecrawlEmailSettings,
        *,
        key_pool: FirecrawlKeyPool | None = None,
        firecrawl_client: object | None = None,
    ) -> None:
        self._settings = settings
        # 如果外部已注入 client（如协议爬虫），跳过 key_pool 构建
        if firecrawl_client is not None:
            self._key_pool = key_pool
            self._firecrawl = firecrawl_client
        else:
            self._key_pool = key_pool or self.build_key_pool(settings)
            self._firecrawl = FirecrawlClient(
                key_pool=self._key_pool,
                config=FirecrawlClientConfig(
                    base_url=settings.base_url,
                    timeout_seconds=settings.timeout_seconds,
                    max_retries=settings.max_retries,
                ),
            )
        self._llm = EmailUrlLlmClient(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
            reasoning_effort=settings.llm_reasoning_effort,
            timeout_seconds=settings.llm_timeout_seconds,
        )

    def close(self) -> None:
        if self._key_pool is not None:
            self._key_pool.close()
        return None

    @staticmethod
    def ensure_keys_file(target_path: Path, inline_keys: list[str]) -> None:
        cleaned = [str(item).strip() for item in inline_keys if str(item).strip()]
        if not cleaned:
            if _keys_file_has_content(target_path):
                return
            raise ValueError("Firecrawl keys 为空，请检查根目录 .env 中的 FIRECRAWL_KEYS。")
        unique: list[str] = []
        for item in cleaned:
            if item not in unique:
                unique.append(item)
        with _KEY_FILE_WRITE_LOCK:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if target_path.exists():
                current = target_path.read_text(encoding="utf-8", errors="replace").strip()
                if current:
                    return
            target_path.write_text("\n".join(unique) + "\n", encoding="utf-8")

    @staticmethod
    def build_key_pool(settings: FirecrawlEmailSettings) -> FirecrawlKeyPool:
        FirecrawlEmailService.ensure_keys_file(settings.keys_file, settings.keys_inline)
        keys = FirecrawlKeyPool.load_keys(settings.keys_file)
        return FirecrawlKeyPool(
            keys=keys,
            key_file=settings.keys_file,
            db_path=settings.pool_db,
            config=KeyPoolConfig(
                per_key_limit=settings.key_per_limit,
                wait_seconds=settings.key_wait_seconds,
                cooldown_seconds=settings.key_cooldown_seconds,
                failure_threshold=settings.key_failure_threshold,
            ),
        )

    def build_domain_cache(self, db_path: Path) -> FirecrawlDomainCache:
        return FirecrawlDomainCache(db_path)

    def seed_domain_cache(self, cache: FirecrawlDomainCache, pairs: list[tuple[str, list[str]]]) -> None:
        cache.seed_done(pairs)

    def get_domain_emails(self, domain: str) -> list[str]:
        return self.discover_emails(company_name="", homepage=domain, domain=domain).emails

    def discover_emails(self, *, company_name: str, homepage: str, domain: str = "") -> EmailDiscoveryResult:
        start_url = self._normalize_start_url(homepage, domain)
        if not start_url:
            return EmailDiscoveryResult(emails=[])
        result = self._discover_pass(
            company_name=company_name,
            start_url=start_url,
            plan=_EmailPassPlan(
                prefilter_limit=max(self._settings.prefilter_limit, 1),
                llm_pick_count=max(self._settings.llm_pick_count, 1),
                extract_max_urls=max(self._settings.extract_max_urls, 1),
            ),
        )
        if result.emails:
            return result
        if result.contact_form_only:
            result.retry_after_seconds = max(float(self._settings.contact_form_retry_seconds), 1.0)
            return result
        result.retry_after_seconds = max(float(self._settings.zero_retry_seconds), 1.0)
        return result

    def _normalize_start_url(self, homepage: str, domain: str) -> str:
        if str(homepage or "").strip().startswith("http"):
            return str(homepage).strip()
        clean_domain = str(domain or "").strip().lower()
        if not clean_domain:
            return ""
        return f"https://{clean_domain}"

    def _discover_pass(self, *, company_name: str, start_url: str, plan: _EmailPassPlan) -> EmailDiscoveryResult:
        mapped_urls = self._map_site(start_url)
        # 全量 URL 排序（不截断），规则推荐仅作参考
        all_urls = self._rank_all_urls(start_url, mapped_urls)
        recommended = all_urls[:plan.prefilter_limit]
        ranked_urls = self._llm.pick_candidate_urls(
            company_name=company_name,
            domain=extract_domain(start_url),
            homepage=start_url,
            candidate_urls=all_urls,
            target_count=plan.llm_pick_count,
            recommended_urls=recommended,
        )
        final_urls = self._build_final_urls(
            start_url,
            ranked_urls,
            all_urls,
            limit=plan.extract_max_urls,
        )
        pages = self._scrape_html_pages(final_urls)
        extracted = self._llm.extract_contacts_from_html(
            company_name=company_name,
            homepage=start_url,
            pages=[{"url": page.url, "html": page.html} for page in pages if page.html],
        )
        emails = self._filter_same_domain_emails(start_url, extracted.emails)
        if not emails:
            emails = extracted.emails
        emails = self._clean_emails(emails)
        return EmailDiscoveryResult(
            company_name=extracted.company_name,
            representative=extracted.representative,
            emails=emails,
            evidence_url=extracted.evidence_url,
            evidence_quote=extracted.evidence_quote,
            contact_form_only=False,
            selected_urls=final_urls,
        )

    def _rank_all_urls(self, start_url: str, mapped_urls: list[str]) -> list[str]:
        """全量 URL 按规则打分排序，不截断。"""
        host = urlparse(start_url).netloc.lower()
        ranked: list[tuple[int, str]] = []
        seen: set[str] = set()
        for raw in [start_url, *mapped_urls]:
            url = str(raw or "").strip()
            if not url or url in seen or not url.startswith("http"):
                continue
            if not self._same_host(host, url):
                continue
            seen.add(url)
            ranked.append((self._score_url(start_url, url), url))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [url for _score, url in ranked]

    def _build_final_urls(self, start_url: str, ranked_urls: list[str], candidate_urls: list[str], *, limit: int) -> list[str]:
        urls: list[str] = []
        for url in [start_url, *ranked_urls, *candidate_urls]:
            value = str(url or "").strip()
            if value and value not in urls:
                urls.append(value)
            if len(urls) >= limit:
                break
        return urls

    def _scrape_html_pages(self, urls: list[str]) -> list[HtmlPageResult]:
        if hasattr(self._firecrawl, "scrape_html_pages"):
            return self._firecrawl.scrape_html_pages(urls)
        pages: list[HtmlPageResult] = []
        for url in urls:
            try:
                page = self._firecrawl.scrape_html(url)
            except FirecrawlError as exc:
                if exc.code in {"firecrawl_http_404", "firecrawl_5xx"}:
                    continue
                raise
            if page.html.strip():
                pages.append(page)
        return pages

    def _map_site(self, start_url: str) -> list[str]:
        if type(self._firecrawl).__name__ == "GoFirecrawlService":
            return self._firecrawl.map_site(
                homepage=start_url,
                domain=extract_domain(start_url),
                limit=self._settings.map_limit,
            )
        return self._firecrawl.map_site(
            start_url,
            limit=self._settings.map_limit,
        )

    def _score_url(self, start_url: str, url: str) -> int:
        if url == start_url:
            return 1000
        lowered = url.lower()
        score = 0
        for keyword, weight in _URL_KEYWORDS.items():
            if keyword in lowered:
                score += weight
        depth = lowered.count("/")
        return score - min(depth, 10)

    def _same_host(self, host: str, url: str) -> bool:
        target = urlparse(url).netloc.lower()
        return bool(target and (target == host or target.endswith(f".{host}") or host.endswith(f".{target}")))

    def _filter_same_domain_emails(self, start_url: str, emails: list[str]) -> list[str]:
        domain = extract_domain(start_url)
        if not domain:
            return []
        matched: list[str] = []
        suffix = domain.lower()
        for email in emails:
            value = str(email or "").strip().lower()
            if not value or "@" not in value:
                continue
            email_domain = value.split("@", 1)[1]
            if email_domain == suffix or email_domain.endswith(f".{suffix}"):
                if value not in matched:
                    matched.append(value)
        return matched

    def _clean_emails(self, emails: list[str]) -> list[str]:
        cleaned: list[str] = []
        for email in emails:
            value = str(email or "").strip().lower()
            if not value or "@" not in value:
                continue
            local = value.split("@", 1)[0]
            if local in _IGNORE_LOCAL_PARTS:
                continue
            if value not in cleaned:
                cleaned.append(value)
        return cleaned

