"""官网规则邮箱 + 外部 LLM 代表人补充服务。"""

from __future__ import annotations

import json
import html
import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote
from urllib.parse import urlparse

from .client import FirecrawlClient
from .client import FirecrawlClientConfig
from .client import FirecrawlError
from .client import HtmlPageResult
from .domain_cache import FirecrawlDomainCache
from .key_pool import FirecrawlKeyPool
from .key_pool import KeyPoolConfig
from .llm_client import EmailUrlLlmClient
from .normalization import analyze_email_set
from .normalization import extract_registrable_domain
from .normalization import split_emails


LOGGER = logging.getLogger(__name__)
DEFAULT_LLM_BASE_URL = "https://gpt-agent.cc/v1"
DEFAULT_LLM_MODEL = "claude-sonnet-4-6"
DEFAULT_LLM_REASONING_EFFORT = ""
DEFAULT_LLM_API_STYLE = "auto"

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

_EMAIL_RE = re.compile(
    r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})",
    re.IGNORECASE,
)
_HTML_COMMENT_RE = re.compile(r"(?is)<!--.*?-->")
_SCRIPT_LIKE_BLOCK_RE = re.compile(r"(?is)<(script|style|template)\b[^>]*>.*?</\1>")
_NON_ALPHA_RE = re.compile(r"[^a-z0-9]+")
_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".ico", ".avif")
_REPRESENTATIVE_LABELS = (
    "代表取締役",
    "代表社員",
    "代表理事",
    "代表者",
    "社長",
    "院長",
    "理事長",
    "代表",
)
_REPRESENTATIVE_BLOCKER_LABELS = (
    "会社名",
    "商号",
    "所在地",
    "住所",
    "電話",
    "tel",
    "fax",
    "設立",
    "創業",
    "資本金",
    "事業内容",
    "従業員",
    "営業時間",
    "定休日",
    "アクセス",
    "お問い合わせ",
)
_REPRESENTATIVE_BLOCKER_VALUES = {
    "あいさつ",
    "挨拶",
    "ごあいさつ",
    "ご挨拶",
    "メッセージ",
    "プロフィール",
    "会社概要",
    "概要",
    "一覧",
}
_REPRESENTATIVE_BLOCKER_VALUES_LOWER = {value.lower() for value in _REPRESENTATIVE_BLOCKER_VALUES}
_BAD_HOST_KEYWORDS = (
    "googlesyndication.com",
    "doubleclick.net",
    "hotelbeds.com",
    "worldota.net",
    "googleusercontent.com",
    "p.fih.io",
)
_BAD_EMAIL_TLDS = {
    "jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "ico", "avif",
    "mp4", "webm", "mov", "pdf", "js", "css", "woff", "woff2", "ttf", "eot",
}
_BAD_EMAIL_HOST_HINTS = (
    "example.com",
    "example.org",
    "example.net",
    "sample.com",
    "sample.co.jp",
    "mysite.com",
    "mysite.co.jp",
    "eksempel.dk",
    "sentry.io",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
)
_MULTI_LABEL_PUBLIC_SUFFIXES = {
    "co.jp",
    "or.jp",
    "ne.jp",
    "go.jp",
    "ac.jp",
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
}
_EMAIL_PRIORITY_LOCAL_PARTS = {
    "contact",
    "hello",
    "help",
    "hr",
    "info",
    "inquiry",
    "office",
    "privacy",
    "pr",
    "press",
    "recruit",
    "recruiting",
    "sales",
    "service",
    "support",
    "saiyo",
    "soumu",
    "kojinjoho",
    "customer",
}
_SKIP_PAGE_SUFFIXES = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".tar", ".gz", ".rar", ".7z",
    ".exe", ".dmg", ".apk",
)
_MAX_PAGE_HTML_CHARS = 250_000

_KEY_FILE_WRITE_LOCK = threading.Lock()
_KEYWORD_POOL_LOCK = threading.Lock()


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
    return extract_registrable_domain(host)


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
    llm_base_url: str = DEFAULT_LLM_BASE_URL
    llm_model: str = DEFAULT_LLM_MODEL
    llm_reasoning_effort: str = DEFAULT_LLM_REASONING_EFFORT
    llm_api_style: str = DEFAULT_LLM_API_STYLE
    llm_timeout_seconds: float = 120.0
    learned_keyword_file: Path = Path("output/cache/high_value_url_keywords.json")
    map_limit: int = 200
    prefilter_limit: int = 40
    llm_pick_count: int = 16
    extract_max_urls: int = 5
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

    def validate(self, *, require_llm: bool = False) -> None:
        # 协议爬虫不需要 Firecrawl key
        if self.crawl_backend != "protocol":
            if not self.keys_inline and not _keys_file_has_content(self.keys_file):
                raise RuntimeError("Firecrawl 阶段缺少 FIRECRAWL_KEYS，请检查根目录 .env。")
        if require_llm and (not self.llm_api_key or not self.llm_model):
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
    """基于协议/Firecrawl 规则提邮箱、按需用 LLM 补代表人的服务。"""

    def __init__(
        self,
        settings: FirecrawlEmailSettings,
        *,
        key_pool: FirecrawlKeyPool | None = None,
        firecrawl_client: object | None = None,
    ) -> None:
        self._settings = settings
        self._learned_keywords = self._load_learned_keywords()
        self._owns_key_pool = False
        self._owns_firecrawl = False
        # 如果外部已注入 client（如协议爬虫），跳过 key_pool 构建
        if firecrawl_client is not None:
            self._key_pool = key_pool
            self._firecrawl = firecrawl_client
        else:
            self._key_pool = key_pool or self.build_key_pool(settings)
            self._owns_key_pool = key_pool is None
            self._firecrawl = FirecrawlClient(
                key_pool=self._key_pool,
                config=FirecrawlClientConfig(
                    base_url=settings.base_url,
                    timeout_seconds=settings.timeout_seconds,
                    max_retries=settings.max_retries,
                ),
            )
            self._owns_firecrawl = True
        self._llm: EmailUrlLlmClient | None = None

    def close(self) -> None:
        if self._llm is not None and hasattr(self._llm, "close"):
            try:
                self._llm.close()
            except Exception:  # noqa: BLE001
                pass
            finally:
                self._llm = None
        if self._owns_key_pool and self._key_pool is not None:
            self._key_pool.close()
        if self._owns_firecrawl and hasattr(self._firecrawl, "close"):
            try:
                self._firecrawl.close()
            except Exception:  # noqa: BLE001
                pass
        return None

    def _get_llm_client(self) -> EmailUrlLlmClient:
        """按需初始化 LLM，避免纯规则邮箱流程被无意义的 LLM 配置卡住。"""
        if self._llm is not None:
            return self._llm
        self._settings.validate(require_llm=True)
        self._llm = EmailUrlLlmClient(
            api_key=self._settings.llm_api_key,
            base_url=self._settings.llm_base_url,
            model=self._settings.llm_model,
            reasoning_effort=self._settings.llm_reasoning_effort,
            api_style=self._settings.llm_api_style,
            timeout_seconds=self._settings.llm_timeout_seconds,
        )
        return self._llm

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

    def discover_emails(
        self,
        *,
        company_name: str,
        homepage: str,
        domain: str = "",
        existing_representative: str = "",
        secondary_email_lookup: Callable[..., list[str]] | None = None,
        allow_llm_email_extraction: bool = True,
    ) -> EmailDiscoveryResult:
        start_url = self._normalize_start_url(homepage, domain)
        if not start_url:
            return EmailDiscoveryResult(emails=[])
        reliable_representative = self._normalize_existing_representative(existing_representative)
        result = self._discover_pass(
            company_name=company_name,
            start_url=start_url,
            existing_representative=reliable_representative,
            secondary_email_lookup=secondary_email_lookup,
            allow_llm_email_extraction=allow_llm_email_extraction,
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
            raw = str(homepage).strip()
            return raw if self._is_supported_site_url(raw) else ""
        clean_domain = str(domain or "").strip().lower()
        if not clean_domain:
            return ""
        raw = f"https://{clean_domain}"
        return raw if self._is_supported_site_url(raw) else ""

    def _discover_pass(
        self,
        *,
        company_name: str,
        start_url: str,
        existing_representative: str,
        secondary_email_lookup: Callable[..., list[str]] | None,
        allow_llm_email_extraction: bool,
        plan: _EmailPassPlan,
    ) -> EmailDiscoveryResult:
        # 兼容旧调用参数；官网邮箱现在固定只走规则提取。
        _ = allow_llm_email_extraction
        mapped_urls = self._map_site(start_url)
        all_urls = self._rank_all_urls(start_url, mapped_urls)
        final_urls = self._select_urls_for_scrape(
            company_name=company_name,
            start_url=start_url,
            all_urls=all_urls,
            plan=plan,
            use_llm=not bool(str(existing_representative or "").strip()),
        )
        pages = self._scrape_html_pages(final_urls)
        emails = self._extract_rule_emails(start_url, pages)
        if not emails and secondary_email_lookup is not None:
            try:
                fallback_emails = secondary_email_lookup(
                    company_name=company_name,
                    homepage=start_url,
                    pages=[{"url": page.url, "html": page.html} for page in pages if page.html],
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("二级邮箱补充失败：company=%s homepage=%s error=%s", company_name or "-", start_url, exc)
            else:
                emails = self._clean_emails(list(fallback_emails or []))
        representative = str(existing_representative or "").strip()
        extracted_company_name = str(company_name or "").strip()
        evidence_url = final_urls[0] if final_urls else start_url
        evidence_quote = ""
        if not pages:
            return EmailDiscoveryResult(
                company_name=extracted_company_name,
                representative=representative,
                emails=emails,
                evidence_url=evidence_url,
                evidence_quote=evidence_quote,
                contact_form_only=False,
                selected_urls=final_urls,
            )

        if not representative:
            representative, evidence_url, evidence_quote = self._extract_rule_representative(pages)

        need_llm_representative = not representative
        if need_llm_representative:
            llm_pages = _truncate_page_results_for_llm(pages)
            LOGGER.info(
                "代表人补充进入 LLM 抽取：company=%s homepage=%s pages=%d",
                company_name or "-",
                start_url,
                len(llm_pages),
            )
            extracted = self._get_llm_client().extract_contacts_from_html(
                company_name=company_name,
                homepage=start_url,
                pages=[{"url": page.url, "html": page.html} for page in llm_pages if page.html],
                need_emails=False,
            )
            if need_llm_representative:
                representative = str(extracted.representative or "").strip()
            if need_llm_representative and extracted.company_name:
                extracted_company_name = str(extracted.company_name).strip()
            evidence_url = str(extracted.evidence_url or evidence_url).strip()
            evidence_quote = str(extracted.evidence_quote or "").strip()
        else:
            LOGGER.debug("官网补充跳过 LLM：已有代表人 company=%s representative=%s", company_name or "-", representative)

        return EmailDiscoveryResult(
            company_name=extracted_company_name,
            representative=representative,
            emails=emails,
            evidence_url=evidence_url,
            evidence_quote=evidence_quote,
            contact_form_only=False,
            selected_urls=final_urls,
        )

    def _select_urls_for_scrape(
        self,
        *,
        company_name: str,
        start_url: str,
        all_urls: list[str],
        plan: _EmailPassPlan,
        use_llm: bool,
    ) -> list[str]:
        rule_shortlist = self._build_rule_shortlist(
            start_url=start_url,
            all_urls=all_urls,
            limit=max(plan.prefilter_limit, plan.extract_max_urls),
        )
        if not use_llm or len(rule_shortlist) <= plan.extract_max_urls:
            return rule_shortlist[: plan.extract_max_urls]
        ranked_urls = self._get_llm_client().pick_candidate_urls(
            company_name=company_name,
            domain=extract_domain(start_url),
            homepage=start_url,
            candidate_urls=rule_shortlist,
            target_count=plan.llm_pick_count,
            recommended_urls=rule_shortlist[: min(plan.extract_max_urls, len(rule_shortlist))],
        )
        self._remember_keywords_from_urls(ranked_urls)
        return self._build_final_urls(
            start_url,
            ranked_urls,
            rule_shortlist,
            limit=plan.extract_max_urls,
        )

    def _build_rule_shortlist(self, *, start_url: str, all_urls: list[str], limit: int) -> list[str]:
        strong: list[str] = []
        weak: list[str] = []
        for url in all_urls:
            if url == start_url:
                continue
            if not _is_supported_page_url(url):
                continue
            score = self._score_url(start_url, url)
            if score >= 60:
                strong.append(url)
            else:
                weak.append(url)
        shortlist = [start_url]
        for url in strong + weak:
            if url not in shortlist:
                shortlist.append(url)
            if len(shortlist) >= limit:
                break
        return shortlist[:limit]

    def _rank_all_urls(self, start_url: str, mapped_urls: list[str]) -> list[str]:
        """全量 URL 按规则打分排序，不截断。"""
        host = urlparse(start_url).netloc.lower()
        ranked: list[tuple[int, str]] = []
        seen: set[str] = set()
        for raw in [start_url, *mapped_urls]:
            url = str(raw or "").strip()
            if not url or url in seen or not url.startswith("http"):
                continue
            if url != start_url and not _is_supported_page_url(url):
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
        filtered_urls = [url for url in urls if _is_supported_page_url(url)]
        if not filtered_urls:
            return []
        if hasattr(self._firecrawl, "scrape_html_pages"):
            raw_pages = self._scrape_full_html_pages(filtered_urls)
            return _normalize_page_results(raw_pages)
        pages: list[HtmlPageResult] = []
        for url in filtered_urls:
            try:
                page = self._scrape_full_html(url)
            except FirecrawlError as exc:
                if exc.code in {"firecrawl_http_404", "firecrawl_5xx"}:
                    continue
                raise
            html = str(page.html or "")
            if html.strip():
                pages.append(HtmlPageResult(url=page.url, html=html))
        return pages

    def _scrape_full_html_pages(self, urls: list[str]) -> object:
        try:
            return self._firecrawl.scrape_html_pages(urls, truncate_html=False)
        except TypeError:
            return self._firecrawl.scrape_html_pages(urls)

    def _scrape_full_html(self, url: str) -> object:
        try:
            return self._firecrawl.scrape_html(url, truncate_html=False)
        except TypeError:
            return self._firecrawl.scrape_html(url)

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
        score += self._score_learned_keywords(lowered)
        depth = lowered.count("/")
        return score - min(depth, 10)

    def _same_host(self, host: str, url: str) -> bool:
        target = urlparse(url).netloc.lower()
        return bool(target and (target == host or target.endswith(f".{host}") or host.endswith(f".{target}")))

    def _clean_emails(self, emails: list[str]) -> list[str]:
        cleaned = split_emails(emails)
        return sorted(
            cleaned,
            key=lambda item: (-_email_priority_score(item), cleaned.index(item)),
        )

    def _normalize_email_candidate(self, value: object) -> str:
        text = unquote(str(value or "")).strip().lower()
        if not text:
            return ""
        text = text.replace("mailto:", "")
        text = re.sub(r"^(?:u003e|u003c|>|<)+", "", text)
        match = _EMAIL_RE.search(text)
        if match is None:
            return ""
        return str(match.group(1) or "").strip().lower()

    def _extract_rule_emails(self, start_url: str, pages: list[HtmlPageResult]) -> list[str]:
        candidates: list[str] = []
        for page in pages:
            page_emails = self._extract_rule_emails_from_html(page.html)
            analysis = analyze_email_set(start_url, page_emails)
            if analysis.suspicious_directory_like:
                if analysis.same_domain_emails:
                    page_emails = analysis.same_domain_emails
                else:
                    LOGGER.info("协议邮箱跳过疑似目录页：start=%s page=%s emails=%d domains=%d", start_url, page.url, len(analysis.emails), analysis.domain_count)
                    continue
            for email in self._clean_emails(page_emails):
                if email not in candidates:
                    candidates.append(email)
        return self._clean_emails(candidates)

    def _extract_rule_representative(self, pages: list[HtmlPageResult]) -> tuple[str, str, str]:
        for page in pages:
            representative, quote = self._extract_rule_representative_from_html(page.html)
            if representative:
                LOGGER.info("代表人规则提取成功：url=%s representative=%s", page.url, representative)
                return representative, page.url, quote
        return "", "", ""

    def _extract_rule_representative_from_html(self, raw_html: str) -> tuple[str, str]:
        html_text = str(raw_html or "")
        if not html_text.strip():
            return "", ""

        normalized = html.unescape(html_text)
        normalized = _HTML_COMMENT_RE.sub(" ", normalized)
        normalized = _SCRIPT_LIKE_BLOCK_RE.sub(" ", normalized)
        normalized = re.sub(r"(?i)<br\s*/?>", "\n", normalized)
        normalized = re.sub(r"(?i)</(?:p|li|td|th|div|tr|dd|dt|h[1-6]|section|article|ul|ol|table)>", "\n", normalized)
        normalized = re.sub(r"<[^>]+>", " ", normalized)
        normalized = re.sub(r"[ \t\r\f\v]+", " ", normalized)
        raw_lines = re.split(r"\n+", normalized)
        lines = [line.strip(" :：|\t\r\n") for line in raw_lines if line.strip(" :：|\t\r\n")]

        for index, line in enumerate(lines):
            candidate = self._extract_inline_representative_candidate(line)
            if candidate:
                return candidate, line
            if line not in _REPRESENTATIVE_LABELS or index + 1 >= len(lines):
                continue
            next_line = lines[index + 1]
            candidate = self._normalize_representative_candidate(next_line)
            if candidate:
                return candidate, f"{line} {next_line}"
        return "", ""

    def _extract_rule_emails_from_html(self, raw_html: str) -> list[str]:
        html_text = str(raw_html or "")
        if not html_text.strip():
            return []
        normalized = html.unescape(html_text)
        normalized = _HTML_COMMENT_RE.sub(" ", normalized)
        normalized = _SCRIPT_LIKE_BLOCK_RE.sub(" ", normalized)
        normalized = normalized.replace("%40", "@").replace("%2E", ".")
        normalized = re.sub(r"(?i)\[(?:at)\]|\((?:at)\)|\s+at\s+", "@", normalized)
        normalized = re.sub(r"(?i)\[(?:dot)\]|\((?:dot)\)|\s+dot\s+", ".", normalized)
        found: list[str] = []
        for match in _EMAIL_RE.findall(normalized):
            value = str(match or "").strip().lower().rstrip(".,);:]}>")
            if value and value not in found:
                found.append(value)
        return found

    def _normalize_existing_representative(self, value: str) -> str:
        text = str(value or "").strip()
        if text in {"-", "—", "--", "?", "？", "N/A", "n/a", "null", "None"}:
            return ""
        return text

    def _extract_inline_representative_candidate(self, line: str) -> str:
        for label in _REPRESENTATIVE_LABELS:
            match = re.match(rf"^{re.escape(label)}(?:\s+|[:：]\s*)(.+)$", line)
            if match is not None:
                return self._normalize_representative_candidate(match.group(1))
        return ""

    def _normalize_representative_candidate(self, value: str) -> str:
        text = str(value or "").replace("\u3000", " ").strip(" :：|/\t\r\n")
        text = re.sub(r"\s+", " ", text)
        if not text:
            return ""
        lowered = text.lower()
        if lowered in _REPRESENTATIVE_BLOCKER_VALUES_LOWER:
            return ""
        if any(label.lower() in lowered for label in _REPRESENTATIVE_BLOCKER_LABELS):
            return ""
        if any(token in lowered for token in ("http", "www.", "@")):
            return ""
        if any(marker in text for marker in ("株式会社", "有限会社", "合同会社", "合名会社", "合資会社")):
            return ""
        if sum(ch.isdigit() for ch in text) >= 3:
            return ""
        if len(text) < 2 or len(text) > 40:
            return ""
        if len(re.findall(r"[一-龥ぁ-んァ-ヶA-Za-z]", text)) < 2:
            return ""
        return text

    def _is_supported_site_url(self, url: str) -> bool:
        parsed = urlparse(str(url or "").strip())
        if parsed.scheme not in {"http", "https"}:
            return False
        host = parsed.netloc.lower()
        if not host:
            return False
        if any(flag in host for flag in _BAD_HOST_KEYWORDS):
            return False
        if not _is_supported_page_url(url):
            return False
        return True

    def _score_learned_keywords(self, lowered_url: str) -> int:
        score = 0
        normalized_url = self._normalize_for_match(lowered_url)
        for keyword in self._learned_keywords:
            if len(keyword) < 4:
                continue
            if keyword in normalized_url or normalized_url in keyword:
                score += 40
        return score

    def _load_learned_keywords(self) -> list[str]:
        path = self._resolve_learned_keyword_file()
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return []
        values = payload.get("keywords") if isinstance(payload, dict) else payload
        if not isinstance(values, list):
            return []
        result: list[str] = []
        for item in values:
            token = self._normalize_for_match(str(item or ""))
            if token and token not in result:
                result.append(token)
        return result

    def _remember_keywords_from_urls(self, urls: list[str]) -> None:
        tokens: list[str] = []
        for url in urls:
            for token in self._extract_path_keywords(url):
                if token not in tokens:
                    tokens.append(token)
        if not tokens:
            return
        changed = False
        for token in tokens:
            if token not in self._learned_keywords:
                self._learned_keywords.append(token)
                changed = True
        if changed:
            self._save_learned_keywords()

    def _save_learned_keywords(self) -> None:
        path = self._resolve_learned_keyword_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        with _KEYWORD_POOL_LOCK:
            payload = {"keywords": sorted(self._learned_keywords)}
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _resolve_learned_keyword_file(self) -> Path:
        configured = Path(self._settings.learned_keyword_file)
        if configured.is_absolute():
            return configured
        project_root = Path(self._settings.project_root)
        if project_root.name == "output":
            country_root = project_root.parent
        else:
            country_root = project_root
        repo_root = country_root.parent if country_root.parent.exists() else country_root
        return repo_root / configured

    def _extract_path_keywords(self, url: str) -> list[str]:
        parsed = urlparse(str(url or ""))
        parts = [segment for segment in parsed.path.split("/") if segment]
        tokens: list[str] = []
        for part in parts:
            normalized = self._normalize_for_match(part)
            if len(normalized) >= 4 and normalized not in tokens:
                tokens.append(normalized)
        return tokens

    def _normalize_for_match(self, value: str) -> str:
        return _NON_ALPHA_RE.sub("", str(value or "").strip().lower())


def _is_supported_page_url(url: str) -> bool:
    path = (urlparse(str(url or "")).path or "").lower()
    if path.endswith(_IMAGE_EXTENSIONS):
        return False
    return not any(path.endswith(suffix) for suffix in _SKIP_PAGE_SUFFIXES)


def _registrable_domain(host: str) -> str:
    labels = [label for label in str(host or "").strip().lower().split(".") if label]
    if len(labels) < 2:
        return str(host or "").strip().lower()
    suffix2 = ".".join(labels[-2:])
    if suffix2 in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix2


def _email_priority_score(email: str) -> int:
    value = str(email or "").strip().lower()
    if "@" not in value:
        return 0
    local = value.split("@", 1)[0]
    if local in _EMAIL_PRIORITY_LOCAL_PARTS:
        return 100
    normalized = re.sub(r"[^a-z0-9]+", "", local)
    if normalized in _EMAIL_PRIORITY_LOCAL_PARTS:
        return 90
    if any(token in normalized for token in _EMAIL_PRIORITY_LOCAL_PARTS):
        return 70
    if re.fullmatch(r"[a-z]+", normalized):
        return 20
    if re.search(r"\d", normalized):
        return 5
    return 10


def _truncate_page_html(url: str, raw_html: str) -> str:
    text = str(raw_html or "")
    if len(text) <= _MAX_PAGE_HTML_CHARS:
        return text
    half = max(_MAX_PAGE_HTML_CHARS // 2, 1)
    LOGGER.info("协议邮箱页面过长已截断：url=%s 原长=%d", url, len(text))
    return text[:half] + "\n<!-- 页面内容过长已截断 -->\n" + text[-half:]


def _normalize_page_results(raw_pages: list[HtmlPageResult]) -> list[HtmlPageResult]:
    pages: list[HtmlPageResult] = []
    for page in raw_pages:
        url = str(page.url or "").strip()
        if not url or not _is_supported_page_url(url):
            continue
        html_text = str(page.html or "")
        if html_text.strip():
            pages.append(HtmlPageResult(url=url, html=html_text))
    return pages


def _truncate_page_results_for_llm(raw_pages: list[HtmlPageResult]) -> list[HtmlPageResult]:
    pages: list[HtmlPageResult] = []
    for page in raw_pages:
        url = str(page.url or "").strip()
        if not url or not _is_supported_page_url(url):
            continue
        html_text = _truncate_page_html(url, page.html)
        if html_text.strip():
            pages.append(HtmlPageResult(url=url, html=html_text))
    return pages
