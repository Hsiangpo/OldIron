"""协议爬虫客户端 — 用 curl_cffi 实现站点链接发现与 HTML 抓取。

接口与现有 FirecrawlClient 对齐，可直接注入 FirecrawlEmailService 作为
firecrawl_client 参数。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from curl_cffi import requests as cffi_requests

from .link_extractor import extract_same_site_links
from .sitemap import discover_sitemap_urls

LOGGER = logging.getLogger(__name__)
_HTTP_FALLBACK_ERROR_HINTS = (
    "ssl",
    "tls",
    "certificate",
    "wrong_version_number",
    "alert_internal_error",
    "no alternative certificate subject name",
)
_SKIP_URL_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".tar", ".gz", ".rar", ".7z",
    ".exe", ".dmg", ".apk",
)
_TEXT_CONTENT_TYPE_HINTS = (
    "text/html",
    "application/xhtml+xml",
    "application/xml",
    "text/xml",
    "text/plain",
)


@dataclass()
class HtmlPageResult:
    """与 fc_email.client.HtmlPageResult 结构一致。"""
    url: str
    html: str


@dataclass()
class SiteCrawlConfig:
    """协议爬虫配置。"""
    timeout_seconds: float = 20.0
    max_retries: int = 2
    proxy_url: str = ""
    impersonate: str = "chrome110"
    max_html_chars: int = 250_000
    default_headers: dict[str, str] = field(default_factory=lambda: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    })


class SiteCrawlClient:
    """基于 curl_cffi 的协议爬虫，提供站点链接发现和 HTML 抓取。

    暴露与 FirecrawlClient 兼容的接口：
    - map_site(url, *, limit) -> list[str]
    - scrape_html(url) -> HtmlPageResult
    - scrape_html_pages(urls) -> list[HtmlPageResult]
    """

    def __init__(self, config: SiteCrawlConfig | None = None) -> None:
        self._config = config or SiteCrawlConfig()
        proxies = {}
        if self._config.proxy_url:
            proxies = {
                "http": self._config.proxy_url,
                "https": self._config.proxy_url,
            }
        self._session = cffi_requests.Session(
            impersonate=self._config.impersonate,
            proxies=proxies,
        )
        self._session.headers.update(self._config.default_headers)

    def map_site(
        self,
        url: str,
        *,
        limit: int = 200,
        include_subdomains: bool = False,
    ) -> list[str]:
        """发现站点链接：先尝试 sitemap，没有则抓首页提取链接。

        Args:
            url: 站点首页 URL
            limit: 最多返回的链接数
            include_subdomains: 保留参数（兼容 Firecrawl 接口），当前不影响行为

        Returns:
            站点链接列表
        """
        # 第一步：尝试 sitemap
        sitemap_urls = discover_sitemap_urls(
            self._session, url,
            limit=limit,
            timeout=self._config.timeout_seconds,
            include_subdomains=include_subdomains,
        )
        if sitemap_urls:
            LOGGER.info("协议爬虫 sitemap 发现链接：url=%s count=%s", url, len(sitemap_urls))
            return sitemap_urls

        # 第二步：fallback 到首页链接提取
        LOGGER.info("协议爬虫无 sitemap，回退首页链接提取：url=%s", url)
        homepage_html = self._fetch_html(url)
        if not homepage_html:
            return []

        links = extract_same_site_links(
            homepage_html,
            url,
            limit=limit,
            include_subdomains=include_subdomains,
        )
        LOGGER.info("协议爬虫首页链接提取：url=%s count=%s", url, len(links))
        return links

    def scrape_html(self, url: str, *, truncate_html: bool = True) -> HtmlPageResult:
        """抓取单个页面的完整 HTML。

        Args:
            url: 目标页面 URL
            truncate_html: 是否按配置截断超长 HTML

        Returns:
            HtmlPageResult(url, html)
        """
        html = self._fetch_html(url, truncate_html=truncate_html)
        return HtmlPageResult(url=url, html=html)

    def scrape_html_pages(self, urls: list[str], *, truncate_html: bool = True) -> list[HtmlPageResult]:
        """批量抓取多个页面的 HTML（兼容 GoFirecrawlService 接口）。

        跳过抓取失败或空内容的页面。

        Args:
            urls: 目标页面 URL 列表
            truncate_html: 是否按配置截断超长 HTML

        Returns:
            成功抓取的 HtmlPageResult 列表
        """
        pages: list[HtmlPageResult] = []
        for url in urls:
            html = self._fetch_html(url, truncate_html=truncate_html)
            if html.strip():
                pages.append(HtmlPageResult(url=url, html=html))
        return pages

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:  # noqa: BLE001
            return None

    def _fetch_html(self, url: str, *, truncate_html: bool = True) -> str:
        """带重试的 HTTP GET 获取 HTML。"""
        attempts = max(self._config.max_retries, 0) + 1
        last_error: Exception | None = None

        for attempt in range(attempts):
            resp = None
            try:
                resp = self._session.get(
                    url, timeout=self._config.timeout_seconds,
                )
                if resp.status_code == 200:
                    content_type = str(resp.headers.get("Content-Type", "") or "").lower()
                    if not _is_supported_page_response(url, content_type):
                        LOGGER.info("协议爬虫跳过非 HTML 内容：url=%s content_type=%s", url, content_type or "-")
                        return ""
                    text = resp.text or ""
                    if truncate_html:
                        return _truncate_html_text(url, text, self._config.max_html_chars)
                    return text
                if resp.status_code == 429:
                    LOGGER.warning(
                        "协议爬虫 HTTP 429：url=%s attempt=%s/%s",
                        url, attempt + 1, attempts,
                    )
                    continue
                if resp.status_code in {403, 404}:
                    LOGGER.debug("协议爬虫 HTTP %s 放弃：url=%s", resp.status_code, url)
                    return ""
                LOGGER.warning(
                    "协议爬虫 HTTP %s：url=%s", resp.status_code, url,
                )
                return ""
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                LOGGER.debug(
                    "协议爬虫请求异常：url=%s attempt=%s/%s error=%s",
                    url, attempt + 1, attempts, exc,
                )
            finally:
                if resp is not None:
                    try:
                        resp.close()
                    except Exception:  # noqa: BLE001
                        pass

        if last_error:
            fallback_url = _http_fallback_url(url, last_error)
            if fallback_url:
                LOGGER.info("协议爬虫 HTTPS 失败，尝试 HTTP 回退：url=%s fallback=%s", url, fallback_url)
                return self._fetch_html(fallback_url)
            LOGGER.warning("协议爬虫请求最终失败：url=%s error=%s", url, last_error)
        return ""


def _is_supported_page_response(url: str, content_type: str) -> bool:
    lowered_url = str(url or "").lower()
    if any(lowered_url.endswith(ext) for ext in _SKIP_URL_EXTENSIONS):
        return False
    clean_type = str(content_type or "").split(";", 1)[0].strip().lower()
    if not clean_type:
        return True
    return any(hint in clean_type for hint in _TEXT_CONTENT_TYPE_HINTS)


def _truncate_html_text(url: str, text: str, limit: int) -> str:
    if limit <= 0 or len(text) <= limit:
        return text
    half = max(limit // 2, 1)
    LOGGER.info("协议爬虫页面过长已截断：url=%s 原长=%d", url, len(text))
    return text[:half] + "\n<!-- 内容过长已截断 -->\n" + text[-half:]


def _http_fallback_url(url: str, error: Exception) -> str:
    text = str(error or "").lower()
    if not str(url or "").startswith("https://"):
        return ""
    if not any(hint in text for hint in _HTTP_FALLBACK_ERROR_HINTS):
        return ""
    return str(url).replace("https://", "http://", 1)
