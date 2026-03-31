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
    default_headers: dict[str, str] = field(default_factory=lambda: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,da;q=0.8",
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
        _ = include_subdomains  # 保留接口兼容性

        # 第一步：尝试 sitemap
        sitemap_urls = discover_sitemap_urls(
            self._session, url,
            limit=limit,
            timeout=self._config.timeout_seconds,
        )
        if sitemap_urls:
            LOGGER.info("协议爬虫 sitemap 发现链接：url=%s count=%s", url, len(sitemap_urls))
            return sitemap_urls

        # 第二步：fallback 到首页链接提取
        LOGGER.info("协议爬虫无 sitemap，回退首页链接提取：url=%s", url)
        homepage_html = self._fetch_html(url)
        if not homepage_html:
            return []

        links = extract_same_site_links(homepage_html, url, limit=limit)
        LOGGER.info("协议爬虫首页链接提取：url=%s count=%s", url, len(links))
        return links

    def scrape_html(self, url: str) -> HtmlPageResult:
        """抓取单个页面的完整 HTML。

        Args:
            url: 目标页面 URL

        Returns:
            HtmlPageResult(url, html)
        """
        html = self._fetch_html(url)
        return HtmlPageResult(url=url, html=html)

    def scrape_html_pages(self, urls: list[str]) -> list[HtmlPageResult]:
        """批量抓取多个页面的 HTML（兼容 GoFirecrawlService 接口）。

        跳过抓取失败或空内容的页面。

        Args:
            urls: 目标页面 URL 列表

        Returns:
            成功抓取的 HtmlPageResult 列表
        """
        pages: list[HtmlPageResult] = []
        for url in urls:
            html = self._fetch_html(url)
            if html.strip():
                pages.append(HtmlPageResult(url=url, html=html))
        return pages

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:  # noqa: BLE001
            return None

    def _fetch_html(self, url: str) -> str:
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
                    return resp.text or ""
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
            fallback_url = self._http_fallback_url(url, last_error)
            if fallback_url:
                LOGGER.info("协议爬虫 HTTPS 失败，尝试 HTTP 回退：url=%s fallback=%s", url, fallback_url)
                return self._fetch_html(fallback_url)
            LOGGER.warning("协议爬虫请求最终失败：url=%s error=%s", url, last_error)
        return ""

    def _http_fallback_url(self, url: str, error: Exception) -> str:
        text = str(error or "").lower()
        if not str(url or "").startswith("https://"):
            return ""
        if not any(hint in text for hint in _HTTP_FALLBACK_ERROR_HINTS):
            return ""
        return str(url).replace("https://", "http://", 1)
