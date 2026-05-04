"""协议爬虫客户端 — 用 curl_cffi 实现站点链接发现与 HTML 抓取。

接口与现有 FirecrawlClient 对齐，可直接注入 FirecrawlEmailService 作为
firecrawl_client 参数。
"""

from __future__ import annotations

import logging
import socket
import re
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from dataclasses import dataclass, field
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urljoin
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests

from .link_extractor import extract_same_site_links
from .sitemap import decode_response_text
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
_LOCAL_PROXY_HOSTS = {"127.0.0.1", "localhost", "::1"}
_LOCAL_PROXY_ERROR_HINTS = (
    "failed to connect",
    "could not connect to server",
    "connection refused",
    "curl: (7)",
)
_INSECURE_HTTPS_ERROR_HINTS = (
    "ssl certificate problem",
    "unable to get local issuer certificate",
    "certificate subject name",
    "certificate verify failed",
)
_RELATED_SUBDOMAIN_HOST_TOKENS = {
    "about", "career", "careers", "company", "contact", "help", "jobs",
    "leadership", "people", "support", "team",
}
_RELATED_SUBDOMAIN_PATH_TOKENS = {
    "about", "board", "career", "careers", "company", "contact", "director",
    "executive", "founder", "governance", "jobs", "leadership", "management",
    "officers", "people", "president", "privacy", "support", "team", "terms",
}
_SUBDOMAIN_SCAN_PAGE_TOKENS = {
    "about", "contact", "company", "help", "people", "privacy", "support", "team",
}
_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "ref_src",
    "srsltid",
}
_COMMON_VALUE_PATHS = (
    "/impressum",
    "/imprint",
    "/kontakt",
    "/kontakt.html",
    "/ueber-uns",
    "/uber-uns",
    "/about-us/our-people",
    "/our-people",
    "/company-leadership",
    "/executive-team",
    "/leadership",
    "/management",
    "/our-team",
    "/team-members",
    "/team",
    "/people",
    "/about-us",
    "/about",
    "/about.html",
    "/company",
    "/contact-us",
    "/contact",
    "/contact.html",
    "/legal-notice",
    "/privacy-policy",
    "/privacy",
    "/terms",
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
    common_probe_target: int = 8
    common_probe_concurrency: int = 8
    common_probe_patience_batches: int = 2
    common_probe_min_hits_after_patience: int = 2
    related_seed_limit: int = 2
    related_per_seed_limit: int = 20
    related_total_limit: int = 60
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
        self._proxy_url = str(self._config.proxy_url or "").strip()
        self._using_proxy = bool(self._proxy_url)
        if self._using_proxy and _is_unavailable_local_proxy(self._proxy_url):
            LOGGER.warning("协议爬虫本地 dl 未监听，改走直连：proxy=%s", self._proxy_url)
            self._using_proxy = False
        self._session = self._build_session(use_proxy=self._using_proxy)

    def _build_session(self, *, use_proxy: bool) -> cffi_requests.Session:
        proxies = {}
        if use_proxy and self._proxy_url:
            proxies = {
                "http": self._proxy_url,
                "https": self._proxy_url,
            }
        session = cffi_requests.Session(
            impersonate=self._config.impersonate,
            proxies=proxies,
        )
        session.trust_env = False
        session.headers.update(self._config.default_headers)
        return session

    def _reset_session(self, *, use_proxy: bool) -> None:
        try:
            self._session.close()
        except Exception:  # noqa: BLE001
            pass
        self._using_proxy = use_proxy
        self._session = self._build_session(use_proxy=use_proxy)

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
        homepage_html = self._fetch_html(url, truncate_html=False)
        sitemap_urls = discover_sitemap_urls(
            self._session, url,
            limit=limit,
            timeout=self._config.timeout_seconds,
            include_subdomains=include_subdomains,
        )
        links = extract_same_site_links(
            homepage_html,
            url,
            limit=limit,
            include_subdomains=include_subdomains,
        ) if homepage_html else []
        probe_urls = self._probe_common_value_urls(url, limit=limit)
        merged = _merge_unique_urls(
            probe_urls,
            links,
            sitemap_urls,
            limit=limit,
        )
        related_urls = self._discover_related_subdomain_urls(
            url,
            homepage_html=homepage_html,
            direct_urls=merged,
            limit=min(limit, self._config.related_total_limit),
        )
        merged = _merge_unique_urls(merged, related_urls, limit=limit)
        if sitemap_urls:
            LOGGER.info("协议爬虫 sitemap 发现链接：url=%s count=%s", url, len(sitemap_urls))
        elif homepage_html:
            LOGGER.info("协议爬虫无 sitemap，回退首页链接提取：url=%s count=%s", url, len(links))
        return merged

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

    def _fetch_html(
        self,
        url: str,
        *,
        truncate_html: bool = True,
        allow_insecure_https_retry: bool = True,
    ) -> str:
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
                    text = decode_response_text(resp)
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
                if self._disable_unavailable_local_proxy(exc):
                    continue
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
            if allow_insecure_https_retry and _should_retry_insecure_https(url, last_error):
                LOGGER.info("协议爬虫 HTTPS 证书异常，尝试宽松校验重试：url=%s", url)
                insecure_html = self._fetch_html_insecure_https(url, truncate_html=truncate_html)
                if insecure_html:
                    return insecure_html
            fallback_url = _http_fallback_url(url, last_error)
            if fallback_url:
                LOGGER.info("协议爬虫 HTTPS 失败，尝试 HTTP 回退：url=%s fallback=%s", url, fallback_url)
                return self._fetch_html(
                    fallback_url,
                    truncate_html=truncate_html,
                    allow_insecure_https_retry=False,
                )
            LOGGER.warning("协议爬虫请求最终失败：url=%s error=%s", url, last_error)
        return ""

    def _fetch_html_insecure_https(self, url: str, *, truncate_html: bool) -> str:
        resp = None
        try:
            resp = self._session.get(
                url,
                timeout=self._config.timeout_seconds,
                verify=False,
            )
            if resp.status_code != 200:
                LOGGER.warning("协议爬虫宽松校验重试失败：url=%s status=%s", url, resp.status_code)
                return ""
            content_type = str(resp.headers.get("Content-Type", "") or "").lower()
            if not _is_supported_page_response(url, content_type):
                LOGGER.info("协议爬虫宽松校验跳过非 HTML 内容：url=%s content_type=%s", url, content_type or "-")
                return ""
            text = decode_response_text(resp)
            if truncate_html:
                return _truncate_html_text(url, text, self._config.max_html_chars)
            return text
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("协议爬虫宽松校验重试失败：url=%s error=%s", url, exc)
            return ""
        finally:
            if resp is not None:
                try:
                    resp.close()
                except Exception:  # noqa: BLE001
                    pass

    def _disable_unavailable_local_proxy(self, error: Exception) -> bool:
        if not self._using_proxy:
            return False
        if not _is_dead_local_proxy_error(self._proxy_url, error):
            return False
        LOGGER.warning("协议爬虫本地 dl 不可用，回退直连：proxy=%s error=%s", self._proxy_url, error)
        self._reset_session(use_proxy=False)
        return True

    def _probe_common_value_urls(self, start_url: str, *, limit: int) -> list[str]:
        probe_urls = _build_common_probe_urls(start_url)
        if not probe_urls:
            return []
        result: list[str] = []
        probe_target = min(max(self._config.common_probe_target, 1), max(limit, 1), len(probe_urls))
        batch_size = min(max(self._config.common_probe_concurrency, 1), len(probe_urls))
        start_index = 0
        empty_batches = 0
        while start_index < len(probe_urls) and len(result) < probe_target:
            batch = probe_urls[start_index : start_index + batch_size]
            start_index += batch_size
            batch_hits = _probe_common_value_batch(batch, self._config)
            result = _merge_unique_urls(result, batch_hits, limit=probe_target)
            if batch_hits:
                empty_batches = 0
            else:
                empty_batches += 1
            if len(result) >= probe_target:
                break
            if empty_batches >= max(self._config.common_probe_patience_batches, 1):
                break
            if start_index >= batch_size * max(self._config.common_probe_patience_batches, 1):
                if len(result) < max(self._config.common_probe_min_hits_after_patience, 1):
                    break
        return result

    def _discover_related_subdomain_urls(
        self,
        start_url: str,
        *,
        homepage_html: str,
        direct_urls: list[str],
        limit: int,
    ) -> list[str]:
        seeds = _collect_related_subdomain_seed_urls(
            self,
            start_url,
            homepage_html=homepage_html,
            direct_urls=direct_urls,
        )
        if not seeds:
            return []
        result: list[str] = []
        for seed_url in seeds[: self._config.related_seed_limit]:
            result = _merge_unique_urls(result, [seed_url], limit=limit)
            extra_urls = self.map_site(seed_url, limit=self._config.related_per_seed_limit)
            result = _merge_unique_urls(result, extra_urls, limit=limit)
            if len(result) >= limit:
                break
        return result


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


def _should_retry_insecure_https(url: str, error: Exception) -> bool:
    if not str(url or "").startswith("https://"):
        return False
    lowered = str(error or "").lower()
    return any(hint in lowered for hint in _INSECURE_HTTPS_ERROR_HINTS)


def _local_proxy_address(proxy_url: str) -> tuple[str, int] | None:
    text = str(proxy_url or "").strip()
    if not text:
        return None
    parsed = urlparse(text if "://" in text else f"http://{text}")
    host = str(parsed.hostname or "").strip().lower()
    if host not in _LOCAL_PROXY_HOSTS:
        return None
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return host, int(port)


def _is_unavailable_local_proxy(proxy_url: str) -> bool:
    address = _local_proxy_address(proxy_url)
    if address is None:
        return False
    try:
        with socket.create_connection(address, timeout=0.25):
            return False
    except OSError:
        return True


def _is_dead_local_proxy_error(proxy_url: str, error: Exception) -> bool:
    if _local_proxy_address(proxy_url) is None:
        return False
    lowered = str(error or "").lower()
    return any(hint in lowered for hint in _LOCAL_PROXY_ERROR_HINTS)


def _build_common_probe_urls(start_url: str) -> list[str]:
    parsed = urlparse(start_url)
    if not parsed.scheme or not parsed.netloc:
        return []
    locale_prefix = _extract_path_locale_prefix(parsed.path)
    result: list[str] = []
    seen: set[str] = set()
    hosts = [parsed.netloc]
    if parsed.netloc and not parsed.netloc.lower().startswith("www."):
        hosts.append(f"www.{parsed.netloc}")
    for host in hosts:
        for base_prefix in ([locale_prefix] if locale_prefix else []) + [""]:
            for path in _COMMON_VALUE_PATHS:
                joined_path = f"{base_prefix}{path}" if base_prefix else path
                probe_url = parsed._replace(netloc=host, path=joined_path, query="", fragment="").geturl()
                normalized = _normalize_discovery_url(probe_url)
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    result.append(normalized)
    return result


def _extract_path_locale_prefix(path: str) -> str:
    cleaned = str(path or "").strip("/")
    if not cleaned:
        return ""
    first = cleaned.split("/", 1)[0].strip().lower()
    if re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", first):
        return f"/{first}"
    return ""


def _normalize_discovery_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return text
    kept_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        clean_key = str(key or "").strip().lower()
        if not clean_key or clean_key.startswith("utm_") or clean_key in _TRACKING_QUERY_KEYS:
            continue
        kept_pairs.append((key, value))
    return parsed._replace(query=urlencode(kept_pairs, doseq=True), fragment="").geturl()


def _merge_unique_urls(*groups: list[str], limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for url in group:
            normalized = _normalize_discovery_url(str(url or "").strip())
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
            if len(result) >= limit:
                return result
    return result


def _probe_common_value_batch(probe_urls: list[str], config: SiteCrawlConfig) -> list[str]:
    if not probe_urls:
        return []
    results: list[str] = []
    futures = {}
    with ThreadPoolExecutor(max_workers=min(len(probe_urls), max(config.common_probe_concurrency, 1))) as executor:
        for probe_url in probe_urls:
            futures[executor.submit(_probe_single_url, probe_url, config)] = probe_url
        done, _pending = wait(futures.keys(), timeout=min(config.timeout_seconds, 6.0), return_when=FIRST_COMPLETED)
        while done:
            for future in list(done):
                futures.pop(future, None)
                try:
                    hit = future.result()
                except Exception:
                    hit = None
                if hit:
                    results.append(hit)
            if not futures:
                break
            done, _pending = wait(futures.keys(), timeout=min(config.timeout_seconds, 6.0), return_when=FIRST_COMPLETED)
        for future in futures:
            future.cancel()
    return results


def _probe_single_url(probe_url: str, config: SiteCrawlConfig) -> str | None:
    crawler = SiteCrawlClient(
        SiteCrawlConfig(
            timeout_seconds=min(config.timeout_seconds, 3.0),
            max_retries=0,
            proxy_url=config.proxy_url,
            impersonate=config.impersonate,
            max_html_chars=config.max_html_chars,
            default_headers=dict(config.default_headers),
        )
    )
    try:
        html_text = crawler.scrape_html(probe_url, truncate_html=False).html
        return probe_url if str(html_text or "").strip() else None
    except Exception:
        return None
    finally:
        crawler.close()


def _collect_related_subdomain_seed_urls(
    crawler: SiteCrawlClient,
    start_url: str,
    *,
    homepage_html: str,
    direct_urls: list[str],
) -> list[str]:
    seeds = list(_pick_subdomain_probe_urls(start_url, direct_urls))
    for normalized in _extract_same_org_seed_urls(homepage_html or "", start_url, start_url):
        if normalized not in seeds:
            seeds.append(normalized)
        if len(seeds) >= 8:
            return seeds
    for probe_url in [url for url in _pick_subdomain_probe_urls(start_url, direct_urls) if url != start_url]:
        try:
            html_text = crawler.scrape_html(probe_url, truncate_html=False).html
        except Exception:
            continue
        for normalized in _extract_same_org_seed_urls(html_text or "", probe_url, start_url):
            if normalized not in seeds:
                seeds.append(normalized)
            if len(seeds) >= 8:
                return seeds
    return seeds


def _extract_same_org_seed_urls(html_text: str, page_url: str, start_url: str) -> list[str]:
    site_domain = _extract_registrable_domain(start_url)
    page_host = (urlparse(page_url).netloc or "").strip().lower()
    result: list[str] = []
    seen: set[str] = set()
    for raw_href in re.findall(r'''(?:href|src)=["']([^"'#]+)["']''', html_text or "", flags=re.I):
        absolute = urljoin(page_url, raw_href.strip())
        normalized = _normalize_discovery_url(absolute)
        if not normalized or normalized in seen:
            continue
        host = (urlparse(normalized).netloc or "").strip().lower()
        if not host or host == page_host:
            continue
        if _extract_registrable_domain(host) != site_domain:
            continue
        if not _looks_related_subdomain_seed(normalized, start_url):
            continue
        seen.add(normalized)
        result.append(normalized)
        if len(result) >= 8:
            break
    return result


def _pick_subdomain_probe_urls(start_url: str, direct_urls: list[str]) -> list[str]:
    start_domain = _extract_registrable_domain(start_url)
    picked: list[str] = []
    for url in direct_urls:
        parsed = urlparse(url)
        host = (parsed.netloc or "").strip().lower()
        if not host or _extract_registrable_domain(host) != start_domain:
            continue
        tokens = _extract_url_hint_tokens(url)
        if not any(token in _SUBDOMAIN_SCAN_PAGE_TOKENS for token in tokens):
            continue
        if url not in picked:
            picked.append(url)
        if len(picked) >= 4:
            break
    return picked


def _looks_related_subdomain_seed(url: str, start_url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.netloc or "").strip().lower()
    start_host = (urlparse(start_url).netloc or "").strip().lower()
    if not host or not start_host or host == start_host:
        return False
    if _extract_registrable_domain(host) != _extract_registrable_domain(start_host):
        return False
    host_tokens = [token for token in re.split(r"[\W_]+", host) if len(token) >= 3]
    path_tokens = _extract_url_hint_tokens(url)
    if any(token in _RELATED_SUBDOMAIN_HOST_TOKENS for token in host_tokens):
        return True
    return any(token in _RELATED_SUBDOMAIN_PATH_TOKENS for token in path_tokens)


def _extract_url_hint_tokens(url: str) -> list[str]:
    parsed = urlparse(str(url or ""))
    host_tokens = [
        clean
        for clean in re.split(r"[\W_]+", parsed.netloc.strip().lower())
        if len(clean) >= 3 and clean not in {"www", "com", "org", "net", "co", "it", "uk", "eu"}
    ]
    path_tokens: list[str] = []
    for part in parsed.path.split("/"):
        for token in re.split(r"[\W_]+", part.strip().lower()):
            clean = token.strip().lower()
            if len(clean) < 3 or clean in path_tokens or clean in host_tokens:
                continue
            path_tokens.append(clean)
    return [*host_tokens, *path_tokens]


def _extract_registrable_domain(value: str) -> str:
    host = str(value or "").strip().lower()
    if not host:
        return ""
    if "://" in host or "/" in host:
        host = (urlparse(host).netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return host
    suffix2 = ".".join(labels[-2:])
    if suffix2 in {"co.jp", "or.jp", "ne.jp", "go.jp", "ac.jp", "co.uk", "org.uk", "gov.uk", "ac.uk"} and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix2
