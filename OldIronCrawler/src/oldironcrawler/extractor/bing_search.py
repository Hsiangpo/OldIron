"""Bing 网页搜索客户端。

用 curl_cffi 直接抓取 Bing 搜索结果页，解析出 title/url/content。
接口与 TavilySearchClient 对齐（search / close / is_configured），可直接注入
ActiveRepresentativeSearcher 替代 Tavily，不需要 API key、不需要额外服务。
"""

from __future__ import annotations

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests


_BING_SEARCH_URL = "https://www.bing.com/search"
_DEFAULT_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class BingSearchClient:
    """抓 Bing 网页搜索结果的轻量客户端（无需 key）。"""

    def __init__(
        self,
        *,
        max_results: int = 8,
        timeout_seconds: float = 20.0,
        proxy_url: str = "",
    ) -> None:
        self._max_results = max(int(max_results or 8), 1)
        self._timeout_seconds = max(float(timeout_seconds or 20.0), 1.0)
        proxy = str(proxy_url or "").strip()
        # 单一出口先测；后面接动态代理时只要把 PROXY_URL 填上即可生效。
        self._proxies = {"http": proxy, "https": proxy} if proxy else None

    @property
    def is_configured(self) -> bool:
        # Bing 网页搜索不需要 key，始终视为已配置。
        return True

    def close(self) -> None:
        return None

    def search(self, query: str) -> list[dict[str, str]]:
        text = str(query or "").strip()
        if not text:
            return []
        response = cffi_requests.get(
            _BING_SEARCH_URL,
            params={"q": text, "count": self._max_results, "mkt": "en-US", "setlang": "en"},
            headers=_DEFAULT_HEADERS,
            proxies=self._proxies,
            timeout=self._timeout_seconds,
            impersonate="chrome",
        )
        response.raise_for_status()
        return parse_bing_results(response.text, self._max_results)


def parse_bing_results(html: str, max_results: int) -> list[dict[str, str]]:
    """从 Bing 结果页 HTML 解析出 title/url/content 列表。"""
    soup = BeautifulSoup(html or "", "lxml")
    results: list[dict[str, str]] = []
    seen: set[str] = set()
    limit = max(int(max_results or 1), 1)
    for item in soup.select("li.b_algo"):
        anchor = item.select_one("h2 a[href]")
        if anchor is None:
            continue
        url = str(anchor.get("href", "") or "").strip()
        title = anchor.get_text(" ", strip=True)
        if not url or not title or url in seen:
            continue
        caption = (
            item.select_one(".b_caption p")
            or item.select_one(".b_caption")
            or item.select_one("p")
        )
        content = caption.get_text(" ", strip=True) if caption is not None else ""
        seen.add(url)
        results.append({"title": title, "url": url, "content": content})
        if len(results) >= limit:
            break
    return results
