"""mynavi HTTP 客户端。"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any
from urllib.parse import urljoin

from curl_cffi.requests import Session


LOGGER = logging.getLogger("mynavi.client")
BASE_URL = "https://tenshoku.mynavi.jp"
PREF_ROUTE_GROUPS = {
    "01": "hokkaido",
    "02": "tohoku",
    "03": "tohoku",
    "04": "tohoku",
    "05": "tohoku",
    "06": "tohoku",
    "07": "tohoku",
    "08": "kitakanto",
    "09": "kitakanto",
    "10": "kitakanto",
    "11": "shutoken",
    "12": "shutoken",
    "13": "shutoken",
    "14": "shutoken",
    "15": "koshinetsu",
    "16": "hokuriku",
    "17": "hokuriku",
    "18": "hokuriku",
    "19": "koshinetsu",
    "20": "koshinetsu",
    "21": "tokai",
    "22": "tokai",
    "23": "tokai",
    "24": "tokai",
    "25": "kansai",
    "26": "kansai",
    "27": "kansai",
    "28": "kansai",
    "29": "kansai",
    "30": "kansai",
    "31": "chugoku",
    "32": "chugoku",
    "33": "chugoku",
    "34": "chugoku",
    "35": "chugoku",
    "36": "shikoku",
    "37": "shikoku",
    "38": "shikoku",
    "39": "shikoku",
    "40": "kyushu",
    "41": "kyushu",
    "42": "kyushu",
    "43": "kyushu",
    "44": "kyushu",
    "45": "kyushu",
    "46": "kyushu",
    "47": "kyushu",
}


class MynaviClient:
    """mynavi 列表页和职位详情页客户端。"""

    def __init__(
        self,
        *,
        request_delay: float = 1.2,
        max_retries: int = 3,
        proxy: str | None = None,
    ) -> None:
        self._delay = request_delay
        self._max_retries = max_retries
        self._session = Session(impersonate="chrome120")
        proxy_url = proxy or os.getenv("HTTP_PROXY", "")
        if proxy_url:
            self._session.proxies = {"http": proxy_url, "https": proxy_url}
            LOGGER.info("使用代理: %s", proxy_url)
        self._session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": f"{BASE_URL}/search/",
            }
        )
        self._request_count = 0
        self._error_count = 0

    def fetch_list_page(self, pref_code: str, page: int = 1) -> str | None:
        """抓取某个都道府県的新着职位列表页。"""
        route_group = PREF_ROUTE_GROUPS.get(pref_code)
        if not route_group:
            return None
        path = f"/{route_group}/list/p{pref_code}/new/"
        if page > 1:
            path += f"pg{page}/"
        response = self._get_with_retry(f"{BASE_URL}{path}")
        return response.text if response is not None else None

    def fetch_detail_page(self, detail_url: str) -> str | None:
        """抓取职位详情页。"""
        response = self._get_with_retry(urljoin(BASE_URL, detail_url))
        return response.text if response is not None else None

    def _get_with_retry(self, url: str) -> Any:
        for attempt in range(self._max_retries):
            try:
                self._polite_delay()
                response = self._session.get(url, timeout=30)
                self._request_count += 1
                if response.status_code == 200:
                    return response
                if response.status_code == 429:
                    self._sleep_backoff(attempt, 4.0, 8.0, "429 限流")
                    continue
                if response.status_code == 403:
                    self._error_count += 1
                    LOGGER.error("403 禁止访问: %s", url)
                    return None
                if response.status_code >= 500:
                    self._sleep_backoff(attempt, 2.0, 5.0, f"{response.status_code} 服务端错误")
                    continue
                LOGGER.warning("HTTP %d: %s", response.status_code, url)
                return None
            except Exception as exc:  # noqa: BLE001
                self._error_count += 1
                LOGGER.warning("请求异常: %s", exc)
                self._sleep_backoff(attempt, 2.0, 4.0, "网络异常重试")
        LOGGER.error("重试耗尽: %s", url)
        return None

    def _polite_delay(self) -> None:
        time.sleep(self._delay + random.uniform(0.2, 0.6))

    def _sleep_backoff(self, attempt: int, base: float, jitter: float, label: str) -> None:
        wait = base * (attempt + 1) + random.uniform(0.5, jitter)
        LOGGER.warning("%s，等待 %.1fs", label, wait)
        time.sleep(wait)

    @property
    def stats(self) -> dict[str, int]:
        return {"requests": self._request_count, "errors": self._error_count}

