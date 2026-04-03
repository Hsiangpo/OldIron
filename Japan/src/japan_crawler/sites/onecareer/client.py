"""OneCareer HTTP 客户端。"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any
from urllib.parse import urljoin

from curl_cffi.requests import Session


LOGGER = logging.getLogger("onecareer.client")
BASE_URL = "https://www.onecareer.jp"
INDEX_URL = f"{BASE_URL}/companies"
CATEGORY_PATH = "/companies/business_categories"


class OnecareerClient:
    """OneCareer 列表与公司详情抓取客户端。"""

    def __init__(
        self,
        *,
        request_delay: float = 1.0,
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
                "Referer": INDEX_URL,
            }
        )
        self._request_count = 0
        self._error_count = 0

    def fetch_index_page(self) -> str | None:
        response = self._get_with_retry(INDEX_URL)
        return response.text if response is not None else None

    def fetch_category_page(self, category_id: str, page: int = 1) -> str | None:
        response = self._get_with_retry(
            f"{BASE_URL}{CATEGORY_PATH}/{category_id}",
            params={"page": str(page)},
        )
        return response.text if response is not None else None

    def fetch_detail_page(self, detail_url: str) -> str | None:
        response = self._get_with_retry(urljoin(BASE_URL, detail_url))
        return response.text if response is not None else None

    def _get_with_retry(self, url: str, params: dict[str, str] | None = None) -> Any:
        for attempt in range(self._max_retries):
            try:
                self._polite_delay()
                response = self._session.get(url, params=params, timeout=30)
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

