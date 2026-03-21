"""HTTP 客户端封装 — 统一 session / headers / 限速 / 重试。"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

# 从 DevTools 抓包获取的请求头
HTML_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "image/avif,image/webp,image/apng,*/*;q=0.8,"
              "application/signed-exchange;v=b3;q=0.7",
    "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "upgrade-insecure-requests": "1",
}


@dataclass(slots=True)
class RateLimitConfig:
    """限速配置。"""

    min_delay: float = 1.0
    max_delay: float = 2.5
    long_rest_interval: int = 50
    long_rest_seconds: float = 15.0


class GapensiClient:
    """gapensi.or.id HTTP 客户端。"""

    BASE_URL = "https://gapensi.or.id"

    def __init__(self, rate_config: RateLimitConfig | None = None) -> None:
        self.session = cffi_requests.Session(impersonate="chrome110")
        self.rate_config = rate_config or RateLimitConfig()
        self._request_count = 0

    def _sleep(self) -> None:
        """请求间随机延迟。"""
        delay = random.uniform(
            self.rate_config.min_delay, self.rate_config.max_delay
        )
        time.sleep(delay)
        self._request_count += 1

        # 长休息
        if (
            self.rate_config.long_rest_interval > 0
            and self._request_count % self.rate_config.long_rest_interval == 0
        ):
            logger.info(
                "已请求 %d 次，休息 %.0fs",
                self._request_count,
                self.rate_config.long_rest_seconds,
            )
            time.sleep(self.rate_config.long_rest_seconds)

    def get_html(self, path: str, max_retries: int = 3) -> str:
        """GET HTML 页面，返回文本。"""
        url = f"{self.BASE_URL}{path}"

        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                resp = self.session.get(
                    url,
                    headers={
                        **HTML_HEADERS,
                        "referer": f"{self.BASE_URL}/anggota",
                    },
                    timeout=30,
                )
            except Exception as exc:
                logger.warning("请求异常 (第%d次): %s — %s", attempt, url, exc)
                if attempt == max_retries:
                    raise
                time.sleep(2 ** attempt)
                continue

            if resp.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning("429 限流，等待 %ds (第%d次)", wait, attempt)
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                logger.error("403 封禁！停止请求。URL: %s", url)
                raise RuntimeError(f"403 Forbidden: {url}")

            resp.raise_for_status()
            return resp.text

        raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}")

    def close(self) -> None:
        """关闭 session。"""
        self.session.close()
