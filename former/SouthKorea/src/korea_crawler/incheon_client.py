"""bizok.incheon.go.kr HTTP 客户端 — 政府网站，限速宽松。"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://bizok.incheon.go.kr/platform/sub/business.jsp",
}


@dataclass(slots=True)
class RateLimitConfig:
    min_delay: float = 0.2
    max_delay: float = 0.8
    long_rest_interval: int = 500
    long_rest_seconds: float = 5.0


class IncheonClient:
    """bizok.incheon.go.kr HTTP 客户端。"""

    BASE_URL = "https://bizok.incheon.go.kr"

    def __init__(self, rate_config: RateLimitConfig | None = None) -> None:
        self.rate_config = rate_config or RateLimitConfig()
        self._request_count = 0
        self.session = self._build_session()

    def _build_session(self) -> cffi_requests.Session:
        """创建带浏览器指纹的会话。"""
        return cffi_requests.Session(impersonate="chrome110")

    def _reset_session(self) -> None:
        """网络层异常后重建会话，避免复用坏连接。"""
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._build_session()

    def _sleep(self) -> None:
        delay = random.uniform(self.rate_config.min_delay, self.rate_config.max_delay)
        time.sleep(delay)
        self._request_count += 1
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

    def get_html(self, path: str, max_retries: int = 4) -> str:
        """GET HTML 页面。"""
        url = f"{self.BASE_URL}{path}"

        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                resp = self.session.get(url, headers=HEADERS, timeout=30)
            except Exception as exc:
                err_text = str(exc)
                logger.warning("请求异常 (第%d次): %s — %s", attempt, url, err_text)

                # TLS 握手/超时类错误常由连接复用触发，重建会话后再重试
                if "curl: (35)" in err_text or "curl: (28)" in err_text:
                    self._reset_session()

                if attempt == max_retries:
                    raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}") from exc
                backoff = min((2 ** attempt) + random.uniform(0, 1.0), 20)
                time.sleep(backoff)
                continue

            if resp.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning("429 限流，等待 %ds (第%d次)", wait, attempt)
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                logger.error("403 封禁！URL: %s", url)
                raise RuntimeError(f"403 Forbidden: {url}")

            if resp.status_code >= 500:
                logger.warning("服务端错误 %d (第%d次): %s", resp.status_code, attempt, url)
                if attempt == max_retries:
                    raise RuntimeError(f"服务端错误 {resp.status_code}: {url}")
                backoff = min((2 ** attempt) + random.uniform(0, 1.0), 20)
                time.sleep(backoff)
                continue

            resp.raise_for_status()
            return resp.text

        raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}")

    def close(self) -> None:
        self.session.close()
