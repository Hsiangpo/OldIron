"""khia.or.kr HTTP 客户端。"""

from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Any

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

BASE_HEADERS = {
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://www.khia.or.kr/sub03_01",
}

HTML_HEADERS = {
    **BASE_HEADERS,
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

NOT_FOUND_TEXT = "글이 존재하지 않습니다"


@dataclass(slots=True)
class RateLimitConfig:
    min_delay: float = 0.2
    max_delay: float = 0.6
    long_rest_interval: int = 300
    long_rest_seconds: float = 5.0


class KhiaClient:
    """khia.or.kr 协议层客户端。"""

    BASE_URL = "https://www.khia.or.kr"

    def __init__(self, rate_config: RateLimitConfig | None = None) -> None:
        self.rate_config = rate_config or RateLimitConfig()
        self._request_count = 0
        self.session = self._build_session()

    def _build_session(self) -> cffi_requests.Session:
        """创建浏览器指纹会话。"""
        return cffi_requests.Session(impersonate="chrome110")

    def _reset_session(self) -> None:
        """网络层异常后重建会话。"""
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._build_session()

    def _sleep(self) -> None:
        """请求间隔和周期休息。"""
        delay = random.uniform(self.rate_config.min_delay, self.rate_config.max_delay)
        time.sleep(delay)
        self._request_count += 1
        if (
            self.rate_config.long_rest_interval > 0
            and self._request_count % self.rate_config.long_rest_interval == 0
        ):
            logger.info(
                "KHIA 已请求 %d 次，休息 %.0fs",
                self._request_count,
                self.rate_config.long_rest_seconds,
            )
            time.sleep(self.rate_config.long_rest_seconds)

    def _request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        max_retries: int = 4,
    ) -> str:
        """执行 GET 请求，带重试和退避。"""
        url = f"{self.BASE_URL}{path}"
        req_headers = headers or HTML_HEADERS
        params = params or {}

        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    headers=req_headers,
                    timeout=30,
                )
            except Exception as exc:
                err_text = str(exc)
                logger.warning("KHIA 请求异常 (第%d次): %s — %s", attempt, url, err_text)
                if re.search(r"curl: \((28|35|56)\)", err_text):
                    self._reset_session()
                if attempt == max_retries:
                    raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}") from exc
                backoff = min((2**attempt) + random.uniform(0, 1.0), 20)
                time.sleep(backoff)
                continue

            if resp.status_code == 429:
                wait = 2**attempt * 5
                logger.warning("KHIA 429 限流，等待 %ds (第%d次)", wait, attempt)
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                raise RuntimeError(f"KHIA 403 Forbidden: {url}")
            if resp.status_code >= 500:
                if attempt == max_retries:
                    raise RuntimeError(f"KHIA 服务端错误 {resp.status_code}: {url}")
                backoff = min((2**attempt) + random.uniform(0, 1.0), 20)
                logger.warning(
                    "KHIA 服务端错误 %d，重试 (第%d次): %s",
                    resp.status_code,
                    attempt,
                    url,
                )
                time.sleep(backoff)
                continue

            resp.raise_for_status()
            return resp.text

        raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}")

    def get_list_html(self, page: int = 1) -> str:
        """获取会员列表页 HTML。"""
        params: dict[str, Any] = {}
        if page > 1:
            params["page"] = str(page)
        return self._request("/sub03_01", params=params, headers=HTML_HEADERS)

    def get_detail_html(self, item_id: str) -> str:
        """获取会员详情页 HTML。"""
        return self._request(f"/sub03_01/{item_id}", headers=HTML_HEADERS)

    @staticmethod
    def is_not_found_page(html_text: str) -> bool:
        """判断详情页是否为“文章不存在”页面。"""
        return NOT_FOUND_TEXT in (html_text or "")

    def close(self) -> None:
        self.session.close()
