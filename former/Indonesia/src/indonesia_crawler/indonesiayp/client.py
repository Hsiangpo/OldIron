"""indonesiayp HTTP 客户端。"""

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
        "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "upgrade-insecure-requests": "1",
}
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)
DEFAULT_IMPERSONATES = ["chrome110", "chrome124", "chrome136"]


def _env_float(name: str, default: float) -> float:
    """读取浮点环境变量。"""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    """读取整型环境变量。"""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(slots=True)
class RateLimitConfig:
    """限速配置。"""

    min_delay: float = 0.8
    max_delay: float = 1.8
    long_rest_interval: int = 80
    long_rest_seconds: float = 12.0


class IndonesiaYpClient:
    """indonesiayp 协议客户端。"""

    BASE_URL = "https://www.indonesiayp.com"

    def __init__(self, rate_config: RateLimitConfig | None = None) -> None:
        self.rate_config = rate_config or RateLimitConfig()
        self._request_count = 0
        self.user_agent = os.getenv("IYP_USER_AGENT", "").strip() or DEFAULT_USER_AGENT
        self.cookie_header = os.getenv("IYP_COOKIE", "").strip()
        self.proxy_url = os.getenv("IYP_PROXY_URL", "").strip()
        self.request_timeout = max(5.0, _env_float("IYP_TIMEOUT", 30.0))
        self.max_retries = max(1, _env_int("IYP_MAX_RETRIES", 3))
        self.retry_backoff = max(1.0, _env_float("IYP_RETRY_BACKOFF", 2.0))
        self.retry_403_wait = max(2.0, _env_float("IYP_403_WAIT", 6.0))
        self.impersonates = self._load_impersonates()
        self._impersonate_idx = 0
        self.session = self._build_session(self.impersonates[self._impersonate_idx])

    def _load_impersonates(self) -> list[str]:
        """读取可轮换的浏览器指纹列表。"""
        raw = os.getenv("IYP_IMPERSONATES", "").strip()
        if not raw:
            return list(DEFAULT_IMPERSONATES)
        values = [item.strip() for item in raw.split(",") if item.strip()]
        return values or list(DEFAULT_IMPERSONATES)

    def _build_session(self, impersonate: str) -> cffi_requests.Session:
        """按指定指纹构建会话。"""
        return cffi_requests.Session(impersonate=impersonate)

    def _rotate_session(self) -> None:
        """重建会话并切换指纹。"""
        try:
            self.session.close()
        except Exception:  # noqa: BLE001
            pass
        self._impersonate_idx = (self._impersonate_idx + 1) % len(self.impersonates)
        impersonate = self.impersonates[self._impersonate_idx]
        self.session = self._build_session(impersonate)
        logger.info("IYP 会话已重建，切换指纹为 %s", impersonate)

    def _build_headers(self) -> dict[str, str]:
        """构造请求头。"""
        headers = {
            **DEFAULT_HEADERS,
            "referer": f"{self.BASE_URL}/category/general_business",
            "user-agent": self.user_agent,
        }
        if self.cookie_header:
            headers["cookie"] = self.cookie_header
        return headers

    def _sleep(self) -> None:
        """请求间随机休眠。"""
        delay = random.uniform(self.rate_config.min_delay, self.rate_config.max_delay)
        time.sleep(delay)
        self._request_count += 1
        if self.rate_config.long_rest_interval > 0 and self._request_count % self.rate_config.long_rest_interval == 0:
            logger.info("已请求 %d 次，长休息 %.0fs", self._request_count, self.rate_config.long_rest_seconds)
            time.sleep(self.rate_config.long_rest_seconds)

    def get_html(self, path_or_url: str, max_retries: int | None = None) -> str:
        """获取 HTML 页面。"""
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            url = path_or_url
        else:
            url = f"{self.BASE_URL}{path_or_url}"

        retry_total = max_retries if max_retries is not None else self.max_retries
        for attempt in range(1, retry_total + 1):
            self._sleep()
            request_kwargs: dict[str, object] = {
                "headers": self._build_headers(),
                "timeout": self.request_timeout,
            }
            if self.proxy_url:
                request_kwargs["proxy"] = self.proxy_url

            try:
                response = self.session.get(url, **request_kwargs)
            except Exception as exc:  # noqa: BLE001
                logger.warning("请求异常（第%d次）%s: %s", attempt, url, exc)
                if attempt == retry_total:
                    raise
                time.sleep(min(self.retry_backoff**attempt, 10))
                continue

            if response.status_code == 429:
                wait = min(2**attempt * 3, 20)
                logger.warning("429 限流（第%d次）%s，等待 %ds", attempt, url, wait)
                time.sleep(wait)
                continue

            if response.status_code == 403:
                wait = min(self.retry_403_wait * attempt, 30)
                if not self.cookie_header:
                    logger.warning("触发 403（第%d次）%s，建议补充 IYP_COOKIE，等待 %.1fs 后重试", attempt, url, wait)
                else:
                    logger.warning("触发 403（第%d次）%s，IYP_COOKIE 可能失效，等待 %.1fs 后重试", attempt, url, wait)
                if attempt >= retry_total:
                    raise RuntimeError(f"403 Forbidden: {url}")
                self._rotate_session()
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.text

        raise RuntimeError(f"请求失败，已重试 {retry_total} 次: {url}")

    def close(self) -> None:
        """关闭会话。"""
        self.session.close()
