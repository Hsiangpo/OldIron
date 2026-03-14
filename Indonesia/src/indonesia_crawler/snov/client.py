"""Snov API 客户端。"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

DOMAIN_PATTERN = re.compile(r"^(?:[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")


class SnovRateLimitError(RuntimeError):
    """Snov 限流异常。"""


def extract_domain(website_url: str) -> str:
    """从官网 URL 提取根域名。"""
    raw = website_url.strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def is_valid_domain(domain: str) -> bool:
    """域名格式检查。"""
    value = domain.strip().lower()
    if not value:
        return False
    return bool(DOMAIN_PATTERN.fullmatch(value))


@dataclass(slots=True)
class SnovConfig:
    """Snov 配置。"""

    client_id: str
    client_secret: str
    timeout: float = 30.0
    max_retries: int = 5
    retry_delay: float = 10.0


class SnovClient:
    """Snov API 封装。"""

    def __init__(self, config: SnovConfig) -> None:
        self.config = config
        self.session = cffi_requests.Session(impersonate="chrome110")
        self._access_token = ""

    def _request_token(self) -> str:
        """请求 access token。"""
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        for attempt in range(1, self.config.max_retries + 1):
            response = self.session.post(
                "https://api.snov.io/v1/oauth/access_token",
                data=payload,
                timeout=self.config.timeout,
            )
            if response.status_code == 429:
                logger.warning("Snov token 限流（第%d次），等待 %.0fs", attempt, self.config.retry_delay)
                time.sleep(self.config.retry_delay)
                continue
            response.raise_for_status()
            data = response.json()
            token = str(data.get("access_token", "")).strip()
            if not token:
                raise RuntimeError(f"Snov 鉴权失败: {data}")
            self._access_token = token
            return token
        raise SnovRateLimitError("Snov token 连续限流")

    def _token(self) -> str:
        """获取 token（有缓存）。"""
        if self._access_token:
            return self._access_token
        return self._request_token()

    def _post_with_retry(self, url: str, payload: dict) -> cffi_requests.Response:
        """统一 POST 重试策略。"""
        for attempt in range(1, self.config.max_retries + 1):
            response = self.session.post(url, data=payload, timeout=self.config.timeout)

            if response.status_code == 401:
                payload["access_token"] = self._request_token()
                continue

            if response.status_code == 429:
                logger.warning("Snov 限流（第%d次）%s", attempt, url)
                time.sleep(self.config.retry_delay)
                continue

            return response
        raise SnovRateLimitError(f"Snov 连续限流: {url}")

    def get_domain_emails(self, domain: str) -> list[str]:
        """查询域名邮箱列表。"""
        if not is_valid_domain(domain):
            return []

        payload = {"access_token": self._token(), "domain": domain}
        start = self._post_with_retry("https://api.snov.io/v2/domain-search/domain-emails/start", payload)
        if start.status_code == 400:
            return []
        start.raise_for_status()
        start_data = start.json()
        result_url = str(start_data.get("links", {}).get("result", "")).strip()
        if not result_url:
            return []

        for attempt in range(1, self.config.max_retries + 1):
            result = self.session.get(
                result_url,
                params={"access_token": self._token()},
                timeout=self.config.timeout,
            )
            if result.status_code == 429:
                logger.warning("Snov result 限流（第%d次）%s", attempt, domain)
                time.sleep(self.config.retry_delay)
                continue
            if result.status_code == 400:
                return []
            result.raise_for_status()
            data = result.json()
            emails: list[str] = []
            for item in data.get("data", []):
                if not isinstance(item, dict):
                    continue
                email = str(item.get("email", "")).strip().lower()
                if email:
                    emails.append(email)
            return list(dict.fromkeys(emails))
        raise SnovRateLimitError(f"Snov result 连续限流: {domain}")

    def close(self) -> None:
        """关闭会话。"""
        self.session.close()

