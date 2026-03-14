"""Snov API 客户端。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

import requests

DOMAIN_PATTERN = re.compile(r"^(?:[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")


def extract_domain(website_url: str) -> str:
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
    value = domain.strip().lower()
    if not value:
        return False
    return bool(DOMAIN_PATTERN.fullmatch(value))


@dataclass(slots=True)
class SnovConfig:
    client_id: str
    client_secret: str
    timeout: float = 30.0


class SnovClient:
    """封装 Snov token、数量查询和邮箱查询。"""

    def __init__(self, config: SnovConfig) -> None:
        self.config = config
        self.session = requests.Session()
        # 中文注释：默认不继承系统代理环境变量，减少本机代理干扰导致的 SSL/连接异常。
        self.session.trust_env = False
        self._access_token = ""

    def _request_token(self) -> str:
        response = self.session.post(
            "https://api.snov.io/v1/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            },
            timeout=self.config.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        token = str(payload.get("access_token", "")).strip()
        if not token:
            raise RuntimeError(f"Snov 鉴权失败：{payload}")
        self._access_token = token
        return token

    def _token(self) -> str:
        if self._access_token:
            return self._access_token
        return self._request_token()

    def get_domain_emails_count(self, domain: str) -> int:
        if not is_valid_domain(domain):
            return 0
        response = self.session.post(
            "https://api.snov.io/v1/get-domain-emails-count",
            data={"access_token": self._token(), "domain": domain},
            timeout=self.config.timeout,
        )
        if response.status_code == 401:
            self._request_token()
            response = self.session.post(
                "https://api.snov.io/v1/get-domain-emails-count",
                data={"access_token": self._token(), "domain": domain},
                timeout=self.config.timeout,
            )
        if response.status_code == 400:
            return 0
        response.raise_for_status()
        payload = response.json()
        value = payload.get("result", 0)
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Snov 数量返回异常：{payload}") from exc

    def get_domain_emails(self, domain: str) -> list[str]:
        if not is_valid_domain(domain):
            return []
        start = self.session.post(
            "https://api.snov.io/v2/domain-search/domain-emails/start",
            data={"access_token": self._token(), "domain": domain},
            timeout=self.config.timeout,
        )
        if start.status_code == 401:
            self._request_token()
            start = self.session.post(
                "https://api.snov.io/v2/domain-search/domain-emails/start",
                data={"access_token": self._token(), "domain": domain},
                timeout=self.config.timeout,
            )
        if start.status_code == 400:
            return []
        start.raise_for_status()
        payload = start.json()
        result_url = str(payload.get("links", {}).get("result", "")).strip()
        if not result_url:
            return []

        result = self.session.get(
            result_url,
            params={"access_token": self._token()},
            timeout=self.config.timeout,
        )
        if result.status_code == 400:
            return []
        result.raise_for_status()
        data = result.json()
        items = data.get("data", [])
        emails: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            email = str(item.get("email", "")).strip().lower()
            if email:
                emails.append(email)
        return list(dict.fromkeys(emails))
