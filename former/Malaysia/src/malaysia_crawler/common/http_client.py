"""HTTP 请求封装。"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass

import requests

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)


@dataclass(slots=True)
class HttpConfig:
    timeout: float = 30.0
    min_delay: float = 0.3
    max_delay: float = 0.8
    verify_ssl: bool = True
    user_agent: str = DEFAULT_USER_AGENT
    use_system_proxy: bool = False


class HttpClient:
    """带随机间隔和统一头的 HTTP 客户端。"""

    def __init__(self, cookie_header: str, config: HttpConfig | None = None) -> None:
        self.config = config or HttpConfig()
        self.session = requests.Session()
        # 中文注释：默认不读取系统代理环境变量，避免被本机代理配置干扰导致随机失败。
        self.session.trust_env = self.config.use_system_proxy
        self.session.headers.update(
            {
                "User-Agent": self.config.user_agent,
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        if cookie_header:
            self.session.headers["Cookie"] = cookie_header

    def sleep_random(self) -> None:
        if self.config.max_delay <= 0:
            return
        if self.config.max_delay < self.config.min_delay:
            delay = self.config.max_delay
        else:
            delay = random.uniform(self.config.min_delay, self.config.max_delay)
        if delay > 0:
            time.sleep(delay)

    def get(self, url: str, **kwargs: object) -> requests.Response:
        self.sleep_random()
        response = self.session.get(
            url,
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
            **kwargs,
        )
        response.raise_for_status()
        return response

    def post(self, url: str, **kwargs: object) -> requests.Response:
        self.sleep_random()
        response = self.session.post(
            url,
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
            **kwargs,
        )
        response.raise_for_status()
        return response
