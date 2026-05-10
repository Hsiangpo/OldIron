"""CNPJ Biz 代理池。"""

from __future__ import annotations

import logging
import threading
import time
import urllib.request
from dataclasses import dataclass


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ProxyPoolConfig:
    feed_url: str = ""
    scheme: str = "http"
    cache_ttl_seconds: float = 60.0


class CnpjBizProxyPool:
    """从 feed URL 拉取代理节点，并在失败时轮换。"""

    def __init__(self, config: ProxyPoolConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._candidates: list[str] = []
        self._index = 0
        self._expire_at = 0.0

    def current_proxy(self) -> str:
        with self._lock:
            self._ensure_loaded()
            if not self._candidates:
                return ""
            return self._candidates[self._index]

    def rotate_proxy(self) -> str:
        with self._lock:
            self._ensure_loaded(force=True)
            if not self._candidates:
                return ""
            self._index = (self._index + 1) % len(self._candidates)
            return self._candidates[self._index]

    def _ensure_loaded(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and self._candidates and now < self._expire_at:
            return
        self._candidates = _fetch_proxy_candidates(self._config.feed_url, self._config.scheme)
        self._index = 0
        self._expire_at = now + max(self._config.cache_ttl_seconds, 1.0)


def _fetch_proxy_candidates(feed_url: str, scheme: str) -> list[str]:
    url = str(feed_url or "").strip()
    if not url:
        return []
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
    return _normalize_proxy_lines(raw, scheme)


def _normalize_proxy_lines(raw: str, scheme: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    prefix = f"{scheme}://"
    for line in str(raw or "").splitlines():
        text = line.strip()
        if not text:
            continue
        candidate = text if "://" in text else f"{prefix}{text}"
        if candidate in seen:
            continue
        seen.add(candidate)
        values.append(candidate)
    return values
