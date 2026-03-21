"""轻量代理池：支持 static/pool 两种模式与失败熔断。"""

from __future__ import annotations

import os
import random
import re
import string
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

SID_PATTERN = re.compile(r"-sid-[A-Za-z0-9]+")
TTL_PATTERN = re.compile(r"-t-\d+")


def _parse_bool(raw: str, default: bool = False) -> bool:
    """解析布尔环境变量。"""
    value = raw.strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on", "y"}


def _parse_int(raw: str, default: int) -> int:
    """解析整型环境变量。"""
    value = raw.strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(slots=True)
class ProxyLease:
    """一次请求拿到的代理租约。"""

    endpoint_id: int
    proxy_url: str
    label: str


@dataclass(slots=True)
class _ProxyEndpoint:
    """代理节点。"""

    scheme: str
    host: str
    port: int
    username: str = ""
    password: str = ""
    fail_count: int = 0
    cooldown_until: float = 0.0
    last_used_at: float = 0.0

    @property
    def label(self) -> str:
        """脱敏标签。"""
        return f"{self.host}:{self.port}"

    def build_url(self, sid_enabled: bool, sid_ttl_minutes: int, sid_length: int) -> str:
        """构造可直接用于 HTTP 客户端的代理 URL。"""
        if not self.username:
            return f"{self.scheme}://{self.host}:{self.port}"

        username = self.username
        if sid_enabled:
            base = SID_PATTERN.sub("", username)
            base = TTL_PATTERN.sub("", base).strip("-")
            sid = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(max(4, sid_length)))
            username = f"{base}-sid-{sid}-t-{max(1, sid_ttl_minutes)}"

        user = quote(username, safe="")
        password = quote(self.password, safe="")
        return f"{self.scheme}://{user}:{password}@{self.host}:{self.port}"


class ProxyPool:
    """代理池：支持失败熔断与自动轮换。"""

    def __init__(
        self,
        endpoints: list[_ProxyEndpoint],
        cooldown_seconds: int = 30,
        max_cooldown_seconds: int = 300,
        sid_enabled: bool = False,
        sid_ttl_minutes: int = 3,
        sid_length: int = 8,
    ) -> None:
        self._endpoints = endpoints
        self._cooldown_seconds = max(5, cooldown_seconds)
        self._max_cooldown_seconds = max(self._cooldown_seconds, max_cooldown_seconds)
        self._sid_enabled = sid_enabled
        self._sid_ttl_minutes = max(1, sid_ttl_minutes)
        self._sid_length = max(4, sid_length)

    @property
    def enabled(self) -> bool:
        """是否已启用代理池。"""
        return bool(self._endpoints)

    @property
    def size(self) -> int:
        """代理节点数量。"""
        return len(self._endpoints)

    def acquire(self) -> ProxyLease | None:
        """拿一个可用代理，优先冷却结束且最久未使用。"""
        if not self._endpoints:
            return None

        now = time.time()
        available = [idx for idx, endpoint in enumerate(self._endpoints) if endpoint.cooldown_until <= now]
        if not available:
            available = list(range(len(self._endpoints)))

        # 选择“最久未使用”的节点，降低单节点热度。
        candidate_id = min(available, key=lambda idx: self._endpoints[idx].last_used_at)
        endpoint = self._endpoints[candidate_id]
        endpoint.last_used_at = now
        return ProxyLease(
            endpoint_id=candidate_id,
            proxy_url=endpoint.build_url(
                sid_enabled=self._sid_enabled,
                sid_ttl_minutes=self._sid_ttl_minutes,
                sid_length=self._sid_length,
            ),
            label=endpoint.label,
        )

    def mark_success(self, endpoint_id: int) -> None:
        """成功后清空失败计数。"""
        endpoint = self._endpoints[endpoint_id]
        endpoint.fail_count = 0
        endpoint.cooldown_until = 0.0

    def mark_failure(self, endpoint_id: int) -> int:
        """失败后增加冷却时间，返回本次冷却秒数。"""
        endpoint = self._endpoints[endpoint_id]
        endpoint.fail_count += 1
        cooldown = min(self._cooldown_seconds * (2 ** (endpoint.fail_count - 1)), self._max_cooldown_seconds)
        endpoint.cooldown_until = time.time() + cooldown
        return int(cooldown)


def _split_proxy_items(raw: str) -> list[str]:
    """拆分环境变量中的代理字符串。"""
    if not raw.strip():
        return []
    return [item.strip() for item in re.split(r"[\n,;]", raw) if item.strip()]


def _read_proxy_list_file(file_path: str) -> list[str]:
    """读取代理列表文件。"""
    path = Path(file_path).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]


def _parse_proxy_item(item: str, default_scheme: str) -> _ProxyEndpoint | None:
    """解析单条代理配置。"""
    value = item.strip()
    if not value:
        return None

    if "://" in value:
        parsed = urlsplit(value)
        if not parsed.hostname or not parsed.port:
            return None
        username = unquote(parsed.username) if parsed.username else ""
        password = unquote(parsed.password) if parsed.password else ""
        scheme = parsed.scheme or default_scheme
        return _ProxyEndpoint(
            scheme=scheme,
            host=parsed.hostname,
            port=int(parsed.port),
            username=username,
            password=password,
        )

    parts = value.split(":")
    if len(parts) == 2:
        host, port = parts
        if not port.isdigit():
            return None
        return _ProxyEndpoint(scheme=default_scheme, host=host, port=int(port))

    if len(parts) == 4:
        host, port, username, password = parts
        if not port.isdigit():
            return None
        return _ProxyEndpoint(
            scheme=default_scheme,
            host=host,
            port=int(port),
            username=username,
            password=password,
        )
    return None


def build_proxy_pool_from_env(prefix: str = "AHU") -> ProxyPool | None:
    """从环境变量构造代理池。"""
    mode = os.getenv(f"{prefix}_PROXY_MODE", "off").strip().lower()
    if mode in {"", "off", "none", "false"}:
        return None

    default_scheme = os.getenv(f"{prefix}_PROXY_DEFAULT_SCHEME", "http").strip().lower() or "http"
    sid_enabled = _parse_bool(os.getenv(f"{prefix}_PROXY_SID_ENABLED", "false"))
    sid_ttl_minutes = _parse_int(os.getenv(f"{prefix}_PROXY_SID_TTL_MINUTES", "3"), 3)
    sid_length = _parse_int(os.getenv(f"{prefix}_PROXY_SID_LENGTH", "8"), 8)
    cooldown = _parse_int(os.getenv(f"{prefix}_PROXY_COOLDOWN_SECONDS", "30"), 30)
    max_cooldown = _parse_int(os.getenv(f"{prefix}_PROXY_MAX_COOLDOWN_SECONDS", "300"), 300)

    proxy_items: list[str] = []
    if mode == "static":
        value = os.getenv(f"{prefix}_PROXY_URL", "").strip()
        if value:
            proxy_items.append(value)
    elif mode == "pool":
        proxy_items.extend(_split_proxy_items(os.getenv(f"{prefix}_PROXY_LIST", "")))
        list_file = os.getenv(f"{prefix}_PROXY_LIST_FILE", "").strip()
        if list_file:
            proxy_items.extend(_read_proxy_list_file(list_file))
    else:
        return None

    endpoints: list[_ProxyEndpoint] = []
    for item in proxy_items:
        endpoint = _parse_proxy_item(item, default_scheme=default_scheme)
        if endpoint is not None:
            endpoints.append(endpoint)

    if not endpoints:
        return None

    return ProxyPool(
        endpoints=endpoints,
        cooldown_seconds=cooldown,
        max_cooldown_seconds=max_cooldown,
        sid_enabled=sid_enabled,
        sid_ttl_minutes=sid_ttl_minutes,
        sid_length=sid_length,
    )
