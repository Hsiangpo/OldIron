from __future__ import annotations

import time
from dataclasses import dataclass
import random
import threading
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Callable

import requests


_LOG_SINK: ContextVar[Callable[[str], None] | None] = ContextVar("gmap_agent_log_sink", default=None)


def set_log_sink(sink: Callable[[str], None] | None) -> Any:
    return _LOG_SINK.set(sink)


def reset_log_sink(token: Any) -> None:
    _LOG_SINK.reset(token)


def _print_ts(message: str) -> None:
    text = str(message or "").strip()
    if not text:
        return
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} {text}", flush=True)


@dataclass
class HttpConfig:
    hl: str = "ja"
    gl: str = "jp"
    cookie: str | None = None
    proxy: str | None = None
    timeout: int = 20
    max_retries: int = 3
    backoff_seconds: float = 1.5
    infinite_retry_429: bool = False
    retry_min_seconds: float = 5.0
    retry_max_seconds: float = 20.0


class HttpClient:
    def __init__(self, config: HttpConfig) -> None:
        self.config = config
        self._local = threading.local()
        self._base_headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate",
            "accept-language": config.hl,
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }
        self._proxy: str | None = None
        if config.cookie:
            self._base_headers["cookie"] = config.cookie
        proxy = config.proxy or _detect_windows_inet_proxy()
        if proxy:
            self._proxy = proxy
            self.config.proxy = proxy

    def _get_session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if isinstance(session, requests.Session):
            return session
        session = requests.Session()
        session.trust_env = True
        session.headers.update(self._base_headers)
        if self._proxy:
            session.proxies.update({"http": self._proxy, "https": self._proxy})
        self._local.session = session
        return session

    def get(self, url: str) -> str:
        """
        访问 Google 搜索接口：
        - 429（限流）可配置：勾选“无限重试”时，等待 20~40s 并持续重试；
        - 其他网络/HTTP 错误按 max_retries 重试。
        """
        session = self._get_session()
        last_exc: Exception | None = None
        normal_attempts = 0
        limited_attempts = 0
        while True:
            try:
                resp = session.get(url, timeout=self.config.timeout)
                status = int(resp.status_code)
                if status == 200:
                    text = resp.text or ""
                    if _looks_like_rate_limited_page(text):
                        limited_attempts += 1
                        if self.config.infinite_retry_429:
                            wait_s = random.uniform(self.config.retry_min_seconds, self.config.retry_max_seconds)
                            msg = f"[谷歌] 访问受限（疑似触发验证/限流），{wait_s:.0f}s 后继续尝试…"
                            sink = _LOG_SINK.get()
                            if sink:
                                sink(msg)
                            else:
                                _print_ts(msg)
                            time.sleep(wait_s)
                            continue
                        raise requests.HTTPError("rate_limited")
                    return text
                if status == 429:
                    limited_attempts += 1
                    if self.config.infinite_retry_429:
                        wait_s = random.uniform(self.config.retry_min_seconds, self.config.retry_max_seconds)
                        msg = f"[谷歌] 访问频繁被限流(429)，{wait_s:.0f}s 后继续尝试…"
                        sink = _LOG_SINK.get()
                        if sink:
                            sink(msg)
                        else:
                            _print_ts(msg)
                        time.sleep(wait_s)
                        continue
                    raise requests.HTTPError("rate_limited_429")
                raise requests.HTTPError(f"HTTP {status}")
            except Exception as exc:  # pragma: no cover - network errors vary
                last_exc = exc
                normal_attempts += 1
                if normal_attempts < max(1, int(self.config.max_retries)):
                    time.sleep(self.config.backoff_seconds * normal_attempts)
                    continue
                raise RuntimeError(f"request failed: {last_exc}") from last_exc


def _looks_like_rate_limited_page(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False
    markers = [
        "our systems have detected unusual traffic",
        "unusual traffic from your computer network",
        "/sorry/",
        "to continue, please type the characters",
        "please show you're not a robot",
    ]
    return any(m in t for m in markers)


def _detect_windows_inet_proxy() -> str | None:
    """
    读取 Windows「Internet 选项」代理设置（WinINet）。

    说明：
    - 某些环境会通过本机代理(如 127.0.0.1:7890)访问 Google；
    - Python requests 默认不读取该注册表配置，导致直连被 DNS 污染/阻断；
    - 这里自动兜底读取，保证 gmap_agent 能像浏览器一样访问 Google。
    """
    try:
        import sys

        if sys.platform != "win32":
            return None
    except Exception:
        return None

    try:
        import winreg  # type: ignore
    except Exception:
        return None

    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
        enabled, _ = winreg.QueryValueEx(key, "ProxyEnable")
        server, _ = winreg.QueryValueEx(key, "ProxyServer")
    except OSError:
        return None

    try:
        if int(enabled) != 1:
            return None
    except Exception:
        return None

    if not isinstance(server, str):
        return None
    value = server.strip()
    if not value:
        return None

    # 常见格式：
    # 1) "127.0.0.1:7890"
    # 2) "http=127.0.0.1:7890;https=127.0.0.1:7890"
    picked: str | None = None
    if "=" in value:
        items = [part.strip() for part in value.split(";") if part.strip()]
        by_proto: dict[str, str] = {}
        for item in items:
            if "=" not in item:
                continue
            proto, addr = item.split("=", 1)
            proto = proto.strip().lower()
            addr = addr.strip()
            if not addr:
                continue
            by_proto[proto] = addr
        picked = by_proto.get("https") or by_proto.get("http")
    else:
        picked = value

    if not picked:
        return None
    if picked.startswith("http://") or picked.startswith("https://"):
        return picked
    return "http://" + picked
