"""从 9222 调试浏览器同步 Virk cookie。"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import requests
import websocket


DEFAULT_CHROME_DEBUG_URL = "http://127.0.0.1:9222"
DEFAULT_VIRK_HOST = "datacvr.virk.dk"


def _cookie_matches_host(cookie_domain: str, host: str) -> bool:
    domain = str(cookie_domain or "").strip().lstrip(".").lower()
    target = str(host or "").strip().lower()
    if not domain or not target:
        return False
    return target == domain or target.endswith(f".{domain}") or domain.endswith(f".{target}")


def _build_cookie_header(cookies: list[dict[str, Any]], host: str) -> str:
    now = time.time()
    selected: dict[str, tuple[tuple[int, int, float], str]] = {}
    for item in cookies:
        name = str(item.get("name", "")).strip()
        value = str(item.get("value", "")).strip()
        if not name or not value:
            continue
        if not _cookie_matches_host(str(item.get("domain", "")), host):
            continue
        expires = item.get("expires", -1)
        try:
            expires_at = float(expires)
        except (TypeError, ValueError):
            expires_at = -1.0
        if expires_at > 0 and expires_at <= now:
            continue
        path = str(item.get("path", "/") or "/")
        domain = str(item.get("domain", "")).strip()
        priority = (len(domain.lstrip(".")), len(path), expires_at)
        current = selected.get(name)
        if current is None or priority >= current[0]:
            selected[name] = (priority, value)
    return "; ".join(f"{name}={value}" for name, (_, value) in sorted(selected.items()))


def _fetch_browser_ws_url(debug_url: str, timeout_seconds: float) -> str:
    response = requests.get(f"{debug_url.rstrip('/')}/json/version", timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    return str(payload.get("webSocketDebuggerUrl", "")).strip()


def _fetch_cdp_cookies(ws_url: str, timeout_seconds: float) -> list[dict[str, Any]]:
    ws = websocket.create_connection(ws_url, timeout=timeout_seconds, suppress_origin=True)
    try:
        ws.send(json.dumps({"id": 1, "method": "Storage.getCookies"}, ensure_ascii=False))
        while True:
            payload = json.loads(ws.recv())
            if int(payload.get("id", 0) or 0) != 1:
                continue
            result = payload.get("result", {})
            cookies = result.get("cookies", [])
            return cookies if isinstance(cookies, list) else []
    finally:
        ws.close()


def fetch_live_virk_cookie_header(
    *,
    debug_url: str = DEFAULT_CHROME_DEBUG_URL,
    host: str = DEFAULT_VIRK_HOST,
    timeout_seconds: float = 10.0,
) -> str:
    ws_url = _fetch_browser_ws_url(debug_url, timeout_seconds)
    if not ws_url:
        return ""
    cookies = _fetch_cdp_cookies(ws_url, timeout_seconds)
    return _build_cookie_header(cookies, host)


def _write_cookie_header_to_env(env_path: Path, cookie_header: str) -> None:
    if not cookie_header:
        return
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    new_lines: list[str] = []
    replaced = False
    for line in lines:
        if line.startswith("VIRK_COOKIE_HEADER="):
            new_lines.append(f"VIRK_COOKIE_HEADER={cookie_header}")
            replaced = True
            continue
        new_lines.append(line)
    if not replaced:
        new_lines.append(f"VIRK_COOKIE_HEADER={cookie_header}")
    env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def resolve_virk_cookie_header(
    *,
    project_root: Path,
    logger: logging.Logger | None = None,
    allow_env_fallback: bool = False,
) -> str:
    log = logger or logging.getLogger(__name__)
    fallback = os.getenv("VIRK_COOKIE_HEADER", "").strip()
    debug_url = os.getenv("VIRK_CHROME_DEBUG_URL", DEFAULT_CHROME_DEBUG_URL).strip() or DEFAULT_CHROME_DEBUG_URL
    try:
        live_cookie = fetch_live_virk_cookie_header(debug_url=debug_url)
    except Exception as exc:  # noqa: BLE001
        if allow_env_fallback and fallback:
            log.warning("读取 9222 浏览器 Virk cookie 失败，继续使用 .env：%s", exc)
            return fallback
        log.warning("读取 9222 浏览器 Virk cookie 失败：%s", exc)
        return ""
    if not live_cookie:
        if allow_env_fallback and fallback:
            log.warning("9222 浏览器未返回 Virk cookie，继续使用 .env。")
            return fallback
        log.warning("9222 浏览器未返回 Virk cookie。")
        return ""
    if live_cookie != fallback:
        _write_cookie_header_to_env(project_root / ".env", live_cookie)
        os.environ["VIRK_COOKIE_HEADER"] = live_cookie
        log.info("已从 9222 浏览器刷新 VIRK_COOKIE_HEADER。")
    return live_cookie


class VirkCookieProvider:
    """运行时按需刷新 Virk cookie。"""

    def __init__(
        self,
        *,
        project_root: Path,
        logger: logging.Logger | None = None,
        min_refresh_seconds: float = 60.0,
        allow_env_fallback: bool = False,
    ) -> None:
        self.project_root = Path(project_root)
        self.logger = logger or logging.getLogger(__name__)
        self.min_refresh_seconds = max(float(min_refresh_seconds), 0.0)
        self.allow_env_fallback = bool(allow_env_fallback)
        self._lock = threading.RLock()
        self._cached_cookie = os.getenv("VIRK_COOKIE_HEADER", "").strip()
        self._last_refresh_at = 0.0

    def get(self, *, force_refresh: bool = False) -> str:
        with self._lock:
            now = time.monotonic()
            should_refresh = force_refresh or not self._cached_cookie
            if not should_refresh and self.min_refresh_seconds > 0:
                should_refresh = (now - self._last_refresh_at) >= self.min_refresh_seconds
            if not should_refresh:
                return self._cached_cookie
            cookie = resolve_virk_cookie_header(
                project_root=self.project_root,
                logger=self.logger,
                allow_env_fallback=self.allow_env_fallback,
            ).strip()
            if cookie:
                self._cached_cookie = cookie
            self._last_refresh_at = now
            return self._cached_cookie

