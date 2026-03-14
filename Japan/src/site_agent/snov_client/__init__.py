from __future__ import annotations

import asyncio
import json
import locale
import os
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover - optional dependency from uvicorn[standard]
    websockets = None


class SnovClient:
    def __init__(
        self,
        *,
        timeout: int = 20,
        extension_selector: str | None = None,
        extension_token: str | None = None,
        extension_fingerprint: str | None = None,
        extension_cdp_host: str | None = None,
        extension_cdp_port: int | None = None,
        extension_only: bool = True,
        extension_cookies: dict[str, str] | None = None,
    ) -> None:
        self.timeout = timeout
        self.extension_selector = extension_selector
        self.extension_token = extension_token
        self.extension_fingerprint = extension_fingerprint
        self.extension_cdp_host = extension_cdp_host or "127.0.0.1"
        self.extension_cdp_port = extension_cdp_port
        self.extension_only = extension_only
        # requests.Session 不是严格线程安全；这里用线程本地 session 避免串行瓶颈
        self._local = threading.local()
        self._auth_session = requests.Session()
        self.last_source: str | None = None
        self._extension_cdp_checked = False
        self._extension_auth_checked = False
        self._extension_cookies: dict[str, str] = {}
        self._extension_cookie_ts: float = 0.0
        self._just_refreshed: bool = False
        self._extension_request_lock = threading.Lock()
        self._extension_last_request_ts: float = 0.0
        self._extension_min_interval = 0.0
        self._extension_last_refresh_ts: float = 0.0
        self._extension_refresh_min_interval = 60.0
        if isinstance(extension_cookies, dict) and extension_cookies:
            self._extension_cookies = {
                str(k): str(v)
                for k, v in extension_cookies.items()
                if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip()
            }
            self._extension_cookie_ts = time.time()
        if not self._extension_cookies:
            cookie_file = (os.environ.get("SNOV_EXTENSION_COOKIE_FILE") or "").strip()
            if not cookie_file:
                cookie_file = "output/snov_extension_cookies.json"
            cookie_map = _load_extension_cookies_from_file(Path(cookie_file))
            if cookie_map:
                self._extension_cookies = cookie_map
                self._extension_cookie_ts = time.time()
        if not self.extension_selector and self._extension_cookies.get("selector"):
            self.extension_selector = self._extension_cookies["selector"]
        if not self.extension_token and self._extension_cookies.get("token"):
            self.extension_token = self._extension_cookies["token"]
        if not self.extension_fingerprint and self._extension_cookies.get("fingerprint"):
            self.extension_fingerprint = self._extension_cookies["fingerprint"]
        self._sync_extension_tokens_from_cookies()

    def _sync_extension_tokens_from_cookies(self) -> None:
        if not self._extension_cookies:
            return
        selector = self._extension_cookies.get("selector")
        token = self._extension_cookies.get("token")
        fingerprint = self._extension_cookies.get("fingerprint")
        if isinstance(selector, str) and selector.strip():
            self.extension_selector = selector.strip()
        if isinstance(token, str) and token.strip():
            self.extension_token = token.strip()
        if isinstance(fingerprint, str) and fingerprint.strip():
            self.extension_fingerprint = fingerprint.strip()

    def get_domain_emails(
        self,
        domain: str,
        *,
        page_url: str | None = None,
        max_wait_seconds: int = 40,
    ) -> list[str]:
        if not domain:
            return []
        self._sync_extension_tokens_from_cookies()
        page_url = page_url.strip() if isinstance(page_url, str) else None
        if page_url and self.extension_cdp_port and not self._extension_cdp_checked:
            self._load_extension_cookies_from_cdp()
            self._sync_extension_tokens_from_cookies()
        if page_url and self._extension_ready() and not self._extension_auth_checked:
            self._ensure_extension_auth()
        if page_url and self._extension_ready():
            try:
                emails = self._get_domain_emails_via_extension(page_url)
            except Exception:
                self.last_source = "extension"
                return []
            if emails:
                self.last_source = "extension"
                return emails
            self.last_source = "extension"
            return []
        self.last_source = "extension_missing"
        return []

    def _extension_ready(self) -> bool:
        return bool(self.extension_selector and self.extension_token)

    def refresh_extension_cookies(self) -> bool:
        if not self.extension_cdp_port:
            self._just_refreshed = False
            return False
        now = time.time()
        if self._extension_last_refresh_ts and now - self._extension_last_refresh_ts < self._extension_refresh_min_interval:
            ready = self._extension_ready()
            self._just_refreshed = bool(ready)
            return ready
        self._extension_cdp_checked = False
        self._extension_auth_checked = False
        self._extension_cookies = {}
        self._load_extension_cookies_from_cdp()
        self._ensure_extension_auth()
        ready = self._extension_ready()
        self._just_refreshed = bool(ready)
        self._extension_last_refresh_ts = time.time()
        return ready

    def _load_extension_cookies_from_cdp(self) -> None:
        self._extension_cdp_checked = True
        if not self.extension_cdp_port or websockets is None:
            return
        try:
            ws_url = _get_cdp_ws_url(self.extension_cdp_host, self.extension_cdp_port, timeout=self.timeout)
            if not ws_url:
                return
            cookies = _fetch_cdp_cookies(ws_url, ["https://app.snov.io/"])
        except Exception:
            return
        if not cookies:
            return
        cookie_map: dict[str, str] = {}
        for cookie in cookies:
            name = cookie.get("name")
            value = cookie.get("value")
            if not (isinstance(name, str) and isinstance(value, str)):
                continue
            cookie_map[name] = value
            if name == "selector":
                self.extension_selector = value
            elif name == "token":
                self.extension_token = value
            elif name == "fingerprint":
                self.extension_fingerprint = value
        if cookie_map:
            self._extension_cookies = cookie_map
            self._extension_cookie_ts = time.time()

    def _ensure_extension_auth(self) -> None:
        self._extension_auth_checked = True
        if not (self.extension_selector and self.extension_token):
            return
        try:
            resp = self._auth_session.post(
                "https://app.snov.io/api/checkAuth",
                data={
                    "selector": self.extension_selector,
                    "token": self.extension_token,
                },
                timeout=self.timeout,
            )
        except Exception:
            return
        if resp.status_code >= 400:
            return
        try:
            payload = resp.json()
        except Exception:
            return
        if not isinstance(payload, dict) or not payload.get("result"):
            return
        new_token = payload.get("token")
        new_fingerprint = payload.get("fingerprint")
        if isinstance(new_token, str) and new_token.strip():
            self.extension_token = new_token.strip()
            self._extension_cookies["token"] = self.extension_token
        if isinstance(new_fingerprint, str) and new_fingerprint.strip():
            self.extension_fingerprint = new_fingerprint.strip()
            self._extension_cookies["fingerprint"] = self.extension_fingerprint

    def _get_domain_emails_via_extension(self, page_url: str) -> list[str]:
        url = "https://app.snov.io/extension/api/contacts/get-by-domain"
        session = self._get_session()

        def _throttle_request() -> None:
            if self._extension_min_interval <= 0:
                return
            with self._extension_request_lock:
                now = time.time()
                wait = self._extension_min_interval - (now - self._extension_last_request_ts)
                if wait > 0:
                    time.sleep(wait)
                self._extension_last_request_ts = time.time()

        def _build_cookies() -> dict[str, str]:
            # 尽量使用完整 CDP cookies；若 cookies 过期/缺失则刷新，避免只带 selector/token 导致脱敏
            if self.extension_cdp_port:
                now = time.time()
                if (not self._extension_cookies) or (now - self._extension_cookie_ts > 60):
                    self._load_extension_cookies_from_cdp()
                    self._sync_extension_tokens_from_cookies()
            cookies = dict(self._extension_cookies)
            if self.extension_selector:
                cookies["selector"] = self.extension_selector
            if self.extension_token:
                cookies["token"] = self.extension_token
            if self.extension_fingerprint:
                cookies["fingerprint"] = self.extension_fingerprint
            return cookies

        def _extension_language() -> str:
            env_lang = (os.environ.get("SNOV_CONTENT_LANGUAGE") or "").strip()
            if env_lang:
                return env_lang.lower().replace("-", "_")
            sys_lang = ""
            try:
                sys_lang = locale.getdefaultlocale()[0] or ""
            except Exception:
                sys_lang = ""
            sys_lang = sys_lang.strip().lower().replace("-", "_")
            return sys_lang or "en"

        def _make_headers() -> dict[str, str]:
            # 模拟扩展请求头，降低被判为“非扩展来源”而返回脱敏值的概率
            return {
                "Accept": "application/json",
                "Content-Language": _extension_language(),
                "Content-Type": "application/json",
                "Ext-Version": "2.3.24",
                "Origin": "chrome-extension://einnffiilpmgldkapbikhkeicohlaapj",
                "Referer": "chrome-extension://einnffiilpmgldkapbikhkeicohlaapj/html/popup.html",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }

        def _build_link_candidates(raw_url: str) -> list[str]:
            raw = raw_url.strip()
            if not raw:
                return []
            candidates: list[str] = []
            seen: set[str] = set()

            def add(value: str | None) -> None:
                if not value:
                    return
                cleaned = value.strip()
                if not cleaned or cleaned in seen:
                    return
                seen.add(cleaned)
                candidates.append(cleaned)

            add(raw)
            normalized = raw if "://" in raw else f"https://{raw}"
            add(normalized)
            parsed = urlparse(normalized)
            host = parsed.netloc or parsed.path
            host = host.strip().lstrip("/")
            if "@" in host:
                host = host.split("@", 1)[-1]
            if ":" in host:
                host = host.split(":", 1)[0]
            if host:
                add(f"{parsed.scheme}://{host}" if parsed.scheme else f"https://{host}")
                add(f"https://{host}")
                add(f"http://{host}")
                add(host)
                if host.startswith("www."):
                    add(host[4:])
                else:
                    add(f"www.{host}")
            return candidates

        def _request_once(link: str) -> list[str]:
            cookies = _build_cookies()
            if not cookies:
                raise RuntimeError("Snov extension cookies missing; 请确认 9222 CDP 可用且扩展已登录")
            _throttle_request()
            resp = session.get(
                url,
                params={"link": link},
                headers=_make_headers(),
                cookies=cookies,
                timeout=self.timeout,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"Snov extension HTTP {resp.status_code}: {resp.text[:180]}")
            try:
                payload = resp.json()
            except Exception as exc:
                raise RuntimeError(f"Snov extension response parse error: {exc}") from exc
            if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                payload = payload.get("data")
            if isinstance(payload, dict) and payload.get("result") == 0:
                return []
            return _extract_extension_emails(payload)

        link_candidates = _build_link_candidates(page_url)
        for link in link_candidates:
            emails = _request_once(link)
            if not emails:
                continue
            # If masked, refresh cookies and retry immediately once.
            if any("*" in e for e in emails):
                refreshed = self.refresh_extension_cookies()
                if refreshed:
                    self._just_refreshed = True
                emails = _request_once(link)
            return emails
        return []

    def _get_session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if isinstance(session, requests.Session):
            return session
        session = requests.Session()
        self._local.session = session
        return session


def _get_cdp_ws_url(host: str, port: int, *, timeout: int) -> str | None:
    base = f"http://{host}:{port}"
    resp = requests.get(f"{base}/json", timeout=timeout)
    if resp.status_code >= 400:
        return None
    try:
        pages = resp.json()
    except Exception:
        return None
    if isinstance(pages, list):
        preferred = None
        fallback = None
        for page in pages:
            if not isinstance(page, dict):
                continue
            ws_url = page.get("webSocketDebuggerUrl")
            if not isinstance(ws_url, str) or not ws_url.strip():
                continue
            page_type = page.get("type")
            page_url = page.get("url") if isinstance(page.get("url"), str) else ""
            if page_type == "page" and page_url.startswith("https://app.snov.io/"):
                preferred = ws_url.strip()
                break
            if page_type == "page" and not fallback:
                fallback = ws_url.strip()
        if preferred:
            return preferred
        if fallback:
            return fallback
    resp = requests.get(f"{base}/json/version", timeout=timeout)
    if resp.status_code >= 400:
        return None
    try:
        payload = resp.json()
    except Exception:
        payload = {}
    ws_url = payload.get("webSocketDebuggerUrl") if isinstance(payload, dict) else None
    if isinstance(ws_url, str) and ws_url.strip():
        return ws_url.strip()
    return None


def _load_extension_cookies_from_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return {}
    if isinstance(data, dict):
        cookies = data.get("cookies") if isinstance(data.get("cookies"), dict) else data
        if isinstance(cookies, dict):
            return {
                str(k): str(v)
                for k, v in cookies.items()
                if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip()
            }
    if isinstance(data, list):
        cookie_map: dict[str, str] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            value = item.get("value")
            if isinstance(name, str) and isinstance(value, str) and name.strip() and value.strip():
                cookie_map[name] = value
        return cookie_map
    return {}

def _run_coro_sync(factory: Any) -> list[dict[str, Any]]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        coro = None
        try:
            coro = factory()
            return asyncio.run(coro)
        except Exception:
            if asyncio.iscoroutine(coro):
                coro.close()
            raise
    result: list[dict[str, Any]] = []
    error: Exception | None = None

    def runner() -> None:
        nonlocal result, error
        loop = asyncio.new_event_loop()
        coro = None
        try:
            asyncio.set_event_loop(loop)
            coro = factory()
            result = loop.run_until_complete(coro)
        except Exception as exc:
            error = exc
            if asyncio.iscoroutine(coro):
                coro.close()
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            asyncio.set_event_loop(None)
            loop.close()

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join()
    if error:
        raise error
    return result

def _fetch_cdp_cookies(ws_url: str, urls: list[str]) -> list[dict[str, Any]]:
    if websockets is None:
        return []

    def _filter_by_urls(cookies: list[dict[str, Any]]) -> list[dict[str, Any]]:
        hosts: set[str] = set()
        for url in urls:
            host = urlparse(url).hostname
            if host:
                hosts.add(host.lower())
        if not hosts:
            return cookies

        filtered: list[dict[str, Any]] = []
        for cookie in cookies:
            domain = cookie.get("domain") if isinstance(cookie, dict) else None
            if not isinstance(domain, str) or not domain:
                continue
            dom = domain.lstrip(".").lower()
            for host in hosts:
                if host == dom or host.endswith(f".{dom}"):
                    filtered.append(cookie)
                    break
        return filtered

    async def _run() -> list[dict[str, Any]]:
        msg_id = 1
        async with websockets.connect(ws_url, ping_interval=None) as ws:
            await ws.send(json.dumps({"id": msg_id, "method": "Network.enable"}))
            msg_id += 1
            await ws.send(json.dumps({"id": msg_id, "method": "Network.getCookies", "params": {"urls": urls}}))
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(raw)
                if data.get("id") == msg_id:
                    result = data.get("result") if isinstance(data, dict) else None
                    cookies = result.get("cookies") if isinstance(result, dict) else None
                    cookies_list = cookies if isinstance(cookies, list) else []
                    if cookies_list:
                        return cookies_list
                    break

            msg_id += 1
            await ws.send(json.dumps({"id": msg_id, "method": "Network.getAllCookies"}))
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(raw)
                if data.get("id") == msg_id:
                    result = data.get("result") if isinstance(data, dict) else None
                    cookies = result.get("cookies") if isinstance(result, dict) else None
                    cookies_list = cookies if isinstance(cookies, list) else []
                    return _filter_by_urls(cookies_list)
    return _run_coro_sync(_run)


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        cleaned = item.strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
    return out


def _extract_extension_emails(payload: Any) -> list[str]:
    emails: list[str] = []
    if isinstance(payload, dict):
        items = payload.get("list")
        if not isinstance(items, list):
            items = payload.get("emails")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, str) and item.strip():
                    emails.append(item.strip())
                elif isinstance(item, dict):
                    email = item.get("email")
                    if isinstance(email, str) and email.strip():
                        emails.append(email.strip())
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, str) and item.strip():
                emails.append(item.strip())
            elif isinstance(item, dict):
                email = item.get("email")
                if isinstance(email, str) and email.strip():
                    emails.append(email.strip())
    return _dedupe_keep_order(emails)
