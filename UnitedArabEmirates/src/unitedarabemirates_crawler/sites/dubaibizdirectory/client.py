"""DubaiBizDirectory 协议客户端。"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path

from curl_cffi import requests as cffi_requests

from .browser import BrowserCookieState
from .browser import fetch_browser_cookie_state
from .browser import _compact_browser_error


LOGGER = logging.getLogger("uae.dubaibizdirectory.client")
COOKIE_STATE_NAME = "cookie_state.json"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
ENV_COOKIE_HEADER = "DUBAIBIZDIRECTORY_COOKIE_HEADER"
ENV_CF_CLEARANCE = "DUBAIBIZDIRECTORY_CF_CLEARANCE"
ENV_USER_AGENT = "DUBAIBIZDIRECTORY_USER_AGENT"
PERSISTED_COOKIE_KEYS = ("cf_clearance", "cf_chl_rc_ni", "CAKEPHP", "FCCDCF", "FCNEC")
CHALLENGE_MARKERS = (
    "window._cf_chl_opt",
    "Just a moment...",
    "Checking your browser before accessing",
    "Attention Required! | Cloudflare",
    "Sorry, you have been blocked",
    "You are unable to access",
)
MAX_RATE_RETRIES = 4
BROWSER_REFRESH_COOLDOWN_SECONDS = 600


@dataclass(frozen=True)
class RuntimeCookieState:
    """运行期 cookie 状态。"""

    cookies: dict[str, str]
    user_agent: str


class DubaiBizDirectoryClient:
    """复用本地 cf cookie 的协议请求客户端。"""

    def __init__(self, output_dir: Path, proxy: str) -> None:
        self._output_dir = Path(output_dir)
        self._state_path = self._output_dir / "session" / COOKIE_STATE_NAME
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = load_runtime_cookie_state(self._state_path)
        self._proxy = str(proxy or "").strip()
        self._using_proxy = bool(self._proxy)
        self._last_browser_refresh_failure_at = 0.0
        if not self._state.cookies.get("cf_clearance"):
            raise RuntimeError(
                "DubaiBizDirectory 缺少可用 cf_clearance。"
                f"请在 .env 写入 {ENV_CF_CLEARANCE}，"
                f"或写入 {self._state_path}。"
            )
        self._session = self._new_session(use_proxy=self._using_proxy)
        self._persist_state()

    def close(self) -> None:
        self._session.close()

    def fetch_list_html(self, url: str) -> str:
        return self._fetch_html(url, referer=url)

    def fetch_detail_html(self, url: str) -> str:
        return self._fetch_html(url, referer="https://dubaibizdirectory.com/organisations/search/page:1")

    def _fetch_html(self, url: str, referer: str) -> str:
        response = self._request(url, referer)
        text = str(response.text or "")
        if _looks_like_challenge(response.status_code, text) and self._using_proxy:
            LOGGER.info("DubaiBizDirectory cookie 与 dl 不一致，自动切直连继续。")
            self._session.close()
            self._using_proxy = False
            self._session = self._new_session(use_proxy=False)
            response = self._request(url, referer)
            text = str(response.text or "")
        if _looks_like_challenge(response.status_code, text):
            if self._refresh_cookie_state_via_browser(url):
                response = self._request(url, referer)
                text = str(response.text or "")
        if _looks_like_challenge(response.status_code, text):
            raise RuntimeError(
                "DubaiBizDirectory 的 cf cookie 已失效，"
                f"请刷新 {self._state_path} 或 .env 里的 {ENV_CF_CLEARANCE}。"
            )
        response.raise_for_status()
        self._capture_session_state()
        return text

    def _new_session(self, *, use_proxy: bool) -> cffi_requests.Session:
        session = _build_session(self._proxy if use_proxy else "", self._state.user_agent)
        session.cookies.update(self._state.cookies)
        return session

    def _request(self, url: str, referer: str):
        for attempt in range(1, MAX_RATE_RETRIES + 1):
            response = self._session.get(
                url,
                headers={"Referer": referer},
                timeout=30,
                allow_redirects=True,
            )
            if response.status_code != 429:
                return response
            wait_seconds = _retry_after_seconds(response.headers.get("Retry-After"))
            LOGGER.info(
                "DubaiBizDirectory 命中 429，等待 %d 秒后重试：attempt=%d url=%s",
                wait_seconds,
                attempt,
                url,
            )
            time.sleep(wait_seconds)
        raise RuntimeError(f"DubaiBizDirectory 连续命中 429：{url}")

    def _persist_state(self) -> None:
        save_runtime_cookie_state(self._state_path, self._state)

    def _capture_session_state(self) -> None:
        session_cookies = getattr(self._session, "cookies", self._state.cookies)
        self._state = RuntimeCookieState(
            cookies=_filter_cookies(dict(session_cookies)),
            user_agent=self._state.user_agent,
        )
        self._persist_state()

    def _refresh_cookie_state_via_browser(self, target_url: str) -> bool:
        last_failure_at = float(getattr(self, "_last_browser_refresh_failure_at", 0.0) or 0.0)
        if time.monotonic() - last_failure_at < BROWSER_REFRESH_COOLDOWN_SECONDS:
            LOGGER.warning("DubaiBizDirectory 浏览器刷新仍在冷却期，暂不重复拉起浏览器。")
            return False
        LOGGER.warning("DubaiBizDirectory 命中 challenge，尝试用浏览器刷新 cookie：%s", target_url)
        try:
            browser_state = fetch_browser_cookie_state(
                user_data_dir=self._output_dir / "browser_profile",
                target_url=target_url,
                proxy_url=self._proxy if self._using_proxy else "",
            )
        except Exception as exc:  # noqa: BLE001
            self._last_browser_refresh_failure_at = time.monotonic()
            LOGGER.warning("DubaiBizDirectory 浏览器刷新 cookie 失败：%s", _compact_browser_error(exc))
            return False
        self._replace_runtime_state(browser_state)
        self._last_browser_refresh_failure_at = 0.0
        return True

    def _replace_runtime_state(self, browser_state: BrowserCookieState) -> None:
        self._state = RuntimeCookieState(
            cookies=_filter_cookies(browser_state.cookies),
            user_agent=str(browser_state.user_agent or self._state.user_agent).strip() or DEFAULT_USER_AGENT,
        )
        try:
            self._session.close()
        except Exception:  # noqa: BLE001
            pass
        self._session = self._new_session(use_proxy=self._using_proxy)
        self._persist_state()


def load_runtime_cookie_state(state_path: Path) -> RuntimeCookieState:
    """读取本地 cookie 状态，并合并环境变量覆盖。"""
    stored = _read_state_file(state_path)
    cookies = _filter_cookies(dict(stored.get("cookies", {})))
    header_cookies = parse_cookie_header(os.getenv(ENV_COOKIE_HEADER, ""))
    if header_cookies:
        cookies.update(_filter_cookies(header_cookies))
    env_clearance = str(os.getenv(ENV_CF_CLEARANCE, "") or "").strip()
    if env_clearance:
        cookies["cf_clearance"] = env_clearance
    user_agent = str(os.getenv(ENV_USER_AGENT, "") or "").strip() or str(
        stored.get("user_agent") or DEFAULT_USER_AGENT
    )
    return RuntimeCookieState(cookies=cookies, user_agent=user_agent)


def save_runtime_cookie_state(state_path: Path, state: RuntimeCookieState) -> None:
    """把当前 cookie 状态持久化到本地文件。"""
    payload = {
        "user_agent": state.user_agent,
        "cookies": _filter_cookies(state.cookies),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_cookie_header(cookie_text: str) -> dict[str, str]:
    """把整段 Cookie 头拆成字典。"""
    results: dict[str, str] = {}
    for chunk in str(cookie_text or "").split(";"):
        item = str(chunk or "").strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        clean_key = str(key or "").strip()
        clean_value = str(value or "").strip()
        if clean_key and clean_value:
            results[clean_key] = clean_value
    return results


def _build_session(proxy: str, user_agent: str) -> cffi_requests.Session:
    proxies = {}
    if proxy:
        proxies = {"http": proxy, "https": proxy}
    session = cffi_requests.Session(impersonate="chrome", proxies=proxies)
    session.trust_env = False
    session.headers.update(
        {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": user_agent,
        }
    )
    return session


def _read_state_file(state_path: Path) -> dict[str, object]:
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if "cookies" in payload and isinstance(payload["cookies"], dict):
        return payload
    direct_cookies = _filter_cookies(payload)
    return {"cookies": direct_cookies, "user_agent": payload.get("user_agent", "")}


def _filter_cookies(source: dict[str, object]) -> dict[str, str]:
    return {
        key: str(source.get(key) or "").strip()
        for key in PERSISTED_COOKIE_KEYS
        if str(source.get(key) or "").strip()
    }


def _looks_like_challenge(status_code: int, html_text: str) -> bool:
    text = str(html_text or "")
    if int(status_code or 0) == 403:
        return True
    return any(marker in text for marker in CHALLENGE_MARKERS)


def _retry_after_seconds(header_value: str | None) -> int:
    try:
        seconds = int(str(header_value or "").strip() or "10")
    except ValueError:
        seconds = 10
    return max(seconds, 1)
