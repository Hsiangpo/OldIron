"""Kompass Germany 列表页客户端。"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from curl_cffi import requests as cffi_requests


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
ENV_COOKIE_HEADER = "KOMPASS_COOKIE_HEADER"
ENV_USER_AGENT = "KOMPASS_USER_AGENT"
LOGIN_STATE_NAME = "login_state.json"
COUNTRY_CODE = "de"


@dataclass(slots=True)
class RuntimeRequestState:
    """运行期请求头信息。"""

    cookie_header: str
    user_agent: str


class KompassChallengeError(RuntimeError):
    """Kompass DataDome challenge。"""


def build_list_url(page_number: int) -> str:
    """构造分页 URL。"""
    page = max(int(page_number or 1), 1)
    if page == 1:
        return f"https://us.kompass.com/businessplace/z/{COUNTRY_CODE}/"
    return f"https://us.kompass.com/businessplace/z/{COUNTRY_CODE}/page-{page}/"


class KompassClient:
    """带浏览器 Cookie 的 Kompass 列表页客户端。"""

    def __init__(self, output_dir: Path, proxy: str) -> None:
        self._output_dir = Path(output_dir)
        self._state_path = self._output_dir / "session" / LOGIN_STATE_NAME
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        state = load_runtime_request_state(self._state_path)
        proxies = {"http": proxy, "https": proxy} if proxy else {}
        self._session = cffi_requests.Session(impersonate="chrome124", proxies=proxies)
        self._session.trust_env = False
        self._session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": state.user_agent or DEFAULT_USER_AGENT,
            }
        )
        cookies = parse_cookie_header(state.cookie_header)
        if cookies:
            self._session.cookies.update(cookies)

    def close(self) -> None:
        self._session.close()

    def fetch_list_page(self, page_number: int) -> str:
        """抓取单页 HTML。"""
        url = build_list_url(page_number)
        response = self._session.get(
            url,
            headers={"Referer": build_list_url(max(page_number - 1, 1))},
            timeout=30,
            allow_redirects=True,
        )
        text = response.text or ""
        if response.status_code == 404:
            return ""
        if _looks_like_challenge_response(response.status_code, text):
            raise KompassChallengeError(
                "Kompass 当前返回 DataDome challenge。"
                " 请先用浏览器打开列表页通过一次验证，"
                f" 再把 Cookie 写入 .env 的 {ENV_COOKIE_HEADER}"
                f" 或 output/kompass/session/{LOGIN_STATE_NAME} 后重试。"
            )
        if response.status_code >= 400:
            raise RuntimeError(f"Kompass 请求失败：status={response.status_code} url={url}")
        return text


def load_runtime_request_state(state_path: Path) -> RuntimeRequestState:
    """读取运行期 Cookie 与 UA。"""
    stored = _read_state_file(state_path)
    return RuntimeRequestState(
        cookie_header=str(os.getenv(ENV_COOKIE_HEADER, "") or stored.get("cookie_header") or "").strip(),
        user_agent=str(os.getenv(ENV_USER_AGENT, "") or stored.get("user_agent") or DEFAULT_USER_AGENT).strip(),
    )


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


def _read_state_file(state_path: Path) -> dict[str, str]:
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _looks_like_challenge_response(status_code: int, text: str) -> bool:
    body = str(text or "").lower()
    if status_code in {403, 429}:
        return True
    if "please enable js and disable any ad blocker" in body:
        return True
    if "captcha-delivery.com" in body and "/businessplace/z/" not in body and "/c/" not in body:
        return True
    return False
