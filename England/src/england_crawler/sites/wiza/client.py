"""Wiza 协议客户端。"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from curl_cffi import requests as cffi_requests


LOGGER = logging.getLogger("england.wiza.client")
LOGIN_STATE_NAME = "login_state.json"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
ENV_COOKIE_HEADER = "WIZA_COOKIE_HEADER"
ENV_USER_AGENT = "WIZA_USER_AGENT"
ENV_CSRF_TOKEN = "WIZA_CSRF_TOKEN"
ENV_USER_ID = "WIZA_USER_ID"
ENV_ACCOUNT_ID = "WIZA_ACCOUNT_ID"
ENV_REFERER = "WIZA_REFERER"
REQUIRED_COOKIE_KEYS = ("user_secret", "wz_session")
COMPANY_FILTER = {"v": "united kingdom", "b": "country", "s": "i"}
DEFAULT_REFERER = "https://wiza.co/app/prospect"


@dataclass(slots=True)
class RuntimeLoginState:
    """运行期登录态。"""

    cookies: dict[str, str]
    user_agent: str
    csrf_token: str
    user_id: str
    account_id: str


@dataclass(slots=True)
class WizaCompanyPage:
    """公司列表分页结果。"""

    items: list[dict[str, Any]]
    last_sort: list[Any]
    total: int
    total_relation: str
    page_size: int


class WizaClient:
    """复用本地登录态的 Wiza 协议客户端。"""

    def __init__(self, output_dir: Path, proxy: str) -> None:
        self._output_dir = Path(output_dir)
        self._state_path = self._output_dir / "session" / LOGIN_STATE_NAME
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = load_runtime_login_state(self._state_path)
        self._proxy = str(proxy or "").strip()
        self._referer = str(os.getenv(ENV_REFERER, DEFAULT_REFERER) or DEFAULT_REFERER).strip()
        _ensure_required_cookies(self._state.cookies)
        self._session = _build_session(self._proxy, self._state.user_agent)
        self._session.cookies.update(self._state.cookies)
        self._save_state()

    def close(self) -> None:
        self._save_state()
        self._session.close()

    def search_companies(self, *, search_after: list[Any] | None, page_size: int) -> WizaCompanyPage:
        body = {
            "query": {"company_location": [dict(COMPANY_FILTER)]},
            "search_after": search_after,
            "page_size": int(page_size),
        }
        payload = self._post_json("/svc/app/prospect/search_companies", body)
        return WizaCompanyPage(
            items=list(payload.get("data") or []),
            last_sort=list(payload.get("last_sort") or []),
            total=int(payload.get("total") or 0),
            total_relation=str(payload.get("total_relation") or ""),
            page_size=int(page_size),
        )

    def _post_json(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        last_error = ""
        for attempt in range(1, 4):
            response = self._session.post(
                f"https://wiza.co{path}",
                json=body,
                headers=_build_request_headers(self._state, self._referer),
                timeout=30,
                allow_redirects=True,
            )
            self._save_state()
            payload = response.json()
            data = payload.get("body", payload) if isinstance(payload, dict) else {}
            if isinstance(data, str):
                data = json.loads(data)
            if not isinstance(data, dict):
                raise RuntimeError(f"Wiza 返回结构异常：{path}")
            status = int(data.get("status") or payload.get("status") or response.status_code or 0)
            message = str(data.get("message") or "").strip()
            if status == 200:
                return data
            last_error = f"{path} -> {data}"
            if _is_temporary_limit(status, message) and attempt < 3:
                wait_seconds = attempt * 3
                LOGGER.warning("Wiza 命中临时 usage limit，等待 %ds 后重试：path=%s", wait_seconds, path)
                time.sleep(wait_seconds)
                continue
            if _is_temporary_limit(status, message):
                raise WizaUsageLimitError("Wiza 当前账号搜索额度已用尽，暂时无法继续抓公司列表。")
            response.raise_for_status()
            raise RuntimeError(f"Wiza 请求失败：{last_error}")
        raise RuntimeError(f"Wiza 请求失败：{last_error}")

    def _save_state(self) -> None:
        save_runtime_login_state(
            self._state_path,
            RuntimeLoginState(
                cookies={key: value for key, value in dict(self._session.cookies).items() if str(value or "").strip()},
                user_agent=self._state.user_agent,
                csrf_token=self._state.csrf_token,
                user_id=self._state.user_id,
                account_id=self._state.account_id,
            ),
        )


def load_runtime_login_state(state_path: Path) -> RuntimeLoginState:
    """读取登录态，并合并环境变量覆盖。"""
    stored = _read_state_file(state_path)
    cookies = {
        key: str(value or "").strip()
        for key, value in dict(stored.get("cookies") or {}).items()
        if str(value or "").strip()
    }
    cookies.update(parse_cookie_header(os.getenv(ENV_COOKIE_HEADER, "")))
    return RuntimeLoginState(
        cookies=cookies,
        user_agent=str(os.getenv(ENV_USER_AGENT, "") or stored.get("user_agent") or DEFAULT_USER_AGENT).strip(),
        csrf_token=str(os.getenv(ENV_CSRF_TOKEN, "") or stored.get("csrf_token") or "").strip(),
        user_id=str(os.getenv(ENV_USER_ID, "") or stored.get("user_id") or "").strip(),
        account_id=str(os.getenv(ENV_ACCOUNT_ID, "") or stored.get("account_id") or "").strip(),
    )


def save_runtime_login_state(state_path: Path, state: RuntimeLoginState) -> None:
    """把当前登录态落到本地文件。"""
    payload = {
        "user_agent": state.user_agent,
        "csrf_token": state.csrf_token,
        "user_id": state.user_id,
        "account_id": state.account_id,
        "cookies": {key: value for key, value in state.cookies.items() if str(value or "").strip()},
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


def _ensure_required_cookies(cookies: dict[str, str]) -> None:
    missing = [key for key in REQUIRED_COOKIE_KEYS if not str(cookies.get(key) or "").strip()]
    if not missing:
        return
    raise RuntimeError(
        "Wiza 缺少可用登录态。"
        f" 至少需要这些 cookie：{', '.join(missing)}。"
        f" 请刷新 {Path('output/wiza/session') / LOGIN_STATE_NAME} 或 .env 里的 {ENV_COOKIE_HEADER}。"
    )


def _build_session(proxy: str, user_agent: str) -> cffi_requests.Session:
    proxies = {"http": proxy, "https": proxy} if proxy else {}
    session = cffi_requests.Session(impersonate="chrome", proxies=proxies)
    session.trust_env = False
    session.headers.update(
        {
            "Accept": "*/*",
            "Accept-Language": "en-GB,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://wiza.co",
            "User-Agent": user_agent or DEFAULT_USER_AGENT,
        }
    )
    return session


def _build_request_headers(state: RuntimeLoginState, referer: str) -> dict[str, str]:
    headers = {"Referer": referer or DEFAULT_REFERER}
    if state.csrf_token:
        headers["x-csrf-token"] = state.csrf_token
    if state.user_id:
        headers["x-user-id"] = state.user_id
    if state.account_id:
        headers["x-account-id"] = state.account_id
    return headers


def _read_state_file(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.warning("Wiza 登录态文件解析失败：%s", state_path)
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_temporary_limit(status: int, message: str) -> bool:
    if status not in {400, 429}:
        return False
    return "usage limit" in str(message or "").lower()


class WizaUsageLimitError(RuntimeError):
    """Wiza usage limit。"""
