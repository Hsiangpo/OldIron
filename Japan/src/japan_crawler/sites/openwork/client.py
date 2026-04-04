"""OpenWork HTTP 客户端。"""

from __future__ import annotations

import base64
import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx
from curl_cffi.requests import Session
from lxml import html

from .browser_profile import (
    DETAIL_READY_SELECTOR,
    LIST_READY_SELECTOR,
    OpenworkBrowserBlocked,
    OpenworkPersistentBrowser,
)


LOGGER = logging.getLogger("openwork.client")
BASE_URL = "https://www.openwork.jp"
LIST_PATH = "/company_list"
DEFAULT_PER_PAGE = 50
_PROXY_FALLBACK_ERROR_HINTS = (
    "curl: (35)",
    "tls connect error",
    "invalid library",
    "curl: (7)",
    "failed to connect to 127.0.0.1",
)
_PROXY_FALLBACK_RESPONSE_HINTS = (
    "awswaf",
    "challenge.js",
    "verify that you're not a robot",
    "javascript is disabled",
    "画像認証",
    "<title>画像認証",
    "openwork,画像認証",
    "g-recaptcha",
)
_TWO_CAPTCHA_CREATE_URL = "https://api.2captcha.com/createTask"
_TWO_CAPTCHA_RESULT_URL = "https://api.2captcha.com/getTaskResult"


@dataclass(slots=True)
class _HtmlResponse:
    """统一协议页与浏览器页的返回形态。"""

    status_code: int
    text: str


@dataclass(slots=True)
class _CaptchaChallenge:
    """OpenWork 图片验证码表单。"""

    submit_url: str
    image_url: str
    answer_field: str
    form_fields: dict[str, str]


class OpenworkClient:
    """OpenWork 列表页与公司详情页抓取客户端。"""

    def __init__(
        self,
        *,
        request_delay: float = 1.2,
        max_retries: int = 3,
        proxy: str | None = None,
        browser_profile_dir: str | Path | None = None,
    ) -> None:
        self._delay = request_delay
        self._max_retries = max_retries
        self._proxy_url = str(proxy or os.getenv("HTTP_PROXY", "")).strip()
        self._local = threading.local()
        self._proxy_cooldown_until = 0.0
        self._browser_lock = threading.Lock()
        self._browser_mode = os.getenv("OPENWORK_FORCE_BROWSER", "").strip() == "1"
        self._browser_notice_logged = False
        self._captcha_api_key = str(
            os.getenv("TWOCAPTCHA_API_KEY", "") or os.getenv("CAPTCHA_API_KEY", "")
        ).strip()
        self._captcha_notice_logged = False
        self._browser_client = None
        if browser_profile_dir is not None:
            self._browser_client = OpenworkPersistentBrowser(
                user_data_dir=Path(browser_profile_dir),
                proxy_url=self._proxy_url,
                headless_default=True,
            )
        if self._proxy_url:
            LOGGER.info("使用代理: %s", self._proxy_url)
        self._base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": f"{BASE_URL}{LIST_PATH}",
        }
        self._request_count = 0
        self._error_count = 0

    def fetch_list_page(self, page: int = 1) -> str | None:
        """抓取公司列表页。"""
        params = {
            "field": "",
            "pref": "",
            "src_str": "",
            "sort": "1",
        }
        if page > 1:
            params["next_page"] = str(page)
        response = self._get_with_retry(f"{BASE_URL}{LIST_PATH}", params=params)
        return response.text if response is not None else None

    def fetch_detail_page(self, detail_url: str) -> str | None:
        """抓取公司详情页。"""
        absolute = urljoin(BASE_URL, detail_url)
        response = self._get_with_retry(absolute)
        return response.text if response is not None else None

    def _get_with_retry(self, url: str, params: dict[str, str] | None = None) -> Any:
        if self._browser_mode:
            browser_response = self._browser_response(url)
            if browser_response is not None:
                return browser_response
            self._browser_mode = False
            return None
        for attempt in range(self._max_retries):
            for use_proxy in self._request_modes():
                try:
                    self._polite_delay()
                    response = self._session(use_proxy).get(url, params=params, timeout=30)
                    self._request_count += 1
                    if self._should_fallback_direct_from_response(response):
                        solved = self._solve_captcha_if_possible(
                            session=self._session(use_proxy),
                            url=url,
                            response=response,
                        )
                        if solved is not None:
                            return solved
                        if use_proxy:
                            self._disable_proxy_temporarily(f"挑战页: {url}")
                            continue
                        LOGGER.warning("OpenWork 验证码页未解开，保留断点等待换 IP 后重试：%s", url)
                        return None
                    if response.status_code == 200:
                        return response
                    if response.status_code == 429:
                        self._sleep_backoff(attempt, 4.0, 8.0, "429 限流")
                        break
                    if response.status_code == 403:
                        self._error_count += 1
                        if use_proxy:
                            LOGGER.warning("OpenWork 403，当前代理/IP 可能被拦，先停当前页等待换 IP：%s", url)
                            self._disable_proxy_temporarily(f"403: {url}")
                            continue
                        LOGGER.warning("OpenWork 403，当前直连/IP 也被拦，保留断点等待换 IP：%s", url)
                        return None
                    if response.status_code >= 500:
                        self._sleep_backoff(attempt, 2.0, 5.0, f"{response.status_code} 服务端错误")
                        break
                    LOGGER.warning("HTTP %d: %s", response.status_code, url)
                    return None
                except Exception as exc:  # noqa: BLE001
                    self._error_count += 1
                    if use_proxy and self._should_fallback_direct_from_exception(exc):
                        self._disable_proxy_temporarily(f"代理异常: {url}")
                        continue
                    LOGGER.warning("请求异常: %s", exc)
                    self._sleep_backoff(attempt, 2.0, 4.0, "网络异常重试")
                    break
        LOGGER.error("重试耗尽: %s", url)
        return None

    def _browser_response(self, url: str) -> _HtmlResponse | None:
        if self._browser_client is None:
            raise RuntimeError("OpenWork 浏览器 profile 未配置。")
        with self._browser_lock:
            self._browser_mode = True
            if not self._browser_notice_logged:
                LOGGER.info("OpenWork 已切换到浏览器详情补抓模式，后续详情页会逐条处理，速度会慢一些。")
                self._browser_notice_logged = True
            try:
                html_text = self._browser_client.fetch_html(url=url, ready_selector=self._ready_selector(url))
            except OpenworkBrowserBlocked as exc:
                LOGGER.warning("OpenWork 浏览器补抓失败，回退为空详情：%s | %s", url, exc)
                return None
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("OpenWork 浏览器补抓失败，回退为空详情：%s | %s", url, exc)
                return None
            self._request_count += 1
            return _HtmlResponse(status_code=200, text=html_text)

    def _ready_selector(self, url: str) -> str:
        if "/company_list" in url:
            return LIST_READY_SELECTOR
        return DETAIL_READY_SELECTOR

    def _request_modes(self) -> list[bool]:
        if not self._proxy_url:
            return [False]
        if time.time() < self._proxy_cooldown_until:
            return [False]
        return [True, False]

    def _session(self, use_proxy: bool) -> Session:
        attr = "proxy_session" if use_proxy else "direct_session"
        session = getattr(self._local, attr, None)
        if session is None:
            session = Session(impersonate="chrome120")
            if use_proxy and self._proxy_url:
                session.proxies = {"http": self._proxy_url, "https": self._proxy_url}
            session.headers.update(self._base_headers)
            setattr(self._local, attr, session)
        return session

    def _disable_proxy_temporarily(self, reason: str) -> None:
        if not self._proxy_url:
            return
        self._proxy_cooldown_until = max(self._proxy_cooldown_until, time.time() + 120)
        session = getattr(self._local, "proxy_session", None)
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass
            delattr(self._local, "proxy_session")

    def _should_fallback_direct_from_exception(self, exc: Exception) -> bool:
        text = str(exc or "").lower()
        return any(hint in text for hint in _PROXY_FALLBACK_ERROR_HINTS)

    def _should_fallback_direct_from_response(self, response: Any) -> bool:
        status_code = int(getattr(response, "status_code", 0) or 0)
        if status_code not in {200, 202, 403}:
            return False
        text = str(getattr(response, "text", "") or "").lower()
        return any(hint in text for hint in _PROXY_FALLBACK_RESPONSE_HINTS)

    def _polite_delay(self) -> None:
        time.sleep(self._delay + random.uniform(0.2, 0.6))

    def _sleep_backoff(self, attempt: int, base: float, jitter: float, label: str) -> None:
        wait = base * (attempt + 1) + random.uniform(0.5, jitter)
        LOGGER.warning("%s，等待 %.1fs", label, wait)
        time.sleep(wait)

    def _solve_captcha_if_possible(
        self,
        *,
        session: Session,
        url: str,
        response: Any,
    ) -> _HtmlResponse | None:
        challenge = self._extract_captcha_challenge(url=url, response=response)
        if challenge is None:
            return None
        if not self._captcha_api_key:
            self._log_missing_2captcha_once()
            return None
        LOGGER.info("OpenWork 检测到图片验证码，开始走 2cc 自动识别：%s", url)
        try:
            image_bytes = self._fetch_captcha_image(session=session, challenge=challenge)
            answer = self._solve_image_with_2captcha(image_bytes)
            solved = self._submit_captcha_answer(session=session, challenge=challenge, answer=answer)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("OpenWork 2cc 自动识别失败，回退浏览器方案：%s", exc)
            return None
        if self._should_fallback_direct_from_response(solved):
            LOGGER.warning("OpenWork 2cc 已提交但站点仍返回验证码，回退浏览器方案。")
            return None
        LOGGER.info("OpenWork 2cc 自动识别成功，继续协议抓取。")
        return solved

    def _extract_captcha_challenge(self, *, url: str, response: Any) -> _CaptchaChallenge | None:
        page_html = str(getattr(response, "text", "") or "")
        if not page_html:
            return None
        if "captcha[captcha]" not in page_html and "generate-captcha" not in page_html:
            return None
        tree = html.fromstring(page_html)
        form = tree.cssselect("form")
        if not form:
            return None
        form_node = form[0]
        submit_url = urljoin(url, str(form_node.get("action", "") or "").strip()) or url
        fields: dict[str, str] = {}
        answer_field = ""
        for input_node in form_node.cssselect("input"):
            name = str(input_node.get("name", "") or "").strip()
            if not name:
                continue
            input_type = str(input_node.get("type", "") or "").strip().lower()
            value = str(input_node.get("value", "") or "")
            fields[name] = value
            if input_type == "text":
                answer_field = name
        image_node = tree.cssselect('img[src*="generate-captcha"], img[src*="captcha"]')
        if not image_node or not answer_field:
            return None
        image_url = urljoin(url, str(image_node[0].get("src", "") or "").strip())
        return _CaptchaChallenge(
            submit_url=submit_url,
            image_url=image_url,
            answer_field=answer_field,
            form_fields=fields,
        )

    def _fetch_captcha_image(self, *, session: Session, challenge: _CaptchaChallenge) -> bytes:
        response = session.get(challenge.image_url, timeout=30)
        content = bytes(getattr(response, "content", b"") or b"")
        if not content:
            raise RuntimeError("验证码图片下载为空")
        return content

    def _solve_image_with_2captcha(self, image_bytes: bytes) -> str:
        proxy_url = str(os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "").strip()
        client_kwargs: dict[str, Any] = {"timeout": 30, "follow_redirects": True}
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        payload = {
            "clientKey": self._captcha_api_key,
            "task": {
                "type": "ImageToTextTask",
                "body": base64.b64encode(image_bytes).decode("ascii"),
                "case": True,
                "numeric": 4,
                "minLength": 6,
                "maxLength": 6,
                "comment": "enter the exact 6-character captcha from the image",
            },
            "languagePool": "en",
        }
        with httpx.Client(**client_kwargs) as client:
            create_resp = client.post(_TWO_CAPTCHA_CREATE_URL, json=payload)
            create_resp.raise_for_status()
            create_data = create_resp.json()
            if int(create_data.get("errorId", 1)) != 0:
                raise RuntimeError(f"2cc createTask 失败: {create_data}")
            task_id = create_data.get("taskId")
            if not task_id:
                raise RuntimeError("2cc 未返回 taskId")
            for _ in range(25):
                time.sleep(3)
                result_resp = client.post(
                    _TWO_CAPTCHA_RESULT_URL,
                    json={"clientKey": self._captcha_api_key, "taskId": task_id},
                )
                result_resp.raise_for_status()
                result_data = result_resp.json()
                if int(result_data.get("errorId", 1)) != 0:
                    raise RuntimeError(f"2cc getTaskResult 失败: {result_data}")
                if result_data.get("status") == "processing":
                    continue
                answer = str((result_data.get("solution") or {}).get("text") or "").strip()
                if answer:
                    return answer
                raise RuntimeError(f"2cc 返回空答案: {result_data}")
        raise RuntimeError("2cc 超时未返回结果")

    def _submit_captcha_answer(
        self,
        *,
        session: Session,
        challenge: _CaptchaChallenge,
        answer: str,
    ) -> _HtmlResponse:
        form_data = dict(challenge.form_fields)
        form_data[challenge.answer_field] = answer
        response = session.post(challenge.submit_url, data=form_data, timeout=30, allow_redirects=True)
        return _HtmlResponse(
            status_code=int(getattr(response, "status_code", 0) or 0),
            text=str(getattr(response, "text", "") or ""),
        )

    def _log_missing_2captcha_once(self) -> None:
        if self._captcha_notice_logged:
            return
        LOGGER.info("OpenWork 未配置 2cc key，遇到验证码页时将直接保留断点，等待换 IP 或补 key。")
        self._captcha_notice_logged = True

    @property
    def stats(self) -> dict[str, int]:
        return {"requests": self._request_count, "errors": self._error_count}

    @property
    def browser_primary(self) -> bool:
        return self._browser_client is not None and self._browser_mode

    @property
    def browser_enabled(self) -> bool:
        return self._browser_client is not None
