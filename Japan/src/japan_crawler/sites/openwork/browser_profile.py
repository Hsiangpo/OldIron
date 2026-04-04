"""OpenWork 浏览器 profile 复用辅助。"""

from __future__ import annotations

import atexit
import base64
import logging
import os
import re
import threading
import time
from pathlib import Path
from urllib.parse import urljoin

import httpx
from playwright.sync_api import Error, TimeoutError, sync_playwright


LOGGER = logging.getLogger("openwork.browser")
LIST_URL = "https://www.openwork.jp/company_list?field=&pref=&sort=1&src_str="
LIST_READY_SELECTOR = "ul.testCompanyList > li"
DETAIL_READY_SELECTOR = "table.definitionList-wiki tr"
_AUTH_TEXT = "画像認証"
_DEFAULT_TIMEOUT_MS = 90000
_DEFAULT_MANUAL_WAIT_SECONDS = 600
_DETAIL_PATH_RE = re.compile(r'href="(/company\.php\?m_id=[^"]+)"')
_TWO_CAPTCHA_CREATE_URL = "https://api.2captcha.com/createTask"
_TWO_CAPTCHA_RESULT_URL = "https://api.2captcha.com/getTaskResult"


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


class OpenworkBrowserBlocked(RuntimeError):
    """OpenWork 浏览器 profile 仍被验证码拦截时抛出。"""


class OpenworkPersistentBrowser:
    """复用固定浏览器 profile 抓取 OpenWork 页面。"""

    def __init__(
        self,
        *,
        user_data_dir: Path,
        proxy_url: str = "",
        timeout_ms: int | None = None,
        manual_wait_seconds: int | None = None,
        headless_default: bool | None = None,
    ) -> None:
        self._user_data_dir = Path(user_data_dir)
        self._proxy_url = str(proxy_url or "").strip()
        self._timeout_ms = int(timeout_ms or _DEFAULT_TIMEOUT_MS)
        self._manual_wait_seconds = int(manual_wait_seconds or _DEFAULT_MANUAL_WAIT_SECONDS)
        self._channel = str(os.getenv("OPENWORK_BROWSER_CHANNEL") or "chrome").strip()
        self._headless = headless_default if headless_default is not None else _env_bool("OPENWORK_BROWSER_HEADLESS", False)
        self._captcha_api_key = str(
            os.getenv("TWOCAPTCHA_API_KEY", "") or os.getenv("CAPTCHA_API_KEY", "")
        ).strip()
        self._lock = threading.Lock()
        self._playwright = None
        self._context = None
        self._page = None
        self._started_headless: bool | None = None
        atexit.register(self.close)

    def prepare_manual_auth(self, target_url: str = LIST_URL, ready_selector: str = LIST_READY_SELECTOR) -> None:
        """打开可见浏览器，让人工完成列表页和详情页验证码。"""
        with self._lock:
            self._start(headless=False)
            page = self._page_handle()
            page.goto(target_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            self._ensure_page_ready(
                page,
                ready_selector=ready_selector,
                target_url=target_url,
                stage_label="列表页",
            )
            detail_url = self._extract_first_detail_url(page.content())
            if not detail_url:
                self._close_no_lock()
                raise OpenworkBrowserBlocked("OpenWork 列表页已打开，但未找到可用的公司详情链接。")
            page.goto(detail_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            self._ensure_page_ready(
                page,
                ready_selector=DETAIL_READY_SELECTOR,
                target_url=detail_url,
                stage_label="详情页",
            )
            LOGGER.info("OpenWork 浏览器 profile 已通过列表页和详情页验证：%s", self._user_data_dir)
            self._close_no_lock()

    def _ensure_page_ready(self, page, *, ready_selector: str, target_url: str, stage_label: str) -> None:
        if self._wait_ready(page, ready_selector) and self._looks_like_real_page(page.content()):
            return
        if self._try_auto_solve_captcha(page, target_url, ready_selector):
            return
        page.bring_to_front()
        LOGGER.warning(
            "OpenWork %s 当前需要人工完成图片验证码。请在打开的 Chrome 窗口中完成验证；最多等待 %d 秒。",
            stage_label,
            self._manual_wait_seconds,
        )
        deadline = time.time() + self._manual_wait_seconds
        while time.time() < deadline:
            if self._wait_ready(page, ready_selector) and self._looks_like_real_page(page.content()):
                return
            page.wait_for_timeout(2000)
        raise OpenworkBrowserBlocked(self._build_blocker_message(target_url))

    def fetch_html(self, *, url: str, ready_selector: str) -> str:
        """使用固定 profile 抓取真实页面 HTML。"""
        with self._lock:
            for _ in range(2):
                self._start(headless=self._headless)
                page = self._page_handle()
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=self._timeout_ms)
                    if self._wait_ready(page, ready_selector) and self._looks_like_real_page(page.content()):
                        return page.content()
                    if self._try_auto_solve_captcha(page, url, ready_selector):
                        return page.content()
                    raise OpenworkBrowserBlocked(self._build_blocker_message(url))
                except Error as exc:
                    if self._is_closed_target_error(exc):
                        LOGGER.warning("OpenWork 浏览器页已关闭，自动重建上下文后重试：%s", url)
                        self._close_no_lock()
                        continue
                    raise
            raise OpenworkBrowserBlocked(f"OpenWork 浏览器上下文反复关闭，无法抓取：{url}")

    def close(self) -> None:
        with self._lock:
            self._close_no_lock()

    def _start(self, *, headless: bool) -> None:
        if self._context is not None and self._started_headless == headless:
            return
        self._close_no_lock()
        self._user_data_dir.mkdir(parents=True, exist_ok=True)
        self._playwright = sync_playwright().start()
        launch_kwargs: dict[str, object] = {
            "user_data_dir": str(self._user_data_dir),
            "headless": headless,
        }
        if self._channel:
            launch_kwargs["channel"] = self._channel
        if self._proxy_url:
            launch_kwargs["proxy"] = {"server": self._proxy_url}
        try:
            self._context = self._playwright.chromium.launch_persistent_context(**launch_kwargs)
        except Error as exc:
            self._handle_launch_error(exc)
        self._started_headless = headless
        self._reset_pages()
        LOGGER.info("OpenWork 启动浏览器 profile：%s | headless=%s", self._user_data_dir, headless)

    def _page_handle(self):
        if self._context is None:
            raise OpenworkBrowserBlocked("OpenWork 浏览器上下文未初始化。")
        if self._page is None or self._page.is_closed():
            self._reset_pages()
        return self._page

    def _reset_pages(self) -> None:
        if self._context is None:
            return
        pages = [page for page in self._context.pages if not page.is_closed()]
        if pages:
            self._page = pages[0]
            for page in pages[1:]:
                try:
                    page.close()
                except Exception:  # noqa: BLE001
                    pass
            return
        self._page = self._context.new_page()

    def _close_no_lock(self) -> None:
        if self._page is not None and not self._page.is_closed():
            self._page.close()
        self._page = None
        if self._context is not None:
            self._context.close()
        self._context = None
        if self._playwright is not None:
            self._playwright.stop()
        self._playwright = None
        self._started_headless = None

    def _wait_ready(self, page, ready_selector: str) -> bool:
        try:
            page.locator(ready_selector).first.wait_for(timeout=5000)
            return True
        except TimeoutError:
            return False
        except Error:
            return False

    def _looks_like_real_page(self, page_html: str) -> bool:
        text = str(page_html or "")
        lowered = text.lower()
        if _AUTH_TEXT in text:
            return False
        if "<title>403 forbidden" in lowered:
            return False
        if "g-recaptcha" in lowered:
            return False
        return True

    def _extract_first_detail_url(self, page_html: str) -> str:
        matched = _DETAIL_PATH_RE.search(str(page_html or ""))
        if matched is None:
            return ""
        return urljoin(LIST_URL, str(matched.group(1) or "").strip())

    def _build_blocker_message(self, url: str) -> str:
        return (
            "OpenWork 当前仍停在验证码/403 页面，无法继续抓取。"
            f" 目标地址: {url}。"
            " 请先执行 `cd Japan && .venv/bin/python run.py openwork auth`，"
            "在打开的 Chrome 窗口里人工完成一次图片验证码，之后再重新运行站点命令。"
        )

    def _handle_launch_error(self, exc: Error) -> None:
        message = str(exc or "")
        if "ProcessSingleton" in message or "profile is already in use" in message:
            raise OpenworkBrowserBlocked(
                "OpenWork 浏览器 profile 当前已被另一个 Chrome/爬虫进程占用。"
                " 如果你已经在跑 `openwork list` 或 `openwork all`，不要重复执行 `openwork auth`，"
                "直接在那个已打开的 Chrome 窗口里处理验证码即可。"
                " 如果当前没有在跑任务，先完全关闭占用该 profile 的 Chrome，再重试 `openwork auth`。"
            ) from exc
        raise

    def _is_closed_target_error(self, exc: Error) -> bool:
        message = str(exc or "").lower()
        return (
            "target page, context or browser has been closed" in message
            or "failed to open a new tab" in message
        )

    def _try_auto_solve_captcha(self, page, target_url: str, ready_selector: str) -> bool:
        if not self._captcha_api_key or not self._is_captcha_page(page):
            return False
        LOGGER.info("OpenWork 浏览器页检测到 yzm，开始走 2cc 自动识别：%s", target_url)
        for _ in range(3):
            try:
                img_src = page.locator('img[src*="generate-captcha"], img[src*="captcha"]').first.get_attribute("src")
                if not img_src:
                    return False
                image_url = urljoin(page.url, img_src)
                response = page.context.request.get(image_url, timeout=self._timeout_ms)
                image_bytes = response.body()
                answer = self._solve_image_with_2captcha(image_bytes).lower()
                page.locator('input[name="captcha[captcha]"]').first.fill(answer)
                page.locator('button[type="submit"], input[type="submit"], button').filter(has_text="送信").first.click(timeout=5000)
                page.wait_for_load_state("domcontentloaded", timeout=self._timeout_ms)
                if self._wait_ready(page, ready_selector) and self._looks_like_real_page(page.content()):
                    LOGGER.info("OpenWork 浏览器页 2cc 自动识别成功：%s", target_url)
                    return True
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("OpenWork 浏览器页 2cc 自动识别失败，准备重试：%s", exc)
        return False

    def _is_captcha_page(self, page) -> bool:
        try:
            return _AUTH_TEXT in str(page.title() or "") or _AUTH_TEXT in page.content()
        except Exception:  # noqa: BLE001
            return False

    def _solve_image_with_2captcha(self, image_bytes: bytes) -> str:
        proxy_url = str(os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "").strip()
        client_kwargs: dict[str, object] = {"timeout": 30, "follow_redirects": True}
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
