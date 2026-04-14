"""DubaiBizDirectory 浏览器 cookie 刷新辅助。"""

from __future__ import annotations

import logging
import os
import socket
import time
from json import dumps
from dataclasses import dataclass
from pathlib import Path

from .profile_lock import cleanup_profile_runtime
from .solver import get_turnstile_hook_script
from .solver import load_2captcha_api_key
from .solver import solve_turnstile_challenge

LOGGER = logging.getLogger("uae.dubaibizdirectory.browser")
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
PERSISTED_COOKIE_KEYS = ("cf_clearance", "cf_chl_rc_ni", "CAKEPHP", "FCCDCF", "FCNEC")
CHALLENGE_MARKERS = (
    "window._cf_chl_opt",
    "Just a moment...",
    "Checking your browser before accessing",
    "Attention Required! | Cloudflare",
)


@dataclass(frozen=True)
class BrowserCookieState:
    cookies: dict[str, str]
    user_agent: str


def fetch_browser_cookie_state(
    *,
    user_data_dir: Path,
    target_url: str,
    proxy_url: str = "",
    timeout_ms: int = 120000,
) -> BrowserCookieState:
    """用真实浏览器刷新可复用 cookie。"""
    user_data_dir = Path(user_data_dir)
    user_data_dir.mkdir(parents=True, exist_ok=True)
    cleanup_profile_runtime(user_data_dir)
    headless = _env_bool("DUBAIBIZDIRECTORY_BROWSER_HEADLESS", False)
    captcha_api_key = load_2captcha_api_key()
    if captcha_api_key:
        try:
            return _fetch_via_playwright(
                user_data_dir=user_data_dir,
                target_url=target_url,
                proxy_url=proxy_url,
                timeout_ms=timeout_ms,
                headless=headless,
                captcha_api_key=captcha_api_key,
            )
        except Exception as solver_error:  # noqa: BLE001
            LOGGER.warning("DubaiBizDirectory 2cc 刷新失败，回退普通浏览器等待：%s", _compact_browser_error(solver_error))
            cleanup_profile_runtime(user_data_dir)
    try:
        return _fetch_via_drissionpage(
            user_data_dir=user_data_dir,
            target_url=target_url,
            proxy_url=proxy_url,
            timeout_ms=timeout_ms,
            headless=headless,
        )
    except Exception as drission_error:  # noqa: BLE001
        LOGGER.warning("DubaiBizDirectory DrissionPage 刷新失败，回退 Playwright：%s", _compact_browser_error(drission_error))
        cleanup_profile_runtime(user_data_dir)
    return _fetch_via_playwright(
        user_data_dir=user_data_dir,
        target_url=target_url,
        proxy_url=proxy_url,
        timeout_ms=timeout_ms,
        headless=headless,
    )

def _fetch_via_drissionpage(
    *,
    user_data_dir: Path,
    target_url: str,
    proxy_url: str,
    timeout_ms: int,
    headless: bool,
) -> BrowserCookieState:
    from DrissionPage import ChromiumOptions
    from DrissionPage import ChromiumPage

    co = ChromiumOptions()
    # 手动指定一个本机空闲端口，兼容当前 DrissionPage 版本，避免误连用户自己的 9222。
    co.set_local_port(_pick_free_local_port())
    co.set_user_data_path(str(user_data_dir))
    co.set_argument("--disable-blink-features=AutomationControlled")
    co.set_argument("--no-sandbox")
    if headless:
        co.headless()
    if proxy_url:
        co.set_proxy(proxy_url)
    page = ChromiumPage(co)
    try:
        page.get(target_url, timeout=max(int(timeout_ms / 1000), 30))
        _wait_until_passed(
            fetch_html=lambda: str(page.html or ""),
            timeout_ms=timeout_ms,
        )
        cookies_result = {
            cookie.get("name", ""): str(cookie.get("value") or "").strip()
            for cookie in page.cookies()
            if str(cookie.get("name") or "").strip() in PERSISTED_COOKIE_KEYS
            and str(cookie.get("value") or "").strip()
        }
        user_agent = str(page.run_js("return navigator.userAgent") or "").strip() or DEFAULT_USER_AGENT
    finally:
        page.quit()
    return _build_browser_state(cookies_result, user_agent, source="DrissionPage")


def _fetch_via_playwright(
    *,
    user_data_dir: Path,
    target_url: str,
    proxy_url: str,
    timeout_ms: int,
    headless: bool,
    captcha_api_key: str = "",
    user_agent_override: str = "",
    allow_solver_ua_retry: bool = True,
) -> BrowserCookieState:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    channel = _browser_channel()
    launch_kwargs: dict[str, object] = {
        "user_data_dir": str(user_data_dir),
        "headless": headless,
    }
    if channel:
        launch_kwargs["channel"] = channel
    if proxy_url:
        launch_kwargs["proxy"] = {"server": proxy_url}
    if user_agent_override:
        launch_kwargs["user_agent"] = user_agent_override
    retry_user_agent = ""
    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(**launch_kwargs)
        try:
            if user_agent_override:
                context.add_init_script(_platform_spoof_script(user_agent_override))
            if captcha_api_key:
                context.add_init_script(get_turnstile_hook_script())
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
            current_page = _pick_live_page(context, page)
            if captcha_api_key and _looks_like_challenge(200, current_page.content()):
                solved_user_agent = solve_turnstile_challenge(
                    page=current_page,
                    api_key=captcha_api_key,
                    timeout_ms=min(timeout_ms, 120000),
                )
            else:
                solved_user_agent = ""
            try:
                _wait_until_passed(fetch_html=lambda: _page_content(context, page), timeout_ms=timeout_ms)
            except RuntimeError:
                current_user_agent = str(_pick_live_page(context, page).evaluate("() => navigator.userAgent") or "").strip()
                if (
                    captcha_api_key
                    and allow_solver_ua_retry
                    and solved_user_agent
                    and solved_user_agent != current_user_agent
                ):
                    retry_user_agent = solved_user_agent
                else:
                    raise
            cookies_result = {
                cookie["name"]: str(cookie.get("value") or "").strip()
                for cookie in context.cookies(target_url)
                if str(cookie.get("name") or "").strip() in PERSISTED_COOKIE_KEYS
                and str(cookie.get("value") or "").strip()
            }
            user_agent = (
                str(solved_user_agent or _pick_live_page(context, page).evaluate("() => navigator.userAgent") or "").strip()
                or DEFAULT_USER_AGENT
            )
        except PlaywrightTimeoutError as exc:
            raise RuntimeError(f"DubaiBizDirectory 浏览器刷新超时：{target_url}") from exc
        finally:
            context.close()
    if retry_user_agent:
        LOGGER.info("DubaiBizDirectory 使用 2cc 返回 UA 重试浏览器刷新：%s", retry_user_agent[:80])
        return _fetch_via_playwright(
            user_data_dir=user_data_dir,
            target_url=target_url,
            proxy_url=proxy_url,
            timeout_ms=timeout_ms,
            headless=headless,
            captcha_api_key=captcha_api_key,
            user_agent_override=retry_user_agent,
            allow_solver_ua_retry=False,
        )
    source = "Playwright+2cc" if captcha_api_key else "Playwright"
    return _build_browser_state(cookies_result, user_agent, source=source)


def _wait_until_passed(*, fetch_html, timeout_ms: int) -> None:
    deadline = time.time() + max(timeout_ms / 1000.0, 30.0)
    while time.time() < deadline:
        html_text = str(fetch_html() or "")
        if not _looks_like_challenge(200, html_text):
            if "div id=\"results\"" in html_text or "Companies in Dubai" in html_text:
                return
        time.sleep(1)
    raise RuntimeError("浏览器未能自动通过 DubaiBizDirectory challenge。")


def _build_browser_state(cookies_result: dict[str, str], user_agent: str, *, source: str) -> BrowserCookieState:
    if "cf_clearance" not in cookies_result:
        raise RuntimeError(f"{source} 刷新后仍未拿到 cf_clearance。")
    LOGGER.info("DubaiBizDirectory %s 刷新 cookie 成功：keys=%s", source, sorted(cookies_result))
    return BrowserCookieState(cookies=cookies_result, user_agent=user_agent)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _pick_free_local_port() -> int:
    """挑一个本机空闲调试端口，避免连接到用户自己的调试浏览器。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _browser_channel() -> str | None:
    raw = os.getenv("DUBAIBIZDIRECTORY_BROWSER_CHANNEL")
    if raw is None:
        return "chrome"
    cleaned = str(raw).strip()
    return cleaned or None


def _platform_spoof_script(user_agent: str) -> str:
    platform = "Win32" if "windows nt" in user_agent.lower() else "MacIntel"
    ua_platform = "Windows" if platform == "Win32" else "macOS"
    return f"""
(() => {{
  const platformValue = {dumps(platform)};
  const uaPlatformValue = {dumps(ua_platform)};
  Object.defineProperty(navigator, 'platform', {{ configurable: true, get: () => platformValue }});
  if (navigator.userAgentData) {{
    try {{
      Object.defineProperty(navigator.userAgentData, 'platform', {{ configurable: true, get: () => uaPlatformValue }});
    }} catch (e) {{}}
    const original = navigator.userAgentData.getHighEntropyValues?.bind(navigator.userAgentData);
    if (original) {{
      navigator.userAgentData.getHighEntropyValues = async (hints) => {{
        const data = await original(hints);
        return {{ ...data, platform: uaPlatformValue }};
      }};
    }}
  }}
}})();
"""


def _pick_live_page(context, fallback_page):
    if not fallback_page.is_closed():
        return fallback_page
    for candidate in reversed(context.pages):
        if not candidate.is_closed():
            return candidate
    raise RuntimeError("浏览器上下文里没有存活页面。")


def _page_content(context, fallback_page) -> str:
    return str(_pick_live_page(context, fallback_page).content() or "")


def _looks_like_challenge(status_code: int, html_text: str) -> bool:
    text = str(html_text or "")
    if int(status_code or 0) == 403:
        return True
    return any(marker in text for marker in CHALLENGE_MARKERS)


def _compact_browser_error(error: Exception) -> str:
    text = str(error or "").strip()
    if not text:
        return error.__class__.__name__
    head = text.splitlines()[0].strip()
    return head or error.__class__.__name__
