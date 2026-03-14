"""BusinessList 运行期 Cookie 同步工具。"""

from __future__ import annotations

import re
import time
from pathlib import Path

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

DEFAULT_TARGET_URL = "https://www.businesslist.my/company/381000/360digitmg"
DEFAULT_LOGIN_PROBE_COMPANY_ID = 62731
_TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", flags=re.I | re.S)
_LOGIN_REQUIRED_TITLE_TOKENS = ("members sign in", "sign in")


def _pick_businesslist_cookies(raw_items: list[dict[str, object]]) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for item in raw_items:
        name = str(item.get("name", "")).strip()
        value = str(item.get("value", "")).strip()
        domain = str(item.get("domain", "")).strip().lower()
        if not name or not value:
            continue
        if "businesslist.my" not in domain:
            continue
        cookies[name] = value
    return cookies


def _build_runtime_cookie_header(cookies: dict[str, str]) -> str:
    cf_clearance = cookies.get("cf_clearance", "").strip()
    cakephp = cookies.get("CAKEPHP", "").strip()
    if not cf_clearance or not cakephp:
        return ""
    # 中文注释：写入完整站点 Cookie，避免 cf 校验依赖的其他字段丢失。
    parts = [f"{name}={value}" for name, value in cookies.items() if name and value]
    return "; ".join(parts)


def _extract_html_title(html: str) -> str:
    matched = _TITLE_PATTERN.search(html)
    if matched is None:
        return ""
    return " ".join(matched.group(1).split()).strip()


def probe_businesslist_login_status(
    cookie_header: str,
    *,
    login_probe_company_id: int = DEFAULT_LOGIN_PROBE_COMPANY_ID,
    timeout: float = 15.0,
    user_agent: str = "",
) -> tuple[bool, str, str]:
    header = cookie_header.strip()
    if not header:
        return False, "cookie_empty", ""

    company_id = max(int(login_probe_company_id), 1)
    probe_url = f"https://www.businesslist.my/sign-in/email:{company_id}"
    session = requests.Session()
    session.headers.update(
        {
            "Cookie": header,
            "User-Agent": user_agent.strip() or "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    try:
        response = session.get(probe_url, timeout=max(float(timeout), 1.0), allow_redirects=True)
    except Exception as exc:  # noqa: BLE001
        return False, f"probe_request_failed:{type(exc).__name__}", probe_url
    finally:
        session.close()

    final_url = str(response.url)
    final_url_lower = final_url.lower()
    title = _extract_html_title(response.text).lower()
    body_lower = response.text.lower()
    if any(token in title for token in _LOGIN_REQUIRED_TITLE_TOKENS) and "business" in title:
        return False, "login_page_title", final_url
    if "signup-user" in final_url_lower:
        return False, "redirect_signup", final_url
    if "sign-in/email:" in final_url_lower and "sign in" in body_lower:
        return False, "redirect_sign_in_email", final_url
    return True, "ok", final_url


def sync_cookie_from_cdp(
    *,
    cdp_url: str,
    output_file: str,
    target_url: str = DEFAULT_TARGET_URL,
    wait_seconds: int = 600,
    poll_seconds: float = 2.0,
    require_login: bool = False,
    login_probe_company_id: int = DEFAULT_LOGIN_PROBE_COMPANY_ID,
) -> dict[str, str]:
    if wait_seconds <= 0:
        raise ValueError("wait_seconds 必须大于 0。")
    if poll_seconds <= 0:
        raise ValueError("poll_seconds 必须大于 0。")

    debugger_address = cdp_url.replace("http://", "").replace("https://", "").strip()
    options = Options()
    options.add_experimental_option("debuggerAddress", debugger_address)
    try:
        driver = webdriver.Chrome(options=options)
    except Exception as exc:  # noqa: BLE001
        # 中文注释：优先给出可执行的排障信息，避免直接暴露冗长底层异常。
        raise RuntimeError(
            f"无法连接调试浏览器 {cdp_url}，请先用 9222 启动 Chrome 并保持打开。"
        ) from exc
    cookie_path = Path(output_file)
    cookie_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if "businesslist.my" not in driver.current_url.lower():
            driver.get(target_url)

        deadline = time.monotonic() + wait_seconds
        round_no = 0
        while time.monotonic() < deadline:
            round_no += 1
            cookie_items = driver.get_cookies()
            cookie_map = _pick_businesslist_cookies(cookie_items)
            cookie_header = _build_runtime_cookie_header(cookie_map)
            if cookie_header:
                login_reason = "skip"
                login_probe_url = ""
                if require_login:
                    login_ok, login_reason, login_probe_url = probe_businesslist_login_status(
                        cookie_header,
                        login_probe_company_id=login_probe_company_id,
                    )
                    if not login_ok:
                        if round_no == 1 or round_no % 5 == 0:
                            print(
                                "[Cookie同步] 已通过 cf，但当前 Cookie 未检测到登录态。"
                                f"原因={login_reason} url={login_probe_url or '-'}，"
                                "请先在浏览器完成 BusinessList 登录。"
                            )
                        time.sleep(poll_seconds)
                        continue
                cookie_path.write_text(cookie_header, encoding="utf-8")
                return {
                    "cookie_file": str(cookie_path),
                    "cf_clearance": cookie_map.get("cf_clearance", ""),
                    "cakephp": cookie_map.get("CAKEPHP", ""),
                    "login_verified": "1" if require_login else "0",
                    "login_probe_reason": login_reason,
                    "login_probe_url": login_probe_url,
                }
            if round_no == 1 or round_no % 5 == 0:
                print("[Cookie同步] 等待你在 9222 浏览器通过 cf 验证...")
            time.sleep(poll_seconds)
        if require_login:
            raise TimeoutError("等待超时：未检测到“cf_clearance + CAKEPHP + 登录态”。")
        raise TimeoutError("等待超时：未检测到 cf_clearance + CAKEPHP。")
    finally:
        driver.quit()
