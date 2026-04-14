"""DubaiBizDirectory 的 turnstile 解题辅助。"""

from __future__ import annotations

import logging
import os
import time

import httpx


LOGGER = logging.getLogger("uae.dubaibizdirectory.solver")
_TWO_CAPTCHA_CREATE_URL = "https://api.2captcha.com/createTask"
_TWO_CAPTCHA_RESULT_URL = "https://api.2captcha.com/getTaskResult"
_HOOK_SCRIPT = """
(() => {
  if (window.__oldironTurnstileHookInstalled) return;
  window.__oldironTurnstileHookInstalled = true;
  const install = () => {
    if (!window.turnstile || window.__oldironTurnstileHooked) return;
    const originalRender = window.turnstile.render;
    window.turnstile.render = (container, params = {}) => {
      window.__oldironTurnstilePayload = {
        sitekey: params.sitekey || '',
        action: params.action || '',
        cData: params.cData || '',
        chlPageData: params.chlPageData || '',
        userAgent: navigator.userAgent || ''
      };
      window.__oldironTurnstileCallback = typeof params.callback === 'function' ? params.callback : null;
      return 'foo';
    };
    window.__oldironTurnstileHooked = true;
  };
  install();
  const timer = setInterval(install, 50);
  setTimeout(() => clearInterval(timer), 30000);
})();
"""


def load_2captcha_api_key() -> str:
    """读取 2cc key。"""
    return str(os.getenv("TWOCAPTCHA_API_KEY", "") or os.getenv("CAPTCHA_API_KEY", "")).strip()


def get_turnstile_hook_script() -> str:
    """返回 turnstile hook 脚本。"""
    return _HOOK_SCRIPT


def solve_turnstile_challenge(*, page, api_key: str, timeout_ms: int) -> str:
    """在页面内等待参数、调用 2cc，并把 token 回填到回调。"""
    payload = _wait_turnstile_payload(page, timeout_ms=min(timeout_ms, 30000))
    LOGGER.info(
        "DubaiBizDirectory 已捕获 turnstile 参数：sitekey=%s action=%s has_data=%s has_pagedata=%s",
        payload["sitekey"][:10],
        payload.get("action", ""),
        bool(payload.get("cData", "")),
        bool(payload.get("chlPageData", "")),
    )
    solution = _solve_turnstile_with_2captcha(
        api_key=api_key,
        page_url=str(page.url or ""),
        sitekey=payload["sitekey"],
        action=payload.get("action", ""),
        data=payload.get("cData", ""),
        pagedata=payload.get("chlPageData", ""),
        user_agent=payload.get("userAgent", ""),
    )
    _apply_turnstile_token(page, solution["token"])
    LOGGER.info("DubaiBizDirectory 已回填 2cc token，返回 UA=%s", str(solution.get("userAgent") or "")[:80])
    return str(solution.get("userAgent") or payload.get("userAgent") or "").strip()


def _wait_turnstile_payload(page, timeout_ms: int) -> dict[str, str]:
    deadline = time.time() + max(timeout_ms / 1000.0, 10.0)
    while time.time() < deadline:
        payload = page.evaluate("() => window.__oldironTurnstilePayload || null")
        if isinstance(payload, dict) and str(payload.get("sitekey") or "").strip():
            return {str(key): str(value or "").strip() for key, value in payload.items()}
        page.wait_for_timeout(500)
    raise RuntimeError("页面里没有拿到 turnstile 参数。")


def _solve_turnstile_with_2captcha(
    *,
    api_key: str,
    page_url: str,
    sitekey: str,
    action: str,
    data: str,
    pagedata: str,
    user_agent: str,
) -> dict[str, str]:
    client_kwargs: dict[str, object] = {"timeout": 30, "follow_redirects": True}
    proxy_url = str(os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "").strip()
    if proxy_url:
        client_kwargs["proxy"] = proxy_url
    task: dict[str, object] = {
        "type": "TurnstileTaskProxyless",
        "websiteURL": page_url,
        "websiteKey": sitekey,
    }
    if action:
        task["action"] = action
    if data:
        task["data"] = data
    if pagedata:
        task["pagedata"] = pagedata
    if user_agent:
        task["userAgent"] = user_agent
    payload = {"clientKey": api_key, "task": task}
    with httpx.Client(**client_kwargs) as client:
        create_resp = client.post(_TWO_CAPTCHA_CREATE_URL, json=payload)
        create_resp.raise_for_status()
        create_data = create_resp.json()
        if int(create_data.get("errorId", 1)) != 0:
            raise RuntimeError(f"2cc createTask 失败: {create_data}")
        task_id = create_data.get("taskId")
        if not task_id:
            raise RuntimeError("2cc 未返回 taskId")
        for _ in range(40):
            time.sleep(3)
            result_resp = client.post(
                _TWO_CAPTCHA_RESULT_URL,
                json={"clientKey": api_key, "taskId": task_id},
            )
            result_resp.raise_for_status()
            result_data = result_resp.json()
            if int(result_data.get("errorId", 1)) != 0:
                raise RuntimeError(f"2cc getTaskResult 失败: {result_data}")
            if result_data.get("status") == "processing":
                continue
            solution = result_data.get("solution") or {}
            token = str(solution.get("token") or "").strip()
            if not token:
                raise RuntimeError(f"2cc 返回空 token: {result_data}")
            return {
                "token": token,
                "userAgent": str(solution.get("userAgent") or user_agent or "").strip(),
            }
    raise RuntimeError("2cc 超时未返回 turnstile 结果")


def _apply_turnstile_token(page, token: str) -> None:
    page.evaluate(
        """(captchaToken) => {
            const input = document.querySelector('input[name="cf-turnstile-response"]');
            if (input) {
              input.value = captchaToken;
              input.dispatchEvent(new Event('input', { bubbles: true }));
              input.dispatchEvent(new Event('change', { bubbles: true }));
            }
            const textarea = document.querySelector('textarea[name="g-recaptcha-response"]');
            if (textarea) {
              textarea.value = captchaToken;
              textarea.dispatchEvent(new Event('input', { bubbles: true }));
              textarea.dispatchEvent(new Event('change', { bubbles: true }));
            }
            const callback = window.__oldironTurnstileCallback;
            if (typeof callback === 'function') {
              callback(captchaToken);
            }
          }""",
        token,
    )
