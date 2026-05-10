"""Blurpath 浏览器登录态提供器。"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import urllib.request
from dataclasses import dataclass
from typing import Any


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class BlurpathAuthBundle:
    token: str
    username: str
    password: str
    white_proxies: list[str]


class BlurpathBrowserProvider:
    """从 9222 浏览器里提取 Blurpath auth token，再调用后台 API。"""

    def __init__(self, cdp_url: str) -> None:
        self._cdp_url = cdp_url

    def fetch_bundle(self) -> BlurpathAuthBundle:
        token = self._fetch_auth_token()
        account = self._fetch_first_account(token)
        white_proxies = self._fetch_white_proxies(token)
        return BlurpathAuthBundle(
            token=token,
            username=str(account.get("username") or "").strip(),
            password=str(account.get("password1") or "").strip(),
            white_proxies=white_proxies,
        )

    def _fetch_auth_token(self) -> str:
        page_ws_url = self._blurpath_page_ws_url()
        payload = self._runtime_eval(
            page_ws_url,
            'JSON.stringify({authToken: localStorage.getItem("authToken") || ""})',
        )
        raw = str(payload.get("authToken") or "").strip()
        if not raw:
            raise RuntimeError("Blurpath 页面 localStorage 里没有 authToken。")
        token_payload = json.loads(raw)
        token = str(token_payload.get("token") or "").strip()
        if not token:
            raise RuntimeError("Blurpath authToken 解析后为空。")
        return token

    def _fetch_first_account(self, token: str) -> dict[str, Any]:
        response = _blurpath_api_json(
            token,
            "https://dashboard.blurpath.com/api/api/study/ip/account/list",
            data={"keyword": "", "page": 1, "pageSize": 10, "status": "", "date": []},
        )
        rows = list(((response.get("data") or {}).get("list") or []))
        if not rows:
            raise RuntimeError("Blurpath 账号列表为空。")
        return dict(rows[0])

    def _fetch_white_proxies(self, token: str) -> list[str]:
        response = _blurpath_api_json(
            token,
            "https://dashboard.blurpath.com/api/api/study/ip/white/proxys?num=20&goodsType=3&protocol=http",
        )
        values = list(response.get("data") or [])
        return [str(item or "").strip() for item in values if str(item or "").strip()]

    def _blurpath_page_ws_url(self) -> str:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        raw = opener.open(f"{self._cdp_url}/json/list", timeout=5).read().decode()
        pages = json.loads(raw)
        for item in pages:
            url = str(item.get("url") or "")
            if url.startswith("https://dashboard.blurpath.com/"):
                return str(item["webSocketDebuggerUrl"])
        raise RuntimeError("9222 浏览器里没有打开 Blurpath 页面。")

    def _runtime_eval(self, page_ws_url: str, expression: str) -> dict[str, Any]:
        script = """
const ws = new WebSocket(process.env.CDP_PAGE_WS_URL);
ws.addEventListener('open', () => {
  ws.send(JSON.stringify({id: 1, method: 'Runtime.evaluate', params: {expression: process.env.CDP_EXPRESSION, returnByValue: true}}));
});
ws.addEventListener('message', (event) => {
  const payload = JSON.parse(event.data);
  if (payload.id !== 1) return;
  process.stdout.write(String(payload.result?.result?.value || '{}'));
  ws.close();
});
ws.addEventListener('error', (event) => {
  process.stderr.write(String((event && event.message) || 'node websocket error'));
  process.exit(1);
});
"""
        result = subprocess.run(  # noqa: S603
            ["node", "-e", script],
            capture_output=True,
            text=True,
            timeout=10,
            env={
                **os.environ,
                "CDP_PAGE_WS_URL": page_ws_url,
                "CDP_EXPRESSION": expression,
            },
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "Blurpath Runtime.evaluate failed")
        return json.loads(result.stdout or "{}")


def _blurpath_api_json(token: str, url: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Authorization": f"token {token}",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en_US",
        },
        method="POST" if data is not None else "GET",
        data=(json.dumps(data).encode("utf-8") if data is not None else None),
    )
    if data is not None:
        req.add_header("Content-Type", "application/json")
    text = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
    payload = json.loads(text)
    if int(payload.get("ret") or 0) != 0:
        raise RuntimeError(f"Blurpath API ret != 0: {payload}")
    return payload
