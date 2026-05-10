"""CNPJ Biz 浏览器同源 fetch 客户端。"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request


class CnpjBizBrowserFetchClient:
    """通过 CDP 在真实浏览器页面里执行 fetch。"""

    def __init__(self, cdp_url: str) -> None:
        self._cdp_url = cdp_url

    def fetch_text(self, url: str, *, referer: str) -> str:
        payload = self._run_fetch(url, referer=referer, method="GET", body=None)
        return str(payload.get("text") or "")

    def fetch_json(self, url: str, *, referer: str, body: dict) -> dict:
        payload = self._run_fetch(url, referer=referer, method="POST", body=body)
        text = str(payload.get("text") or "")
        return json.loads(text) if text else {}

    def _run_fetch(self, url: str, *, referer: str, method: str, body: dict | None) -> dict:
        page_ws_url = self._target_page_ws_url()
        expression = _build_fetch_expression(url=url, referer=referer, method=method, body=body)
        script = """
const ws = new WebSocket(process.env.CDP_PAGE_WS_URL);
ws.addEventListener('open', () => {
  ws.send(JSON.stringify({id: 1, method: 'Runtime.evaluate', params: {expression: process.env.CDP_EXPRESSION, awaitPromise: true, returnByValue: true}}));
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
            timeout=60,
            env={
                **os.environ,
                "CDP_PAGE_WS_URL": page_ws_url,
                "CDP_EXPRESSION": expression,
            },
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "browser fetch failed")
        payload = json.loads(result.stdout or "{}")
        status = int(payload.get("status") or 0)
        if status >= 400:
            raise RuntimeError(f"browser fetch status={status} url={url}")
        return payload

    def _target_page_ws_url(self) -> str:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        raw = opener.open(f"{self._cdp_url}/json/list", timeout=5).read().decode()
        pages = json.loads(raw)
        for item in pages:
            url = str(item.get("url") or "")
            title = str(item.get("title") or "")
            if url.startswith("https://cnpj.biz/") and ("Lista de empresas" in title or "Consulta de CNPJ" in title or "CNPJ.Biz" in title):
                return str(item["webSocketDebuggerUrl"])
        raise RuntimeError("CDP 浏览器里没有可用的 cnpj.biz 页面。")


def _build_fetch_expression(*, url: str, referer: str, method: str, body: dict | None) -> str:
    body_js = "undefined" if body is None else json.dumps(body, ensure_ascii=False)
    headers = {}
    if body is not None:
        headers["Content-Type"] = "application/json"
        headers["X-Requested-With"] = "XMLHttpRequest"
    return (
        "(async () => {"
        f"const response = await fetch({json.dumps(url)}, {{"
        f"method: {json.dumps(method)},"
        "credentials: 'include',"
        f"referrer: {json.dumps(referer)},"
        "referrerPolicy: 'strict-origin-when-cross-origin',"
        f"headers: {json.dumps(headers, ensure_ascii=False)},"
        f"body: {body_js if body is None else f'JSON.stringify({body_js})'}"
        "});"
        "const text = await response.text();"
        "return JSON.stringify({status: response.status, text});"
        "})()"
    )
