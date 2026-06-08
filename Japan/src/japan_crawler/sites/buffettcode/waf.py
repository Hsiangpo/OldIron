# -*- coding: utf-8 -*-
"""buffettcode 反爬层：blurpath 日本住宅 sticky 会话 + CapSolver 铸 aws-waf-token。

原理（已端到端实测）：
  1. 干净日本住宅 IP 上，buffett-code 对无 token 请求返回 202 静默挑战（不会升级图像验证码）;
  2. 从 202 正文抽出 key/iv/context + challenge.js，交给 CapSolver(AntiAwsWafTask) 穿同一个日本代理求解;
  3. CapSolver 返回 aws-waf-token，curl_cffi 带它 + 同代理即可拿 200 SSR HTML;
  4. token 失效（再次 202/405）则自动重铸。多线程共享会话，重铸加锁去重。
"""
from __future__ import annotations

import logging
import re
import time

from curl_cffi import requests

log = logging.getLogger("buffettcode.waf")

_MINT_URL = "https://www.buffett-code.com/industries/1000001"
_RE_KEY = re.compile(r'"key"\s*:\s*"([^"]+)"')
_RE_IV = re.compile(r'"iv"\s*:\s*"([^"]+)"')
_RE_CTX = re.compile(r'"context"\s*:\s*"([^"]+)"')
_RE_JS = re.compile(r'src="(https://[^"]*?challenge\.js)"')

# 浏览器指纹池：换 IP 时一并换指纹（JA3+UA 一致由 curl_cffi 保证），降低跨 IP 关联/风控
_IMPERSONATIONS = [
    "chrome116", "chrome119", "chrome120", "chrome123", "chrome124",
    "chrome131", "chrome133a", "chrome136", "chrome142", "chrome145", "edge101",
]


def _extract_challenge(body: str) -> dict | None:
    """从挑战页正文抽取 CapSolver 所需参数。"""
    js = _RE_JS.search(body or "")
    if not js:
        return None
    out = {"awsChallengeJS": js.group(1)}
    for field, rx in (("awsKey", _RE_KEY), ("awsIv", _RE_IV), ("awsContext", _RE_CTX)):
        m = rx.search(body)
        if m:
            out[field] = m.group(1)
    return out


class CapSolver:
    """CapSolver AntiAwsWafTask 客户端。"""

    def __init__(self, api_key: str, timeout: int = 120):
        self._key = api_key
        self._timeout = timeout

    def solve(self, website_url: str, cap_proxy: str, params: dict) -> str | None:
        task = {"type": "AntiAwsWafTask", "websiteURL": website_url, "proxy": cap_proxy}
        task.update(params)
        try:
            cr = requests.post("https://api.capsolver.com/createTask",
                               json={"clientKey": self._key, "task": task}, timeout=45).json()
        except Exception as e:  # noqa: BLE001
            log.warning("CapSolver createTask 异常: %s", e)
            return None
        if cr.get("errorId"):
            log.warning("CapSolver createTask 失败: %s %s", cr.get("errorCode"), cr.get("errorDescription"))
            return None
        tid = cr.get("taskId")
        if not tid:
            return None
        deadline = time.time() + self._timeout
        while time.time() < deadline:
            time.sleep(3)
            try:
                res = requests.post("https://api.capsolver.com/getTaskResult",
                                    json={"clientKey": self._key, "taskId": tid}, timeout=45).json()
            except Exception as e:  # noqa: BLE001
                log.warning("CapSolver 轮询异常: %s", e)
                continue
            if res.get("errorId"):
                log.warning("CapSolver 求解失败: %s", res.get("errorDescription"))
                return None
            if res.get("status") == "ready":
                sol = res.get("solution", {})
                return sol.get("cookie") or sol.get("token")
        log.warning("CapSolver 求解超时")
        return None


class _Challenged(Exception):
    """过墙失败：拿不到有效 token。"""


class _RateLimited(Exception):
    """被站点拦截(403/429)，需换 IP 重试。"""


class WafSession:
    """单个日本 sticky 会话（每个 worker 独占一个，无锁）。

    固定代理 + 当前 token；遇 202/405 用 CapSolver 铸 token；任何过墙/代理失败
    都立刻轮换到一个全新住宅 IP 重试（绝不在坏 IP 上死磕）。
    """

    def __init__(self, session_id: str, cfg: dict, capsolver: CapSolver):
        self._cfg = cfg
        self._capsolver = capsolver
        self._base_sid = session_id
        self._imp_seed = sum(ord(c) for c in session_id)
        self._rot = 0
        self._token: str | None = None
        self._net_retry = 4
        self._max_rotate = 4
        self._build(session_id)

    def _build(self, sid: str) -> None:
        cfg = self._cfg
        user = (f"{cfg['user']}-zone-resi-region-{cfg['region']}"
                f"-st--city--session-{sid}-sessionTime-{cfg['sticky_min']}")
        host, port, pw = cfg["host"], cfg["port"], cfg["password"]
        self.session_id = sid
        self._proxy_url = f"http://{user}:{pw}@{host}:{port}"          # curl_cffi 用
        self._cap_proxy = f"http:{host}:{port}:{user}:{pw}"            # CapSolver 用
        self._proxies = {"http": self._proxy_url, "https": self._proxy_url}
        fixed = cfg.get("impersonate")
        self._impersonate = fixed or _IMPERSONATIONS[(self._imp_seed + self._rot) % len(_IMPERSONATIONS)]

    def rotate(self) -> None:
        """换一个全新住宅 IP（新 sticky 会话），清空旧 token。"""
        self._rot += 1
        self._build(f"{self._base_sid}r{self._rot}")
        self._token = None

    @staticmethod
    def _challenged(resp) -> bool:
        return resp.status_code in (202, 405) or ("gokuProps" in (resp.text or ""))

    def _raw_get(self, url: str, token: str | None):
        cookies = {"aws-waf-token": token} if token else None
        last = None
        for i in range(self._net_retry):
            try:
                return requests.get(url, proxies=self._proxies, impersonate=self._impersonate,
                                    timeout=45, cookies=cookies,
                                    headers={"Accept-Language": "ja,en;q=0.9"})
            except Exception as e:  # noqa: BLE001  代理 CONNECT 偶发失败，短退避重试
                last = e
                time.sleep(min(1.5 * (i + 1), 6))
        raise last

    def _mint(self, body: str) -> bool:
        """用 CapSolver 铸一个 token（穿当前 IP）。成功返回 True。"""
        params = _extract_challenge(body)
        if not params:
            try:
                params = _extract_challenge(self._raw_get(_MINT_URL, None).text)
            except Exception:  # noqa: BLE001
                return False
        if not params:
            return False
        token = self._capsolver.solve(_MINT_URL, self._cap_proxy, params)
        if token:
            self._token = token
            return True
        return False

    def _get_once(self, url: str):
        resp = self._raw_get(url, self._token)
        if resp.status_code in (403, 429):
            raise _RateLimited(str(resp.status_code))
        if not self._challenged(resp):
            return resp
        if self._mint(resp.text):
            resp = self._raw_get(url, self._token)
            if resp.status_code in (403, 429):
                raise _RateLimited(str(resp.status_code))
            if not self._challenged(resp):
                return resp
        raise _Challenged()

    def get(self, url: str):
        """抓取；429限流/过墙/代理失败都换 IP+指纹 重试，超过上限抛异常交上层。"""
        last_err: Exception = RuntimeError("unknown")
        for attempt in range(self._max_rotate + 1):
            try:
                return self._get_once(url)
            except _RateLimited as e:
                last_err = RuntimeError(f"被拦截{e}")
                why = f"被拦截({e})"
            except _Challenged:
                last_err = RuntimeError("过墙失败(拿不到token)")
                why = "过墙失败"
            except Exception as e:  # noqa: BLE001
                last_err = e
                why = f"代理失败:{str(e)[:40]}"
            if attempt < self._max_rotate:
                log.info("会话 %s %s，换 IP+指纹 重试(%d/%d)", self.session_id, why, attempt + 1, self._max_rotate)
                self.rotate()
        raise last_err


def build_sessions(cfg: dict, capsolver: CapSolver, n: int) -> list[WafSession]:
    """构建 n 个独立日本 sticky 会话（每个 worker 一个，互不抢锁）。"""
    pre = cfg.get("session_prefix", "")
    return [WafSession(f"bcj{pre}{i:02d}", cfg, capsolver) for i in range(max(1, n))]
