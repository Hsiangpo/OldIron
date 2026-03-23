"""Virk CVR HTTP 客户端 — Cloudflare 绕过 + 搜索/详情 API。"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

from curl_cffi import requests as cffi_requests

LOGGER = logging.getLogger(__name__)

# Cloudflare cookie 有效期约 30 分钟，提前 5 分钟刷新
_CF_COOKIE_LIFETIME = 1500  # 25 分钟

# CF challenge 页面标题关键词（多语言）
_CF_CHALLENGE_TITLES = ("just a moment", "请稍候", "un moment")


def _is_cf_challenge(title: str) -> bool:
    """判断页面标题是否为 CF challenge。"""
    t = (title or "").lower().strip()
    return any(kw in t for kw in _CF_CHALLENGE_TITLES)


class CfCookieManager:
    """用独立 Chrome 获取并刷新 cf_clearance cookie。

    不复用 9222 调试端口，而是启动自己的 Chrome 实例。
    优先使用 DrissionPage（非 headless），因为 CF 能检测 headless 特征。
    """

    def __init__(
        self,
        *,
        target_url: str = "https://datacvr.virk.dk/",
        headless: bool = False,
        refresh_interval: int = _CF_COOKIE_LIFETIME,
        proxy_url: str = "",
    ) -> None:
        self._target_url = target_url
        self._headless = headless
        self._refresh_interval = max(refresh_interval, 300)
        self._proxy_url = proxy_url
        self._lock = threading.Lock()
        self._refresh_lock = threading.Lock()  # 防止多线程同时启动浏览器
        self._cookies: dict[str, str] = {}
        self._user_agent: str = ""
        self._last_refresh: float = 0.0

    @property
    def cookies(self) -> dict[str, str]:
        """返回当前有效的 cookie，自动刷新过期的。"""
        if self._should_refresh():
            self.refresh()
        with self._lock:
            return dict(self._cookies)

    @property
    def user_agent(self) -> str:
        if not self._user_agent:
            self.refresh()
        return self._user_agent

    def _should_refresh(self) -> bool:
        return (time.monotonic() - self._last_refresh) > self._refresh_interval or not self._cookies

    def refresh(self) -> dict[str, str]:
        """启动独立浏览器获取 CF cookie。优先 DrissionPage。
        使用 _refresh_lock 保证同一时刻只有一个线程启动浏览器。
        """
        with self._refresh_lock:
            # 拿到锁后二次检查：可能已被其他线程刷新过
            if not self._should_refresh():
                with self._lock:
                    return dict(self._cookies)
            LOGGER.info("Virk CF cookie 刷新开始...")
            try:
                cookies, ua = self._get_cf_cookies_via_drissionpage()
            except Exception as e1:
                LOGGER.warning("DrissionPage 获取 CF cookie 失败: %s，尝试 Playwright...", e1)
                try:
                    cookies, ua = self._get_cf_cookies_via_playwright()
                except Exception as e2:
                    raise RuntimeError(f"所有 CF cookie 获取方式均失败: DP={e1}, PW={e2}") from e2

            with self._lock:
                self._cookies = cookies
                self._user_agent = ua
                self._last_refresh = time.monotonic()
            LOGGER.info("Virk CF cookie 刷新完成，keys=%s", list(cookies.keys()))
            return cookies

    def _get_cf_cookies_via_drissionpage(self) -> tuple[dict[str, str], str]:
        """主要方案：DrissionPage（非 headless，能过 CF 检测）。"""
        from DrissionPage import ChromiumPage, ChromiumOptions

        co = ChromiumOptions()
        # 不用 headless — CF 检测 headless 特征会直接拒绝
        if self._headless:
            co.headless()
        co.set_argument("--disable-blink-features=AutomationControlled")
        if self._proxy_url:
            co.set_proxy(self._proxy_url)

        page = ChromiumPage(co)
        page.get(self._target_url)

        # 等 CF challenge 完成
        for _ in range(60):
            if not _is_cf_challenge(page.title or ""):
                break
            time.sleep(1)

        cookies_result = {}
        for cookie in page.cookies():
            cookies_result[cookie.get("name", "")] = cookie.get("value", "")

        ua = page.run_js("return navigator.userAgent")
        page.quit()

        if "cf_clearance" not in cookies_result:
            raise RuntimeError("DrissionPage 未获取到 cf_clearance cookie")
        return cookies_result, ua

    def _get_cf_cookies_via_playwright(self) -> tuple[dict[str, str], str]:
        """备用方案：Playwright。"""
        from playwright.sync_api import sync_playwright

        cookies_result: dict[str, str] = {}
        ua_result = ""

        with sync_playwright() as pw:
            launch_args = ["--disable-blink-features=AutomationControlled"]
            proxy_opt = {"server": self._proxy_url} if self._proxy_url else None
            browser = pw.chromium.launch(
                headless=False,  # 必须非 headless 才能过 CF
                args=launch_args,
                proxy=proxy_opt,
            )
            context = browser.new_context()
            page = context.new_page()
            ua_result = page.evaluate("navigator.userAgent")

            page.goto(self._target_url, wait_until="domcontentloaded", timeout=60000)
            for _ in range(60):
                if not _is_cf_challenge(page.title()):
                    break
                time.sleep(1)

            for cookie in context.cookies():
                cookies_result[cookie["name"]] = cookie["value"]
            browser.close()

        if "cf_clearance" not in cookies_result:
            raise RuntimeError("Playwright 未获取到 cf_clearance cookie")
        return cookies_result, ua_result


def _chrome_version_from_ua(ua: str) -> str:
    """从 User-Agent 提取 Chrome 主版本号，映射到 curl_cffi impersonate 字符串。"""
    import re
    match = re.search(r"Chrome/(\d+)", ua)
    if not match:
        return "chrome"
    major = int(match.group(1))
    # curl_cffi 支持的版本列表，取最接近且不超过的
    supported = [99, 100, 101, 104, 107, 110, 116, 119, 120, 123, 124, 131]
    best = "chrome"
    for ver in supported:
        if ver <= major:
            best = f"chrome{ver}"
    return best


class VirkClient:
    """Virk CVR API 客户端。"""

    BASE_URL = "https://datacvr.virk.dk"

    def __init__(
        self,
        *,
        cf_manager: CfCookieManager,
        timeout_seconds: float = 30.0,
        proxy_url: str = "",
    ) -> None:
        self._cf = cf_manager
        self._timeout = timeout_seconds
        self._proxy = proxy_url
        self._session_local = threading.local()
        self._impersonate_version: str = ""

    def _get_impersonate(self) -> str:
        """根据浏览器 UA 选择匹配的 curl_cffi impersonate 版本。"""
        if not self._impersonate_version:
            ua = self._cf.user_agent
            self._impersonate_version = _chrome_version_from_ua(ua)
            LOGGER.info("Virk curl_cffi impersonate 版本: %s (UA=%s)",
                        self._impersonate_version, ua[:60])
        return self._impersonate_version

    def _get_session(self) -> cffi_requests.Session:
        """每个线程一个独立 session，impersonate 匹配浏览器版本。"""
        session = getattr(self._session_local, "session", None)
        imp = self._get_impersonate()
        if session is None:
            session = cffi_requests.Session(impersonate=imp)
            self._session_local.session = session
        return session

    def _reset_sessions(self) -> None:
        """CF cookie 刷新后重置所有线程 session。"""
        self._impersonate_version = ""
        self._session_local = threading.local()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        """带 CF cookie 发 HTTP 请求。"""
        session = self._get_session()
        url = f"{self.BASE_URL}{path}"
        cookies = self._cf.cookies
        ua = self._cf.user_agent

        headers = kwargs.pop("headers", {})
        # 强制设置核心请求头，不用 setdefault，确保覆盖 impersonate 默认值
        headers["User-Agent"] = ua
        headers.setdefault("Accept", "application/json, text/plain, */*")
        headers.setdefault("X-Requested-With", "XMLHttpRequest")
        # CF 和 virk API 校验同源请求头
        headers.setdefault("Origin", self.BASE_URL)
        headers.setdefault("Referer", f"{self.BASE_URL}/soegeresultater")

        proxies = {"https": self._proxy, "http": self._proxy} if self._proxy else None

        resp = session.request(
            method,
            url,
            headers=headers,
            cookies=cookies,
            proxies=proxies,
            timeout=self._timeout,
            **kwargs,
        )

        if resp.status_code == 403:
            # CF cookie 过期，强制刷新一次重试
            LOGGER.warning("Virk 请求 403，刷新 CF cookie 后重试: %s", path)
            self._cf.refresh()
            self._reset_sessions()
            session = self._get_session()
            cookies = self._cf.cookies
            headers["User-Agent"] = self._cf.user_agent
            resp = session.request(
                method, url, headers=headers, cookies=cookies,
                proxies=proxies, timeout=self._timeout, **kwargs,
            )

        resp.raise_for_status()
        return resp.json()

    # ---- 常量 API ----

    def fetch_constants(self) -> dict[str, Any]:
        """获取所有常量（Kommune、Region、公司类型等列表）。"""
        return self._request("GET", "/gateway/konstanter/hentAlle")

    # ---- 搜索 API ----

    def search_companies(
        self,
        *,
        kommune: list[str] | None = None,
        virksomhedsform: list[str] | None = None,
        virksomhedsstatus: list[str] | None = None,
        page_index: int = 0,
        page_size: int = 1000,
    ) -> tuple[list[dict[str, Any]], int]:
        """搜索公司列表。

        返回 (公司列表, 总数)。
        """
        payload = {
            "fritekstCommand": {
                "soegOrd": "",
                "sideIndex": str(page_index),
                "enhedstype": "virksomhed",
                "kommune": kommune or [],
                "region": [],
                "antalAnsatte": [],
                "virksomhedsform": virksomhedsform or [],
                "virksomhedsstatus": virksomhedsstatus or ["aktiv", "normal"],
                "virksomhedsmarkering": [],
                "personrolle": [],
                "startdatoFra": "",
                "startdatoTil": "",
                "ophoersdatoFra": "",
                "ophoersdatoTil": "",
                "branchekode": "",
                "size": [str(page_size)],
                "sortering": "",
            }
        }
        data = self._request(
            "POST",
            "/gateway/soeg/fritekst",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        companies = data.get("enheder", [])
        total = int(data.get("total", 0))
        return companies, total

    # ---- 详情 API ----

    def fetch_company_detail(self, cvr: str) -> dict[str, Any]:
        """获取公司完整详情。"""
        return self._request("GET", f"/gateway/virksomhed/hentVirksomhed?cvrnummer={cvr}&locale=da")

    # ---- 解析辅助 ----

    @staticmethod
    def parse_search_company(raw: dict[str, Any], segment: str = "", page: int = 0):
        """将搜索 API 返回的单条记录解析为 VirkCompany 所需字段。"""
        from denmark_crawler.sites.virk.models import VirkCompany
        return VirkCompany(
            cvr=str(raw.get("cvr", "")),
            company_name=str(raw.get("senesteNavn", "")),
            address=str(raw.get("beliggenhedsadresse", "")).replace("\n", ", "),
            postcode=str(raw.get("postnummer", "")),
            city=str(raw.get("by", "")),
            phone=str(raw.get("telefonnummer") or ""),
            email=str(raw.get("email") or ""),
            industry_code=str(raw.get("hovedbranche", "")).split(" ", 1)[0] if raw.get("hovedbranche") else "",
            industry_name=str(raw.get("hovedbranche", "")).split(" ", 1)[1] if " " in str(raw.get("hovedbranche", "")) else str(raw.get("hovedbranche", "")),
            company_type=str(raw.get("virksomhedsform", "")),
            status=str(raw.get("status", "")),
            start_date=str(raw.get("startDato", "")),
            source_segment=segment,
            source_page=page,
        )

    @staticmethod
    def enrich_with_detail(company, detail: dict[str, Any]) -> None:
        """把详情 API 数据合并到公司记录里。"""
        # 扩展信息
        ext = detail.get("udvidedeOplysninger") or {}
        if not company.email and ext.get("email"):
            company.email = str(ext["email"])
        if not company.phone and ext.get("telefon"):
            company.phone = str(ext["telefon"])
        company.kommune = str(ext.get("kommune") or "")
        company.purpose = str(ext.get("formaal") or "")
        company.registered_capital = str(ext.get("registreretKapital") or "")
        branch = ext.get("hovedbranche") or {}
        if branch and not company.industry_code:
            company.industry_code = str(branch.get("branchekode", ""))
            company.industry_name = str(branch.get("titel", ""))

        # 法人/股东
        owners_section = detail.get("ejerforhold") or {}
        legal_owners = owners_section.get("aktiveLegaleEjere") or []
        owners_list = []
        for owner in legal_owners:
            owners_list.append({
                "name": str(owner.get("senesteNavn") or owner.get("navn", "")),
                "address": str(owner.get("adresse", "")),
                "type": str(owner.get("enhedstype", "")),
            })
        company.owners_json = json.dumps(owners_list, ensure_ascii=False)

        # 人员（董事等）— rolle 在组级别，personRoller 是具体人
        # rolle.name 的值如 DIREKTOERER, BESTYRELSE 等
        _DIRECTOR_ROLES = {"DIREKTOERER", "DIREKTION", "ADM_DIREKTION"}
        personkreds = detail.get("personkreds") or {}
        for pk in (personkreds.get("personkredser") or []):
            rolle_obj = pk.get("rolle") or {}
            rolle_name = str(rolle_obj.get("name", "")).upper()
            # 检查是否为董事类角色
            if rolle_name not in _DIRECTOR_ROLES:
                continue
            for pr in (pk.get("personRoller") or []):
                person_name = str(pr.get("senesteNavn") or pr.get("navn", ""))
                if person_name:
                    company.representative = person_name
                    # 取 funktionsVaerdi 的第一个值作为具体职位
                    fv = rolle_obj.get("funktionsVaerdi") or []
                    company.representative_role = str(fv[0]) if fv else rolle_name
                    break
            if company.representative:
                break

        # 如果没有 Direktør，取第一个法人作为代表人
        if not company.representative and owners_list:
            first = owners_list[0]
            # 只取自然人（非公司实体）
            if first.get("type", "").upper() != "VIRKSOMHED":
                company.representative = first["name"]
                company.representative_role = "Ejer"

