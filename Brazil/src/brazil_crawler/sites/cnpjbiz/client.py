"""CNPJ Biz 协议客户端。"""

from __future__ import annotations

import base64
import codecs
import json
import logging
import os
import re
import subprocess
import threading
import time
import urllib.request
from dataclasses import dataclass
from html import unescape
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from curl_cffi import requests as cffi_requests
from websocket import create_connection

from .config import CnpjBizConfig
from .proxy_pool import CnpjBizProxyPool
from .proxy_pool import ProxyPoolConfig
from .selector import RepresentativeCandidate


LOGGER = logging.getLogger(__name__)
_DEFAULT_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
_CHALLENGE_HINTS = (
    "Aguarde um Momento",
    "Just a moment",
    "Verificando se sua conexão é segura",
    "Verify you are human",
)
_STOP_SECTION_LABELS = (
    "Qualificação do responsável pela empresa",
    "Sobre",
    "Compartilhar",
    "FAQ - Perguntas e Respostas",
    "Outras empresas",
)


@dataclass(slots=True)
class CnpjBizBrowserState:
    cookie_header: str
    user_agent: str
    accept_language: str
    sec_ch_ua: str
    sec_ch_ua_platform: str


@dataclass(slots=True)
class CnpjBizListRecord:
    cnpj: str
    company_name: str
    detail_url: str
    city: str
    region: str
    status_text: str
    opened_at: str


@dataclass(slots=True)
class CnpjBizListPage:
    page_url: str
    next_url: str
    records: list[CnpjBizListRecord]


@dataclass(slots=True)
class CnpjBizDetailProfile:
    cnpj: str
    company_name: str
    trade_name: str
    status_text: str
    city: str
    region: str
    opened_at: str
    address: str
    phone: str
    emails: list[str]
    representative_candidates: list[RepresentativeCandidate]
    representative_candidates_json: str
    evidence_url: str


class CnpjBizCookieProvider:
    """从当前 cnpj.biz 页面提取运行态 cookie。"""

    def __init__(self, config: CnpjBizConfig) -> None:
        self._cdp_url = config.cdp_url
        self._timeout_seconds = config.cdp_timeout_seconds
        self._cache_ttl_seconds = config.cookie_cache_seconds
        self._lock = threading.Lock()
        self._cached_state: CnpjBizBrowserState | None = None
        self._cached_expire_at = 0.0

    def fetch_state(self, *, force: bool) -> CnpjBizBrowserState:
        with self._lock:
            now = time.time()
            if not force and self._cached_state is not None and now < self._cached_expire_at:
                return self._cached_state
            page_ws_url = self._target_page_ws_url()
            version = self._browser_version_payload()
            cookies = self._fetch_page_cookies(page_ws_url)
            cookie_header = _build_cookie_header(cookies)
            if "cf_clearance=" not in cookie_header:
                raise RuntimeError("9222 浏览器里没有可用的 CNPJ Biz cf cookie，请先在该浏览器里通过 cnpj.biz challenge。")
            state = CnpjBizBrowserState(
                cookie_header=cookie_header,
                user_agent=str(version.get("User-Agent") or _DEFAULT_USER_AGENT).strip() or _DEFAULT_USER_AGENT,
                accept_language="en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                sec_ch_ua=_sec_ch_ua_header(str(version.get("Browser") or "")),
                sec_ch_ua_platform=_sec_ch_platform_header(str(version.get("User-Agent") or "")),
            )
            self._cached_state = state
            self._cached_expire_at = now + self._cache_ttl_seconds
            return state

    def _browser_version_payload(self) -> dict[str, Any]:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        raw = opener.open(f"{self._cdp_url}/json/version", timeout=5).read().decode()
        return json.loads(raw)

    def _target_page_ws_url(self) -> str:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        raw = opener.open(f"{self._cdp_url}/json/list", timeout=5).read().decode()
        pages = json.loads(raw)
        exact_pages = []
        preferred = []
        for item in pages:
            url = str(item.get("url") or "")
            title = str(item.get("title") or "")
            if url == "https://cnpj.biz/empresas" and "Consulta de CNPJ" in title:
                exact_pages.append(item)
                continue
            if url.startswith("https://cnpj.biz/") and "blob:" not in url:
                preferred.append(item)
        if exact_pages:
            return str(exact_pages[0]["webSocketDebuggerUrl"])
        if not preferred:
            raise RuntimeError("9222 浏览器里没有打开 cnpj.biz 页面，无法提取运行态 cookie。")
        return str(preferred[0]["webSocketDebuggerUrl"])

    def _fetch_page_cookies(self, page_ws_url: str) -> list[dict[str, str]]:
        try:
            return self._fetch_page_cookies_via_python(page_ws_url)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CNPJ Biz Python WebSocket 取 cookie 失败，回退 Node：%s", exc)
            return self._fetch_page_cookies_via_node(page_ws_url)

    def _fetch_page_cookies_via_python(self, page_ws_url: str) -> list[dict[str, str]]:
        ws = create_connection(
            page_ws_url,
            timeout=self._timeout_seconds,
            http_no_proxy=["127.0.0.1", "localhost"],
        )
        try:
            ws.send(json.dumps({"id": 1, "method": "Network.getAllCookies"}))
            while True:
                message = json.loads(ws.recv())
                if int(message.get("id") or 0) != 1:
                    continue
                cookies = list((message.get("result") or {}).get("cookies") or [])
                return [
                    item
                    for item in cookies
                    if "cnpj.biz" in str(item.get("domain") or "")
                    and "cdn.cnpj.biz" not in str(item.get("domain") or "")
                ]
        finally:
            ws.close()

    def _fetch_page_cookies_via_node(self, page_ws_url: str) -> list[dict[str, str]]:
        script = """
const ws = new WebSocket(process.env.CDP_PAGE_WS_URL);
ws.addEventListener('open', () => {
  ws.send(JSON.stringify({id: 1, method: 'Network.getAllCookies'}));
});
ws.addEventListener('message', (event) => {
  const payload = JSON.parse(event.data);
  if (payload.id !== 1) return;
  const cookies = ((payload.result || {}).cookies || []).filter((item) => {
    const domain = String(item.domain || '');
    return domain.includes('cnpj.biz') && !domain.includes('cdn.cnpj.biz');
  });
  process.stdout.write(JSON.stringify(cookies));
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
            timeout=max(int(self._timeout_seconds), 5),
            env={
                **os.environ,
                "CDP_PAGE_WS_URL": page_ws_url,
            },
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "node websocket helper failed")
        payload = json.loads(result.stdout or "[]")
        return list(payload if isinstance(payload, list) else [])


class CnpjBizClient:
    """CNPJ Biz 列表、详情、解密入口。"""

    def __init__(self, config: CnpjBizConfig) -> None:
        self._config = config
        self._state_provider = CnpjBizCookieProvider(config)
        self._proxy_pool = self._build_proxy_pool(config)
        self._session = self._build_session()

    def close(self) -> None:
        self._session.close()

    def fetch_list_page(self, page_url: str) -> CnpjBizListPage:
        html = self._request_html(page_url, referer="https://cnpj.biz/empresas")
        return parse_list_page(html, page_url)

    def fetch_detail_profile(self, detail_url: str) -> CnpjBizDetailProfile:
        html = self._request_html(detail_url, referer="https://cnpj.biz/empresas")
        return self._parse_detail_profile(html, detail_url)

    def _parse_detail_profile(self, html: str, detail_url: str) -> CnpjBizDetailProfile:
        soup = BeautifulSoup(html, "html.parser")
        lines = _normalize_lines(soup.get_text("\n"))
        cnpj = _extract_cnpj_digits(lines)
        company_name = _extract_label_value(lines, "Razão Social")
        trade_name = _extract_label_value(lines, "Nome Fantasia")
        status_text = _extract_label_value(lines, "Situação")
        opened_at = _extract_label_value(lines, "Data da Abertura")
        city = _extract_label_value(lines, "Município")
        region = _extract_label_value(lines, "Estado")
        address = _build_address(lines)
        phone = _extract_visible_phone(lines)
        emails = _extract_visible_emails(lines)
        representative_candidates = _extract_representative_candidates(lines)
        if not phone and not emails:
            revealed = self._reveal_contacts(
                detail_url=detail_url,
                cnpj=cnpj,
                company_name=company_name,
                soup=soup,
            )
            phone = str(revealed.get("phone") or "").strip()
            emails = [item for item in list(revealed.get("emails") or []) if item]
        return CnpjBizDetailProfile(
            cnpj=cnpj,
            company_name=company_name,
            trade_name=trade_name,
            status_text=status_text,
            city=city,
            region=region,
            opened_at=opened_at,
            address=address,
            phone=phone,
            emails=emails,
            representative_candidates=representative_candidates,
            representative_candidates_json=json.dumps(
                [{"name": item.name, "role": item.role} for item in representative_candidates],
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            evidence_url=detail_url,
        )

    def _reveal_contacts(self, *, detail_url: str, cnpj: str, company_name: str, soup: BeautifulSoup) -> dict[str, Any]:
        email_node = soup.select_one(".email-container[data-email-ct]")
        phone_node = soup.select_one(".telefone-container[data-telefone-ct]")
        if email_node is None and phone_node is None:
            return {"emails": [], "phone": ""}
        key = self._fetch_reveal_key(cnpj=cnpj, detail_url=detail_url, company_name=company_name)
        emails: list[str] = []
        phone = ""
        if email_node is not None:
            email = _decrypt_gcm_b64(
                key_b64=key,
                iv_b64=str(email_node.get("data-email-iv") or ""),
                ct_b64=str(email_node.get("data-email-ct") or ""),
                tag_b64=str(email_node.get("data-email-tag") or ""),
            )
            if email:
                emails.append(email.lower())
        if phone_node is not None:
            phone = _decrypt_gcm_b64(
                key_b64=key,
                iv_b64=str(phone_node.get("data-telefone-iv") or ""),
                ct_b64=str(phone_node.get("data-telefone-ct") or ""),
                tag_b64=str(phone_node.get("data-telefone-tag") or ""),
            )
        return {"emails": emails, "phone": phone}

    def _fetch_reveal_key(self, *, cnpj: str, detail_url: str, company_name: str) -> str:
        payload = {
            "cnpj": cnpj,
            "type": "reveal",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "bd": {
                "user_agent": _DEFAULT_USER_AGENT,
                "page_url": detail_url,
                "page_title": company_name,
                "interaction_data": {
                    "human_indicators": {
                        "has_mouse_movement": True,
                        "natural_movement": True,
                        "time_before_action": 120000,
                    }
                },
            },
        }
        response = self._request_json(
            "https://cnpj.biz/api/getContactCNPJ",
            json_body={"data": _encode_rot13_json(payload)},
            referer=detail_url,
        )
        key = str(response.get("key") or "").strip().replace("\\/", "/")
        if not key:
            raise RuntimeError(f"CNPJ Biz reveal 未返回解密 key：cnpj={cnpj}")
        return key

    def _request_html(self, url: str, *, referer: str) -> str:
        response = self._request("GET", url, referer=referer)
        return str(response.text or "")

    def _request_json(self, url: str, *, json_body: dict[str, Any], referer: str) -> dict[str, Any]:
        response = self._request("POST", url, referer=referer, json_body=json_body)
        try:
            return response.json()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"CNPJ Biz JSON 解析失败：url={url} error={exc}") from exc

    def _request(
        self,
        method: str,
        url: str,
        *,
        referer: str,
        json_body: dict[str, Any] | None = None,
    ):
        last_error: Exception | None = None
        attempts = [(False, self._active_proxy_url())]
        if self._proxy_pool is not None:
            attempts.extend(
                [
                    (False, self._proxy_pool.rotate_proxy()),
                    (True, self._proxy_pool.rotate_proxy()),
                ]
            )
        else:
            attempts.append((True, self._active_proxy_url()))
        for force, proxy_url in attempts:
            state = self._state_provider.fetch_state(force=force)
            headers = self._build_headers(state, referer=referer, is_xhr=json_body is not None)
            self._apply_proxy(proxy_url)
            try:
                response = self._session.request(
                    method,
                    url,
                    headers=headers,
                    json=json_body,
                    impersonate="chrome136",
                    timeout=25,
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue
            if not _looks_like_challenge(response.status_code, str(response.text or "")):
                return response
            last_error = RuntimeError(
                "CNPJ Biz 返回 Cloudflare challenge，"
                "请先在 9222 浏览器里打开 cnpj.biz 并通过验证后再续跑。"
            )
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"CNPJ Biz 请求失败：{method} {url}")

    def _active_proxy_url(self) -> str:
        if self._proxy_pool is not None:
            return self._proxy_pool.current_proxy()
        return str(self._config.proxy_url or "").strip()

    def _build_proxy_pool(self, config: CnpjBizConfig) -> CnpjBizProxyPool | None:
        if not config.proxy_feed_url:
            return None
        return CnpjBizProxyPool(
            ProxyPoolConfig(
                feed_url=config.proxy_feed_url,
                scheme=config.proxy_feed_scheme,
            )
        )

    def _apply_proxy(self, proxy_url: str) -> None:
        text = str(proxy_url or "").strip()
        proxies = {}
        if text:
            proxies = {"http": text, "https": text}
        self._session.proxies = proxies

    def _build_headers(self, state: CnpjBizBrowserState, *, referer: str, is_xhr: bool) -> dict[str, str]:
        headers = {
            "user-agent": state.user_agent,
            "accept-language": state.accept_language,
            "sec-ch-ua": state.sec_ch_ua,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": state.sec_ch_ua_platform,
            "cookie": state.cookie_header,
            "referer": referer,
        }
        if is_xhr:
            headers.update(
                {
                    "accept": "*/*",
                    "content-type": "application/json",
                    "origin": "https://cnpj.biz",
                    "x-requested-with": "XMLHttpRequest",
                }
            )
        else:
            headers.update(
                {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "upgrade-insecure-requests": "1",
                }
            )
        return headers

    def _build_session(self) -> cffi_requests.Session:
        session = cffi_requests.Session(proxies={})
        session.trust_env = False
        return session


def parse_list_page(html: str, page_url: str) -> CnpjBizListPage:
    soup = BeautifulSoup(html, "html.parser")
    records: list[CnpjBizListRecord] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        match = re.search(r"/(\d{14})/?$", href)
        if match is None:
            continue
        text = " ".join(anchor.stripped_strings)
        if not text:
            continue
        company_name = _first_nonempty_text(anchor.find_all("p"))
        if not company_name:
            company_name = text.split(" BAIXADA ")[0].split(" ATIVA ")[0].strip()
        status_text = _first_class_text(anchor, "rounded-full")
        location = _find_location_text(text)
        city, region = _split_city_region(location)
        opened_at = _extract_opened_at(text)
        records.append(
            CnpjBizListRecord(
                cnpj=match.group(1),
                company_name=company_name,
                detail_url=urljoin(page_url, href),
                city=city,
                region=region,
                status_text=status_text,
                opened_at=opened_at,
            )
        )
    next_url = ""
    next_link = soup.find("link", rel="next")
    if next_link is not None:
        next_url = urljoin(page_url, str(next_link.get("href") or "").strip())
    if not next_url:
        next_anchor = soup.find("a", string=lambda value: isinstance(value, str) and "Próxima Página" in value)
        if next_anchor is not None:
            next_url = urljoin(page_url, str(next_anchor.get("href") or "").strip())
    return CnpjBizListPage(page_url=page_url, next_url=next_url, records=records)


def _first_nonempty_text(nodes) -> str:
    for node in nodes:
        text = " ".join(node.stripped_strings).strip()
        if text:
            return text
    return ""


def _first_class_text(anchor: BeautifulSoup, class_fragment: str) -> str:
    target = anchor.find(class_=lambda value: isinstance(value, str) and class_fragment in value)
    return " ".join(target.stripped_strings).strip() if target is not None else ""


def _find_location_text(text: str) -> str:
    match = re.search(r"([A-Za-zÀ-ÿ' .-]+/[A-Z]{2})", text)
    return match.group(1).strip() if match is not None else ""


def _split_city_region(location: str) -> tuple[str, str]:
    if "/" not in location:
        return "", ""
    city, region = location.rsplit("/", 1)
    return city.strip(), region.strip()


def _extract_opened_at(text: str) -> str:
    match = re.search(r"Aberta em (\d{2}/\d{2}/\d{4})", text)
    return match.group(1) if match is not None else ""


def _normalize_lines(text: str) -> list[str]:
    cleaned = unescape(str(text or "")).replace("\xa0", " ")
    return [line.strip(" \t\r\n:") for line in cleaned.splitlines() if line.strip(" \t\r\n:")]


def _extract_label_value(lines: list[str], label: str) -> str:
    prefix = f"{label}:"
    for index, line in enumerate(lines):
        if line.startswith(prefix):
            value = line[len(prefix):].strip()
            if value:
                return value
            if index + 1 < len(lines):
                return lines[index + 1].strip()
        if line == label and index + 1 < len(lines):
            return lines[index + 1].strip()
    return ""


def _extract_cnpj_digits(lines: list[str]) -> str:
    raw = _extract_label_value(lines, "CNPJ")
    matches = re.findall(r"\d{14}", re.sub(r"\D", "", raw))
    if matches:
        return matches[-1]
    digits = re.sub(r"\D+", "", raw)
    return digits[-14:] if len(digits) >= 14 else digits


def _build_address(lines: list[str]) -> str:
    parts = [
        _extract_label_value(lines, "Logradouro"),
        _extract_label_value(lines, "Bairro"),
        _extract_label_value(lines, "CEP"),
        _extract_label_value(lines, "Município"),
        _extract_label_value(lines, "Estado"),
    ]
    return ", ".join([part for part in parts if part])


def _extract_visible_emails(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for line in lines:
        for match in re.findall(r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", line, flags=re.I):
            email = match.strip().lower()
            if "*" in email or email in seen:
                continue
            seen.add(email)
            result.append(email)
    return result


def _extract_visible_phone(lines: list[str]) -> str:
    for line in lines:
        match = re.search(r"(\(\d{2}\)\s*\d[\d\-]+)", line)
        if match is None:
            continue
        phone = match.group(1).strip()
        if "*" in phone:
            continue
        if len(re.sub(r"\D+", "", phone)) < 10:
            continue
        return phone
    return ""


def _extract_representative_candidates(lines: list[str]) -> list[RepresentativeCandidate]:
    collecting = False
    results: list[RepresentativeCandidate] = []
    seen: set[tuple[str, str]] = set()
    for line in lines:
        if line == "Quadro de Sócios e Administradores":
            collecting = True
            continue
        if not collecting:
            continue
        if any(line.startswith(stop) for stop in _STOP_SECTION_LABELS):
            break
        match = re.match(r"(.+?)\s+-\s+(.+)$", line)
        if match is None:
            continue
        name = match.group(1).strip()
        role = match.group(2).strip()
        if not _looks_like_person_name(name):
            continue
        key = (name.lower(), role.lower())
        if key in seen:
            continue
        seen.add(key)
        results.append(RepresentativeCandidate(name=name, role=role))
    return results


def _looks_like_person_name(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    tokens = [token for token in re.split(r"\s+", text) if token]
    if not 2 <= len(tokens) <= 6:
        return False
    for token in tokens:
        if any(ch.isdigit() for ch in token):
            return False
        cleaned = token.replace(".", "").replace("-", "").replace("'", "")
        if not cleaned:
            return False
        if not all(ch.isalpha() or ch in "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝàáâãäåæçèéêëìíîïñòóôõöùúûüýÿ" for ch in cleaned):
            return False
    return True


def _encode_rot13_json(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return base64.b64encode(codecs.encode(text, "rot_13").encode("utf-8")).decode("ascii")


def _decode_b64(value: str) -> bytes:
    text = str(value or "").strip().replace("-", "+").replace("_", "/")
    while len(text) % 4:
        text += "="
    return base64.b64decode(text)


def _decrypt_gcm_b64(*, key_b64: str, iv_b64: str, ct_b64: str, tag_b64: str) -> str:
    if not key_b64 or not iv_b64 or not ct_b64 or not tag_b64:
        return ""
    aes = AESGCM(_decode_b64(key_b64))
    plaintext = aes.decrypt(_decode_b64(iv_b64), _decode_b64(ct_b64) + _decode_b64(tag_b64), None)
    return plaintext.decode("utf-8", errors="ignore").strip()


def _sec_ch_ua_header(browser_label: str) -> str:
    match = re.search(r"/(\d+)\.", str(browser_label or ""))
    major = match.group(1) if match is not None else "147"
    return f'"Google Chrome";v="{major}", "Not.A/Brand";v="8", "Chromium";v="{major}"'


def _sec_ch_platform_header(user_agent: str) -> str:
    lowered = str(user_agent or "").lower()
    if "windows" in lowered:
        return '"Windows"'
    if "linux" in lowered:
        return '"Linux"'
    return '"macOS"'


def _build_cookie_header(cookies: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for item in cookies:
        name = str(item.get("name") or "").strip()
        value = str(item.get("value") or "").strip()
        if not name or not value or name in seen:
            continue
        seen.add(name)
        parts.append(f"{name}={value}")
    return "; ".join(parts)


def _looks_like_challenge(status_code: int, text: str) -> bool:
    if int(status_code or 0) == 403:
        return True
    lowered = str(text or "")
    return any(hint in lowered for hint in _CHALLENGE_HINTS)
