"""丹麦 Virk 浏览器页内 fetch 客户端与解析。"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from html import unescape
from typing import Any
from urllib.parse import quote

import requests
import websocket

from denmark_crawler.virk.models import VirkCompanyRecord
from denmark_crawler.virk.models import VirkSearchCompany


PERSON_LINK_RE = re.compile(r'/enhed/person/[^"]*"[^>]*>([^<]+)</a>', flags=re.I)
DEFAULT_DEBUG_URL = "http://127.0.0.1:9222"
DEFAULT_SITE_URL = "https://datacvr.virk.dk/"


def _clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", unescape(str(value or ""))).strip()


def _clean_email(value: object) -> str:
    text = _clean_text(value).lower()
    return text if "@" in text else ""


def _http_json(url: str, *, method: str = "GET", timeout_seconds: float = 10.0) -> dict[str, Any]:
    response = requests.request(method=method, url=url, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _http_json_list(url: str, *, timeout_seconds: float = 10.0) -> list[dict[str, Any]]:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, list) else []


def _page_ws_url(debug_url: str, *, timeout_seconds: float) -> str:
    targets = _http_json_list(f"{debug_url.rstrip('/')}/json/list", timeout_seconds=timeout_seconds)
    for item in targets:
        if not isinstance(item, dict):
            continue
        if str(item.get("type", "")).strip() != "page":
            continue
        url = str(item.get("url", "")).strip()
        if url.startswith("https://datacvr.virk.dk/"):
            return str(item.get("webSocketDebuggerUrl", "")).strip()
    created = _http_json(
        f"{debug_url.rstrip('/')}/json/new?{quote(DEFAULT_SITE_URL, safe=':/?&=%')}",
        method="PUT",
        timeout_seconds=timeout_seconds,
    )
    time.sleep(1.0)
    return str(created.get("webSocketDebuggerUrl", "")).strip()


def _fetch_via_page(
    ws_url: str,
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    json_body: dict[str, Any] | None,
    timeout_seconds: float,
) -> tuple[int, str]:
    ws = websocket.create_connection(ws_url, timeout=timeout_seconds, suppress_origin=True)
    try:
        body_text = json.dumps(json_body, ensure_ascii=False) if json_body is not None else None
        expression = (
            "(async () => {"
            f"const response = await fetch({json.dumps(url)}, {{"
            f"method: {json.dumps(method)}, "
            f"headers: {json.dumps(headers, ensure_ascii=False)}, "
            f"credentials: 'include', "
            f"body: {json.dumps(body_text) if body_text is not None else 'undefined'}"
            "});"
            "const text = await response.text();"
            "return {status: response.status, body: text};"
            "})()"
        )
        ws.send(
            json.dumps(
                {
                    "id": 1,
                    "method": "Runtime.evaluate",
                    "params": {
                        "expression": expression,
                        "awaitPromise": True,
                        "returnByValue": True,
                    },
                },
                ensure_ascii=False,
            )
        )
        while True:
            payload = json.loads(ws.recv())
            if int(payload.get("id", 0) or 0) != 1:
                continue
            result = ((payload.get("result") or {}).get("result") or {}).get("value")
            if isinstance(result, dict):
                return int(result.get("status", 0) or 0), str(result.get("body", "") or "")
            raise RuntimeError(f"CDP fetch 返回异常：{payload}")
    finally:
        ws.close()


def _collect_detail_emails(payload: dict[str, Any]) -> list[str]:
    emails: list[str] = []
    product_units = ((payload.get("produktionsenheder") or {}).get("aktiveProduktionsenheder") or [])
    for item in product_units:
        if not isinstance(item, dict):
            continue
        stamdata = item.get("stamdata") or {}
        if not isinstance(stamdata, dict):
            continue
        email = _clean_email(stamdata.get("email"))
        if email and email not in emails:
            emails.append(email)
    return emails


def _representative_candidates(payload: dict[str, Any]) -> list[tuple[int, str, str]]:
    candidates: list[tuple[int, str, str]] = []
    personkredser = ((payload.get("personkreds") or {}).get("personkredser") or [])
    for group in personkredser:
        if not isinstance(group, dict):
            continue
        role_key = _clean_text(group.get("rolleTekstnogle"))
        role_score = 10
        if "direktion" in role_key.lower():
            role_score = 100
        elif "bestyrelse" in role_key.lower():
            role_score = 80
        elif "stiftere" in role_key.lower():
            role_score = 20
        for person in group.get("personRoller") or []:
            if not isinstance(person, dict) or _clean_text(person.get("enhedstype")) != "PERSON":
                continue
            name = _clean_text(person.get("senesteNavn"))
            if not name:
                continue
            title_prefix = " ".join(str(v) for v in (person.get("titlePrefix") or []) if str(v).strip())
            score = role_score + (20 if "adm_dir" in title_prefix.lower() else 0)
            candidates.append((score, name, role_key or title_prefix))
    return sorted(candidates, key=lambda item: (-item[0], item[1]))


def _extract_representative(payload: dict[str, Any]) -> tuple[str, str]:
    candidates = _representative_candidates(payload)
    if candidates:
        _score, name, role = candidates[0]
        return name, role
    registrations = payload.get("virksomhedRegistreringer") or []
    for item in registrations:
        if not isinstance(item, dict):
            continue
        text = str(((item.get("registreringsTekst") or {}).get("tekstMedLink")) or "")
        if "Direktion" not in text and "Bestyrelse" not in text:
            continue
        match = PERSON_LINK_RE.search(text)
        if match:
            return _clean_text(match.group(1)), "historisk_registrering"
    return "", ""


def _extract_legal_owner(payload: dict[str, Any]) -> str:
    owners = ((payload.get("ejerforhold") or {}).get("aktiveLegaleEjere") or [])
    for item in owners:
        if not isinstance(item, dict):
            continue
        name = _clean_text(item.get("senesteNavn"))
        if name:
            return name
    return ""


def parse_search_rows(payload: dict[str, Any]) -> tuple[list[VirkSearchCompany], int]:
    rows: list[VirkSearchCompany] = []
    for item in payload.get("enheder") or []:
        if not isinstance(item, dict) or _clean_text(item.get("enhedstype")) != "virksomhed":
            continue
        email = _clean_email(item.get("email"))
        rows.append(
            VirkSearchCompany(
                cvr=_clean_text(item.get("cvr")),
                company_name=_clean_text(item.get("senesteNavn")),
                address=_clean_text(item.get("beliggenhedsadresse")),
                city=_clean_text(item.get("by")),
                postal_code=_clean_text(item.get("postnummer")),
                status=_clean_text(item.get("status")),
                company_form=_clean_text(item.get("virksomhedsform")),
                main_industry=_clean_text(item.get("hovedbranche")),
                start_date=_clean_text(item.get("startDato")),
                phone=_clean_text(item.get("telefonnummer")),
                emails=[email] if email else [],
            )
        )
    total = int(payload.get("virksomhedTotal", payload.get("total", 0)) or 0)
    return rows, total


def merge_detail(search_row: VirkSearchCompany, payload: dict[str, Any]) -> VirkCompanyRecord:
    stamdata = payload.get("stamdata") or {}
    emails: list[str] = []
    for email in [*_collect_detail_emails(payload), *search_row.emails]:
        value = _clean_email(email)
        if value and value not in emails:
            emails.append(value)
    representative, representative_role = _extract_representative(payload)
    legal_owner = _extract_legal_owner(payload)
    return VirkCompanyRecord(
        cvr=search_row.cvr,
        company_name=_clean_text(stamdata.get("navn")) or search_row.company_name,
        address=_clean_text(stamdata.get("adresse")) or search_row.address,
        city=search_row.city,
        postal_code=search_row.postal_code,
        status=_clean_text(stamdata.get("status")) or search_row.status,
        company_form=_clean_text(stamdata.get("virksomhedsform")) or search_row.company_form,
        main_industry=search_row.main_industry,
        start_date=_clean_text(stamdata.get("startdato")) or search_row.start_date,
        phone=search_row.phone,
        emails=emails,
        representative=representative,
        representative_role=representative_role,
        legal_owner=legal_owner,
    )


class VirkClient:
    """基于 9222 浏览器页面执行 fetch 的 Virk 客户端。"""

    def __init__(
        self,
        *,
        cookie_header: str,
        cookie_provider: object | None = None,
        base_url: str = "https://datacvr.virk.dk",
        timeout_seconds: float = 30.0,
        proxy_url: str | None = None,
    ) -> None:
        self.cookie_header = str(cookie_header or "").strip()
        self.cookie_provider = cookie_provider
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(float(timeout_seconds), 5.0)
        self.proxy_url = str(proxy_url or "").strip()
        self.debug_url = os.getenv("VIRK_CHROME_DEBUG_URL", DEFAULT_DEBUG_URL).strip() or DEFAULT_DEBUG_URL
        self.min_interval_seconds = max(float(getattr(cookie_provider, "min_interval_seconds", 0.0) or 0.0), 0.0)
        self.rate_limit_retry_seconds = max(float(getattr(cookie_provider, "rate_limit_retry_seconds", 20.0) or 20.0), 1.0)
        self._request_lock = threading.Lock()
        self._last_request_at = 0.0

    def _request_json(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        query = ""
        if params:
            query = "?" + "&".join(f"{quote(str(key))}={quote(str(value))}" for key, value in params.items())
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "x-requested-with": "XMLHttpRequest",
        }
        if method.upper() == "GET":
            headers.pop("content-type", None)
        url = f"{self.base_url}{path}{query}"
        last_error: Exception | None = None
        for attempt in range(4):
            with self._request_lock:
                if self.min_interval_seconds > 0 and self._last_request_at > 0:
                    wait = self.min_interval_seconds - (time.monotonic() - self._last_request_at)
                    if wait > 0:
                        time.sleep(wait)
                ws_url = _page_ws_url(self.debug_url, timeout_seconds=self.timeout_seconds)
                status, body = _fetch_via_page(
                    ws_url,
                    method=method,
                    url=url,
                    headers=headers,
                    json_body=json_body,
                    timeout_seconds=self.timeout_seconds,
                )
                self._last_request_at = time.monotonic()
            if status == 200:
                payload = json.loads(body)
                return payload if isinstance(payload, dict) else {}
            if status == 429:
                last_error = RuntimeError(f"Virk 请求失败：HTTP 429: {body[:200]}")
                time.sleep(self.rate_limit_retry_seconds)
                continue
            last_error = RuntimeError(f"Virk 请求失败：HTTP {status}: {body[:200]}")
            break
        raise RuntimeError(str(last_error))

    def search_companies(self, *, page_index: int, page_size: int) -> tuple[list[VirkSearchCompany], int]:
        payload = self._request_json("POST", "/gateway/soeg/fritekst", json_body={
            "fritekstCommand": {
                "soegOrd": "",
                "sideIndex": str(max(page_index, 0)),
                "enhedstype": "virksomhed",
                "kommune": [],
                "region": [],
                "antalAnsatte": [],
                "virksomhedsform": [],
                "virksomhedsstatus": ["aktiv", "normal"],
                "virksomhedsmarkering": [],
                "personrolle": [],
                "startdatoFra": "",
                "startdatoTil": "",
                "ophoersdatoFra": "",
                "ophoersdatoTil": "",
                "branchekode": "",
                "size": [str(max(page_size, 1))],
                "sortering": "",
            }
        })
        return parse_search_rows(payload)

    def fetch_company_record(self, search_row: VirkSearchCompany) -> VirkCompanyRecord:
        payload = self._request_json(
            "GET",
            "/gateway/virksomhed/hentVirksomhed",
            params={"cvrnummer": search_row.cvr, "locale": "da"},
        )
        return merge_detail(search_row, payload)
