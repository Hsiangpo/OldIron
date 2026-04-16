"""Snov 官方 API 客户端。"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

from curl_cffi import requests as cffi_requests


LOGGER = logging.getLogger("oldiron_core.snov.client")
DEFAULT_BASE_URL = "https://api.snov.io"
_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", flags=re.I)
_PROCESSING_STATES = {"created", "queued", "pending", "processing", "in_progress", "running"}
_TRANSIENT_STATUS = {408, 425, 500, 502, 503, 504}
_AUTH_NEEDLES = ("unauthorized", "invalid_client", "invalid token", "invalid_token", "access denied")
_QUOTA_NEEDLES = ("credit", "quota", "limit reached", "limit exceeded", "insufficient")
_RATE_LIMIT_NEEDLES = ("too many requests", "rate limit", "try again later")


@dataclass(slots=True)
class SnovCredential:
    """单个 Snov 凭据对。"""

    client_id: str
    client_secret: str


@dataclass(slots=True)
class SnovClientConfig:
    """Snov 客户端配置。"""

    credentials: tuple[SnovCredential, ...]
    base_url: str = DEFAULT_BASE_URL
    proxy_url: str = ""
    timeout_seconds: float = 30.0
    requests_per_minute: int = 50
    poll_interval_seconds: float = 2.0
    poll_timeout_seconds: float = 90.0

    @classmethod
    def from_env(cls) -> SnovClientConfig:
        credentials = tuple(_load_credentials_from_env())
        return cls(
            credentials=credentials,
            base_url=str(os.getenv("SNOV_BASE_URL", DEFAULT_BASE_URL) or DEFAULT_BASE_URL).strip(),
            proxy_url=_default_proxy_url(),
            timeout_seconds=max(float(os.getenv("SNOV_TIMEOUT_SECONDS", "30") or 30), 10.0),
            requests_per_minute=max(int(os.getenv("SNOV_REQUESTS_PER_MINUTE", "50") or 50), 1),
            poll_interval_seconds=max(float(os.getenv("SNOV_POLL_INTERVAL_SECONDS", "2") or 2), 0.5),
            poll_timeout_seconds=max(float(os.getenv("SNOV_POLL_TIMEOUT_SECONDS", "90") or 90), 10.0),
        )

    def validate(self) -> None:
        if not self.credentials:
            raise RuntimeError("缺少 Snov 凭据：请在国家 .env 里配置 SNOV_CLIENT_ID / SNOV_CLIENT_SECRET。")


@dataclass(slots=True)
class SnovProspect:
    """Snov 人员候选。"""

    name: str
    title: str
    prospect_hash: str
    email_lookup_path: str
    source_page: str


class SnovApiError(RuntimeError):
    """Snov API 一般错误。"""


class SnovAuthError(SnovApiError):
    """Snov 认证错误。"""


class SnovQuotaError(SnovApiError):
    """Snov 额度不足。"""


class SnovClient:
    """带自动续 token、限速和重试的 Snov 客户端。"""

    def __init__(self, config: SnovClientConfig) -> None:
        config.validate()
        self._config = config
        self._session = _build_session(config)
        self._token_lock = threading.Lock()
        self._rate_lock = threading.Lock()
        self._credential_index = 0
        self._tokens: dict[int, tuple[str, float]] = {}
        self._last_request_monotonic = 0.0

    def close(self) -> None:
        self._session.close()

    def get_balance(self) -> dict[str, Any]:
        return self._request_json("GET", "/v1/get-balance")

    def get_domain_emails_count(self, domain: str) -> int:
        payload = self._request_json("POST", "/v1/get-domain-emails-count", data={"domain": domain})
        return _extract_first_int(payload, default=0)

    def company_domain_by_name(self, company_name: str) -> str:
        task_url = self._start_task("/v2/company-domain-by-name/start", data={"names[]": [company_name]})
        payload = self._read_task_pages(task_url)[0]
        return _extract_first_domain(payload)

    def fetch_domain_emails(self, domain: str) -> list[str]:
        task_url = self._start_task("/v2/domain-search/domain-emails/start", data={"domain": domain})
        return _collect_emails_from_pages(self._read_task_pages(task_url))

    def fetch_generic_contacts(self, domain: str) -> list[str]:
        task_url = self._start_task("/v2/domain-search/generic-contacts/start", data={"domain": domain})
        return _collect_emails_from_pages(self._read_task_pages(task_url))

    def fetch_prospects(self, domain: str) -> list[SnovProspect]:
        task_url = self._start_task("/v2/domain-search/prospects/start", data={"domain": domain})
        pages = self._read_task_pages(task_url)
        return _collect_prospects_from_pages(pages)

    def fetch_prospect_emails(self, prospect: SnovProspect) -> list[str]:
        lookup_path = prospect.email_lookup_path or f"/v2/domain-search/prospects/search-emails/start/{prospect.prospect_hash}"
        task_url = self._start_task(lookup_path, data={})
        return _collect_emails_from_pages(self._read_task_pages(task_url))

    def _start_task(self, path: str, *, data: dict[str, Any]) -> str:
        payload = self._request_json("POST", path, data=data)
        result_url = _extract_link(payload, "result")
        if result_url:
            return result_url
        task_hash = _extract_task_hash(payload)
        if not task_hash:
            raise SnovApiError(f"Snov 任务创建失败：{path}")
        return self._build_absolute_url(f"/result/{task_hash}")

    def _read_task_pages(self, result_url: str) -> list[dict[str, Any]]:
        pages: list[dict[str, Any]] = []
        next_url = result_url
        while next_url:
            payload = self._poll_task_page(next_url)
            pages.append(payload)
            next_url = _extract_link(payload, "next")
        return pages

    def _poll_task_page(self, url: str) -> dict[str, Any]:
        started = time.monotonic()
        while True:
            payload = self._request_json("GET", url, absolute_url=True)
            if _payload_ready(payload):
                return payload
            if time.monotonic() - started >= self._config.poll_timeout_seconds:
                raise SnovApiError(f"Snov 任务等待超时：{url}")
            time.sleep(self._config.poll_interval_seconds)

    def _request_json(
        self,
        method: str,
        path_or_url: str,
        *,
        data: dict[str, Any] | None = None,
        absolute_url: bool = False,
    ) -> dict[str, Any]:
        tried_credentials: set[int] = set()
        transient_backoff = 2.0
        while True:
            credential_index = self._credential_index
            token = self._get_access_token(credential_index)
            url = path_or_url if absolute_url else self._build_absolute_url(path_or_url)
            try:
                response = self._send_request(method, url, token=token, data=data)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Snov 请求异常，%ss 后重试：url=%s error=%s", transient_backoff, url, exc)
                time.sleep(transient_backoff)
                transient_backoff = min(transient_backoff * 2, 60.0)
                continue
            payload = _safe_json(response)
            error_text = _extract_error_text(payload, response.status_code)
            if response.status_code == 401 or _contains_any(error_text, _AUTH_NEEDLES):
                self._clear_token(credential_index)
                tried_credentials.add(credential_index)
                if self._advance_credential(tried_credentials):
                    continue
                raise SnovAuthError(error_text or "Snov 凭据无效")
            if response.status_code == 429 or _contains_any(error_text, _RATE_LIMIT_NEEDLES):
                delay = _retry_delay_seconds(response.headers.get("Retry-After"), transient_backoff)
                LOGGER.warning("Snov 命中限速，%ss 后继续：url=%s", delay, url)
                time.sleep(delay)
                transient_backoff = min(max(delay * 1.5, transient_backoff), 60.0)
                continue
            if response.status_code in _TRANSIENT_STATUS:
                LOGGER.warning("Snov 上游临时错误，%ss 后重试：url=%s status=%s", transient_backoff, url, response.status_code)
                time.sleep(transient_backoff)
                transient_backoff = min(transient_backoff * 2, 60.0)
                continue
            if _contains_any(error_text, _QUOTA_NEEDLES):
                tried_credentials.add(credential_index)
                if self._advance_credential(tried_credentials):
                    continue
                raise SnovQuotaError(error_text or "Snov 额度不足")
            if response.status_code >= 400:
                raise SnovApiError(error_text or f"Snov 请求失败：status={response.status_code}")
            return payload

    def _send_request(self, method: str, url: str, *, token: str, data: dict[str, Any] | None) -> Any:
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        self._wait_for_rate_window()
        return self._session.request(method, url, headers=headers, data=data, timeout=self._config.timeout_seconds)

    def _get_access_token(self, credential_index: int) -> str:
        cached = self._tokens.get(credential_index)
        if cached and cached[1] > time.time() + 30:
            return cached[0]
        with self._token_lock:
            cached = self._tokens.get(credential_index)
            if cached and cached[1] > time.time() + 30:
                return cached[0]
            credential = self._config.credentials[credential_index]
            self._wait_for_rate_window()
            response = self._session.post(
                self._build_absolute_url("/v1/oauth/access_token"),
                data={
                    "grant_type": "client_credentials",
                    "client_id": credential.client_id,
                    "client_secret": credential.client_secret,
                },
                timeout=self._config.timeout_seconds,
            )
            payload = _safe_json(response)
            token = str(payload.get("access_token") or "").strip()
            expires_in = max(int(payload.get("expires_in") or 3600), 60)
            if not token:
                error_text = _extract_error_text(payload, response.status_code)
                raise SnovAuthError(error_text or "Snov token 获取失败")
            self._tokens[credential_index] = (token, time.time() + expires_in)
            return token

    def _clear_token(self, credential_index: int) -> None:
        with self._token_lock:
            self._tokens.pop(credential_index, None)

    def _advance_credential(self, tried_credentials: set[int]) -> bool:
        if len(self._config.credentials) <= 1:
            return False
        for _ in range(len(self._config.credentials)):
            self._credential_index = (self._credential_index + 1) % len(self._config.credentials)
            if self._credential_index not in tried_credentials:
                LOGGER.warning("Snov 当前凭据不可用，切到下一组凭据：index=%d", self._credential_index + 1)
                return True
        return False

    def _wait_for_rate_window(self) -> None:
        interval = 60.0 / max(self._config.requests_per_minute, 1)
        with self._rate_lock:
            now = time.monotonic()
            sleep_seconds = interval - (now - self._last_request_monotonic)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            self._last_request_monotonic = time.monotonic()

    def _build_absolute_url(self, path_or_url: str) -> str:
        if str(path_or_url or "").startswith("http"):
            return str(path_or_url)
        return urljoin(self._config.base_url.rstrip("/") + "/", str(path_or_url or "").lstrip("/"))


def _build_session(config: SnovClientConfig) -> cffi_requests.Session:
    proxies = {}
    if config.proxy_url:
        proxies = {"http": config.proxy_url, "https": config.proxy_url}
    session = cffi_requests.Session(impersonate="chrome110", proxies=proxies)
    session.trust_env = False
    session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    return session


def _load_credentials_from_env() -> list[SnovCredential]:
    credentials: list[SnovCredential] = []
    for suffix in [""] + [f"_{index}" for index in range(2, 21)]:
        client_id = str(os.getenv(f"SNOV_CLIENT_ID{suffix}", "") or "").strip()
        client_secret = str(os.getenv(f"SNOV_CLIENT_SECRET{suffix}", "") or "").strip()
        if not client_id and not client_secret:
            continue
        if not client_id or not client_secret:
            raise RuntimeError(f"Snov 凭据配置不完整：SNOV_CLIENT_ID{suffix} / SNOV_CLIENT_SECRET{suffix}")
        credentials.append(SnovCredential(client_id=client_id, client_secret=client_secret))
    return credentials


def _default_proxy_url() -> str:
    return str(os.getenv("SNOV_PROXY_URL") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "").strip()


def _safe_json(response: Any) -> dict[str, Any]:
    try:
        payload = response.json()
    except Exception:  # noqa: BLE001
        text = str(getattr(response, "text", "") or "").strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except Exception:  # noqa: BLE001
            return {}
    return payload if isinstance(payload, dict) else {}


def _payload_ready(payload: dict[str, Any]) -> bool:
    state = str(payload.get("status") or payload.get("state") or "").strip().lower()
    if state and state not in _PROCESSING_STATES:
        return True
    if payload.get("data"):
        return True
    return bool(_extract_link(payload, "next"))


def _extract_first_int(payload: dict[str, Any], *, default: int) -> int:
    for key in ("count", "result", "domain_emails_count", "emails_count", "data"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return default


def _extract_task_hash(payload: dict[str, Any]) -> str:
    for key in ("task_hash", "taskHash", "hash"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _extract_link(payload: dict[str, Any], name: str) -> str:
    links = payload.get("links")
    if isinstance(links, dict):
        value = str(links.get(name) or "").strip()
        if value:
            return value
    meta = payload.get("meta")
    if isinstance(meta, dict):
        links = meta.get("links")
        if isinstance(links, dict):
            value = str(links.get(name) or "").strip()
            if value:
                return value
    return ""


def _extract_first_domain(payload: dict[str, Any]) -> str:
    candidates: list[Any] = []
    data = payload.get("data")
    if isinstance(data, list):
        candidates.extend(data)
    elif isinstance(data, dict):
        candidates.append(data)
    candidates.append(payload)
    for item in candidates:
        if not isinstance(item, dict):
            continue
        for key in ("domain", "company_domain", "website", "url"):
            value = _normalize_domain_value(item.get(key))
            if value:
                return value
    return ""


def _collect_emails_from_pages(pages: list[dict[str, Any]]) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for page in pages:
        for email in _extract_emails_from_payload(page.get("data")):
            lowered = email.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            found.append(email)
    return found


def _extract_emails_from_payload(node: Any) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for child in value.values():
                walk(child)
            return
        if isinstance(value, list):
            for child in value:
                walk(child)
            return
        if not isinstance(value, str):
            return
        for match in _EMAIL_RE.findall(value):
            clean = str(match or "").strip(" <>()[]{}.,;:\"'")
            lowered = clean.lower()
            if not clean or lowered in seen:
                continue
            seen.add(lowered)
            found.append(clean)

    walk(node)
    return found


def _collect_prospects_from_pages(pages: list[dict[str, Any]]) -> list[SnovProspect]:
    results: list[SnovProspect] = []
    seen: set[tuple[str, str]] = set()
    for page in pages:
        data = page.get("data")
        if not isinstance(data, list):
            continue
        for item in data:
            prospect = _parse_prospect(item)
            if prospect is None:
                continue
            key = (prospect.name.lower(), prospect.title.lower())
            if key in seen:
                continue
            seen.add(key)
            results.append(prospect)
    return results


def _parse_prospect(item: Any) -> SnovProspect | None:
    if not isinstance(item, dict):
        return None
    name = _build_name(item)
    title = str(item.get("position") or item.get("job_title") or item.get("title") or "").strip()
    prospect_hash = str(item.get("prospect_hash") or item.get("hash") or item.get("id") or "").strip()
    email_lookup_path = (
        str(item.get("search_emails_start") or "").strip()
        or _extract_link(item, "search_emails")
        or _extract_link(item, "searchEmails")
    )
    source_page = str(item.get("source_page") or item.get("source") or "").strip()
    if not name or not title:
        return None
    return SnovProspect(
        name=name,
        title=title,
        prospect_hash=prospect_hash,
        email_lookup_path=email_lookup_path,
        source_page=source_page,
    )


def _build_name(item: dict[str, Any]) -> str:
    full_name = str(item.get("full_name") or item.get("name") or "").strip()
    if full_name:
        return full_name
    first = str(item.get("first_name") or "").strip()
    last = str(item.get("last_name") or "").strip()
    return " ".join(part for part in (first, last) if part)


def _normalize_domain_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text)
    text = text.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    if text.startswith("www."):
        text = text[4:]
    if ":" in text:
        text = text.split(":", 1)[0]
    if "." not in text or " " in text:
        return ""
    return text


def _retry_delay_seconds(retry_after: str | None, default_seconds: float) -> float:
    raw = str(retry_after or "").strip()
    if raw.isdigit():
        return max(float(raw), 1.0)
    return max(default_seconds, 1.0)


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    return any(needle in lowered for needle in needles)


def _extract_error_text(payload: dict[str, Any], status_code: int) -> str:
    candidates: list[Any] = [
        payload.get("message"),
        payload.get("error"),
        payload.get("errors"),
        payload.get("detail"),
        payload.get("description"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
        if isinstance(candidate, dict):
            nested = json.dumps(candidate, ensure_ascii=False)
            if nested and nested != "{}":
                return nested
        if isinstance(candidate, list) and candidate:
            nested = json.dumps(candidate, ensure_ascii=False)
            if nested and nested != "[]":
                return nested
    return "" if status_code < 400 else f"HTTP {status_code}"
