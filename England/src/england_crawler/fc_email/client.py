"""Firecrawl 同步客户端。"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from urllib.parse import urljoin

from curl_cffi import requests as cffi_requests

from .key_pool import FirecrawlKeyPool
from .key_pool import KeyLease


class FirecrawlError(RuntimeError):
    """Firecrawl 请求异常。"""

    def __init__(self, code: str, message: str | None = None, *, retry_after: float | None = None) -> None:
        super().__init__(message or code)
        self.code = code
        self.retry_after = retry_after


@dataclass(slots=True)
class FirecrawlClientConfig:
    base_url: str = "https://api.firecrawl.dev/v2/"
    timeout_seconds: float = 45.0
    max_retries: int = 2
    poll_interval_seconds: float = 1.5
    poll_timeout_seconds: float = 120.0


@dataclass(slots=True)
class EmailExtractResult:
    emails: list[str]
    evidence_url: str
    evidence_quote: str
    contact_form_only: bool


def _safe_json(response: object) -> dict[str, object]:
    try:
        payload = response.json()
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_retry_after(response: object) -> float | None:
    raw = str(response.headers.get("Retry-After", "")).strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _normalize_emails(values: object) -> list[str]:
    if not isinstance(values, list):
        return []
    emails: list[str] = []
    for item in values:
        text = str(item or "").strip().lower()
        if text and "@" in text and text not in emails:
            emails.append(text)
    return emails


def _extract_json_payload(payload: dict[str, object]) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return {}
    for key in ("data", "extract", "result"):
        candidate = data.get(key)
        if isinstance(candidate, dict):
            return candidate
    return data


class FirecrawlClient:
    """基于 Key 池的 Firecrawl 请求器。"""

    def __init__(self, *, key_pool: FirecrawlKeyPool, config: FirecrawlClientConfig) -> None:
        self._key_pool = key_pool
        self._config = config
        self._session = cffi_requests.Session(impersonate="chrome110")
        self._session.headers.update({"Accept": "application/json"})

    def map_site(self, url: str, *, limit: int = 200, include_subdomains: bool = False) -> list[str]:
        payload = {
            "url": url,
            "limit": max(int(limit), 1),
            "ignoreQueryParameters": True,
            "includeSubdomains": bool(include_subdomains),
            "sitemap": "include",
        }
        data = self._request_json("POST", "map", json_body=payload)
        if isinstance(data.get("links"), list):
            return [str(item).strip() for item in data.get("links", []) if str(item).strip()]
        body = data.get("data") if isinstance(data.get("data"), dict) else data
        if isinstance(body, dict) and isinstance(body.get("links"), list):
            return [str(item).strip() for item in body.get("links", []) if str(item).strip()]
        return []

    def extract_emails(
        self,
        urls: list[str],
        *,
        include_subdomains: bool = False,
        allow_external_links: bool = False,
    ) -> EmailExtractResult:
        schema = {
            "type": "object",
            "properties": {
                "emails": {"type": "array", "items": {"type": "string"}},
                "contact_form_only": {"type": "boolean"},
                "evidence_url": {"type": "string"},
                "evidence_quote": {"type": "string"},
            },
            "required": ["emails", "contact_form_only", "evidence_url", "evidence_quote"],
        }
        prompt = (
            "Extract publicly listed company email addresses from these official site pages. "
            "Return only real email addresses that appear on the pages or in mailto links. "
            "Prefer official company contact emails. Exclude social media handles and fake placeholders. "
            "If the site only provides a contact form and no email, set contact_form_only=true and emails=[]. "
            "Always fill evidence_url with the best source page URL and evidence_quote with the shortest direct evidence."
        )
        payload = {
            "urls": urls,
            "prompt": prompt,
            "schema": schema,
            "allowExternalLinks": bool(allow_external_links),
            "enableWebSearch": False,
            "includeSubdomains": bool(include_subdomains),
        }
        start = self._request_json("POST", "extract", json_body=payload)
        data = start.get("data") if isinstance(start.get("data"), dict) else None
        if isinstance(data, dict) and data:
            payload_data = _extract_json_payload(start)
            return EmailExtractResult(
                emails=_normalize_emails(payload_data.get("emails")),
                evidence_url=str(payload_data.get("evidence_url", "") or "").strip(),
                evidence_quote=str(payload_data.get("evidence_quote", "") or "").strip(),
                contact_form_only=bool(payload_data.get("contact_form_only")),
            )
        job_id = str(start.get("id", "") or "").strip()
        if not job_id:
            raise FirecrawlError("firecrawl_extract_no_id")
        return self._poll_extract(job_id)

    def _poll_extract(self, job_id: str) -> EmailExtractResult:
        deadline = time.monotonic() + max(self._config.poll_timeout_seconds, 5.0)
        while True:
            try:
                data = self._request_json("GET", f"extract/{job_id}")
            except FirecrawlError as exc:
                if exc.code == "firecrawl_http_404" and time.monotonic() < deadline:
                    time.sleep(self._config.poll_interval_seconds)
                    continue
                raise
            status = str(data.get("status", "") or "").strip().lower()
            if status in {"completed", "success"}:
                payload = _extract_json_payload(data)
                return EmailExtractResult(
                    emails=_normalize_emails(payload.get("emails")),
                    evidence_url=str(payload.get("evidence_url", "") or "").strip(),
                    evidence_quote=str(payload.get("evidence_quote", "") or "").strip(),
                    contact_form_only=bool(payload.get("contact_form_only")),
                )
            if status in {"failed", "error"}:
                raise FirecrawlError("firecrawl_extract_failed")
            if time.monotonic() >= deadline:
                raise FirecrawlError("firecrawl_extract_timeout")
            time.sleep(self._config.poll_interval_seconds)

    def _request_json(self, method: str, path: str, *, json_body: dict[str, object] | None = None) -> dict[str, object]:
        attempts = max(self._config.max_retries, 0) + 1
        last_error: Exception | None = None
        for attempt_index in range(attempts):
            try:
                lease = self._key_pool.acquire()
            except RuntimeError as exc:
                raise FirecrawlError("firecrawl_key_unavailable", str(exc)) from exc
            try:
                return self._request_once(lease, method, path, json_body=json_body)
            except FirecrawlError as exc:
                last_error = exc
            except Exception as exc:  # noqa: BLE001
                self._key_pool.mark_failure(lease)
                last_error = exc
            finally:
                self._key_pool.release(lease)
            if attempt_index < attempts - 1:
                if not isinstance(last_error, FirecrawlError) or last_error.code != "firecrawl_5xx":
                    time.sleep(0.6 * (attempt_index + 1))
        if isinstance(last_error, FirecrawlError):
            raise last_error
        raise FirecrawlError("firecrawl_request_failed", str(last_error or "unknown"))

    def _request_once(self, lease: KeyLease, method: str, path: str, *, json_body: dict[str, object] | None = None) -> dict[str, object]:
        url = urljoin(self._config.base_url, path)
        response = self._session.request(
            method=method,
            url=url,
            headers={"Authorization": f"Bearer {lease.key}"},
            json=json_body,
            timeout=max(self._config.timeout_seconds, 5.0),
        )
        status = int(response.status_code)
        if status == 200:
            self._key_pool.mark_success(lease)
            return _safe_json(response)
        if status == 401:
            self._key_pool.disable(lease, "unauthorized")
            raise FirecrawlError("firecrawl_401")
        if status == 402:
            self._key_pool.disable(lease, "payment_required")
            raise FirecrawlError("firecrawl_402")
        if status == 429:
            retry_after = _parse_retry_after(response)
            self._key_pool.mark_rate_limited(lease, retry_after)
            raise FirecrawlError("firecrawl_429", retry_after=retry_after)
        if status >= 500:
            raise FirecrawlError("firecrawl_5xx")
        raise FirecrawlError(f"firecrawl_http_{status}")
