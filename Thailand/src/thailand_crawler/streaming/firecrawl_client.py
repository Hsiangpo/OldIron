"""Firecrawl 同步客户端。"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

from curl_cffi import requests as cffi_requests

from thailand_crawler.streaming.key_pool import FirecrawlKeyPool
from thailand_crawler.streaming.key_pool import KeyLease


logger = logging.getLogger(__name__)


class FirecrawlError(RuntimeError):
    """Firecrawl 请求异常。"""

    def __init__(self, code: str, message: str | None = None, *, retry_after: float | None = None) -> None:
        super().__init__(message or code)
        self.code = code
        self.retry_after = retry_after


@dataclass(slots=True)
class FirecrawlClientConfig:
    base_url: str
    timeout_seconds: float = 45.0
    max_retries: int = 2
    only_main_content: bool = True


@dataclass(slots=True)
class FirecrawlKeyAuditSummary:
    total: int = 0
    usable: int = 0
    removed_unauthorized: int = 0
    removed_no_credit: int = 0
    kept_rate_limited: int = 0
    kept_unknown: int = 0


def _safe_json(response: object) -> dict[str, object]:
    try:
        payload = response.json()
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_retry_after(response: object) -> float | None:
    raw = str(response.headers.get('Retry-After', '')).strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _parse_remaining_credits(response: object) -> int | None:
    payload = _safe_json(response)
    data = payload.get('data', {}) if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        return None
    raw = data.get('remainingCredits')
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return None


def _write_key_file(path: Path, keys: list[str]) -> None:
    cleaned = [item.strip() for item in keys if item.strip()]
    text = '\n'.join(cleaned)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text((text + '\n') if text else '', encoding='utf-8')


def _probe_scrape_status(session: object, config: FirecrawlClientConfig, key: str) -> int | str:
    response = session.post(
        urljoin(config.base_url, 'scrape'),
        headers={'Authorization': f'Bearer {key}', 'Accept': 'application/json', 'Content-Type': 'application/json'},
        json={
            'url': 'https://example.com',
            'formats': ['markdown'],
            'onlyMainContent': True,
            'removeBase64Images': True,
        },
        timeout=max(config.timeout_seconds, 5.0),
    )
    return int(response.status_code)


def audit_firecrawl_keys(*, key_file: Path, config: FirecrawlClientConfig) -> FirecrawlKeyAuditSummary:
    keys = FirecrawlKeyPool.load_keys(key_file)
    session = cffi_requests.Session(impersonate='chrome110')
    session.headers.update({'Accept': 'application/json'})
    kept: list[str] = []
    summary = FirecrawlKeyAuditSummary(total=len(keys))
    for key in keys:
        try:
            credit_resp = session.get(
                urljoin(config.base_url, 'team/credit-usage'),
                headers={'Authorization': f'Bearer {key}'},
                timeout=max(config.timeout_seconds, 5.0),
            )
            status = int(credit_resp.status_code)
        except Exception as exc:  # noqa: BLE001
            logger.warning('Firecrawl key 预检异常，保留重试：%s', exc)
            kept.append(key)
            summary.kept_unknown += 1
            continue
        if status == 200:
            remaining = _parse_remaining_credits(credit_resp)
            if remaining is not None and remaining > 0:
                kept.append(key)
                summary.usable += 1
            else:
                summary.removed_no_credit += 1
            continue
        if status == 401:
            summary.removed_unauthorized += 1
            continue
        if status == 402:
            summary.removed_no_credit += 1
            continue
        if status == 429:
            try:
                scrape_status = _probe_scrape_status(session, config, key)
            except Exception as exc:  # noqa: BLE001
                logger.warning('Firecrawl key 429 二次探测异常，先保留：%s', exc)
                kept.append(key)
                summary.kept_rate_limited += 1
                continue
            if scrape_status == 401:
                summary.removed_unauthorized += 1
            elif scrape_status == 402:
                summary.removed_no_credit += 1
            elif scrape_status == 200:
                kept.append(key)
                summary.usable += 1
            else:
                kept.append(key)
                summary.kept_rate_limited += 1
            continue
        kept.append(key)
        summary.kept_unknown += 1
    _write_key_file(key_file, kept)
    return summary


class FirecrawlClient:
    """基于 Key 池的 Firecrawl 请求器。"""

    def __init__(self, *, key_pool: FirecrawlKeyPool, config: FirecrawlClientConfig) -> None:
        self._key_pool = key_pool
        self._config = config
        self._session = cffi_requests.Session(impersonate='chrome110')
        self._session.headers.update({'Accept': 'application/json'})

    def scrape_page(self, url: str) -> dict[str, str]:
        payload = {
            'url': url,
            'formats': ['markdown', 'rawHtml'],
            'onlyMainContent': self._config.only_main_content,
            'timeout': int(max(self._config.timeout_seconds, 5.0) * 1000),
            'removeBase64Images': True,
        }
        data = self._request_json('POST', 'scrape', json_body=payload)
        body = data.get('data', data)
        if not isinstance(body, dict):
            return {'url': url, 'markdown': '', 'raw_html': ''}
        markdown = str(body.get('markdown', '') or '')
        raw_html = str(body.get('rawHtml', '') or '')
        metadata = body.get('metadata', {})
        source_url = str(metadata.get('sourceURL', '') if isinstance(metadata, dict) else '')
        real_url = source_url if source_url.startswith('http') else url
        return {'url': real_url, 'markdown': markdown, 'raw_html': raw_html}

    def _request_json(self, method: str, path: str, *, json_body: dict[str, object] | None = None) -> dict[str, object]:
        attempts = max(self._config.max_retries, 0) + 1
        last_error: Exception | None = None
        for index in range(attempts):
            try:
                lease = self._key_pool.acquire()
            except RuntimeError as exc:
                raise FirecrawlError('firecrawl_key_unavailable', str(exc)) from exc
            try:
                return self._request_once(lease, method, path, json_body=json_body)
            except FirecrawlError as exc:
                last_error = exc
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            finally:
                self._key_pool.release(lease)
            if index < attempts - 1:
                time.sleep(0.6 * (index + 1))
        if isinstance(last_error, FirecrawlError):
            raise last_error
        raise FirecrawlError('firecrawl_request_failed', str(last_error or 'unknown'))

    def _request_once(self, lease: KeyLease, method: str, path: str, *, json_body: dict[str, object] | None = None) -> dict[str, object]:
        url = urljoin(self._config.base_url, path)
        headers = {'Authorization': f'Bearer {lease.key}'}
        response = self._session.request(
            method=method,
            url=url,
            headers=headers,
            json=json_body,
            timeout=max(self._config.timeout_seconds, 5.0),
        )
        status = int(response.status_code)
        if status == 200:
            self._key_pool.mark_success(lease)
            return _safe_json(response)
        if status == 401:
            self._key_pool.disable(lease, 'unauthorized')
            raise FirecrawlError('firecrawl_401')
        if status == 402:
            self._key_pool.disable(lease, 'payment_required')
            raise FirecrawlError('firecrawl_402')
        if status == 429:
            retry_after = _parse_retry_after(response)
            self._key_pool.mark_rate_limited(lease, retry_after)
            raise FirecrawlError('firecrawl_429', retry_after=retry_after)
        if status >= 500:
            raise FirecrawlError('firecrawl_5xx')
        raise FirecrawlError(f'firecrawl_http_{status}')
