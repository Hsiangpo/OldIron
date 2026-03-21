"""Snov 相关工具与客户端，支持多 Key 轮询与额度切换。"""

from __future__ import annotations

import os
import re
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests


class SnovRateLimitError(Exception):
    """Snov 429 限流异常。"""


class SnovNoCreditError(Exception):
    """Snov Key 额度不足，需要切换到其他 Key。"""


@dataclass(frozen=True, slots=True)
class SnovCredential:
    client_id: str
    client_secret: str


DOMAIN_PATTERN = re.compile(
    r"^(?:[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$"
)
NO_CREDIT_HINTS = (
    "not_enough_credits",
    "insufficient_credits",
    "insufficient_tokens",
    "payment_required",
)
EXCLUDED_COMPANY_DOMAIN_SUFFIXES = (
    'go.th',
    'wordpress.com',
    'booking.com',
    'traveloka.com',
    'trip.com',
    'bluepillow.com',
    'laterooms.com',
    'trivago.com',
    'trivago.co.kr',
)
EXCLUDED_COMPANY_DOMAINS = {
    'fb.me',
    'bit.ly',
    'centarahotelsresorts.com',
    'expedia.com',
    'expedia.co.th',
    'expedia.co.kr',
    'agoda.com',
    'hotels.com',
    'zenhotels.com',
    'imperialhotels.com',
    'capellahotels.com',
    'wyndhamhotels.com',
    'dreamhotels.com',
    'ozohotels.com',
    'saiihotels.com',
    'shasahotels.com',
    'shotelsresorts.com',
}


def is_excluded_company_domain(domain: str) -> bool:
    value = str(domain or '').strip().lower()
    if not value or not is_valid_domain(value):
        return False
    if value in EXCLUDED_COMPANY_DOMAINS:
        return True
    for suffix in EXCLUDED_COMPANY_DOMAIN_SUFFIXES:
        if value == suffix or value.endswith('.' + suffix):
            return True
    return False


def extract_domain(raw_url: str) -> str:
    """从 URL 提取根域名。"""
    raw = raw_url.strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    try:
        parsed = urlparse(raw)
    except ValueError:
        return ""
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def is_valid_domain(domain: str) -> bool:
    value = domain.strip().lower()
    return bool(value and DOMAIN_PATTERN.fullmatch(value))


def merge_emails(existing: list[str], incoming: list[str]) -> list[str]:
    merged: list[str] = []
    for item in [*existing, *incoming]:
        value = str(item).strip().lower()
        if value and value not in merged:
            merged.append(value)
    return merged


def load_snov_credentials_from_env(
    fallback_id: str = '',
    fallback_secret: str = '',
) -> tuple[SnovCredential, ...]:
    """加载主 Key 与编号备用 Key。"""
    values: list[SnovCredential] = []
    primary_id = os.getenv('SNOV_CLIENT_ID', '').strip() or fallback_id.strip()
    primary_secret = os.getenv('SNOV_CLIENT_SECRET', '').strip() or fallback_secret.strip()
    if primary_id and primary_secret:
        values.append(SnovCredential(primary_id, primary_secret))
    indexes = sorted(
        {
            int(key.rsplit('_', 1)[1])
            for key in os.environ
            if key.startswith('SNOV_CLIENT_ID_') and key.rsplit('_', 1)[1].isdigit()
        }
    )
    for index in indexes:
        client_id = os.getenv(f'SNOV_CLIENT_ID_{index}', '').strip()
        client_secret = os.getenv(f'SNOV_CLIENT_SECRET_{index}', '').strip()
        if not client_id or not client_secret:
            continue
        credential = SnovCredential(client_id, client_secret)
        if credential not in values:
            values.append(credential)
    return tuple(values)


def _flatten_payload_strings(value: object) -> list[str]:
    if isinstance(value, dict):
        result: list[str] = []
        for item in value.values():
            result.extend(_flatten_payload_strings(item))
        return result
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(_flatten_payload_strings(item))
        return result
    if isinstance(value, str):
        return [value]
    return []


def _is_no_credit_response(response) -> bool:
    if response.status_code == 402:
        return True
    try:
        payload = response.json()
    except Exception:
        return False
    flattened = ' '.join(_flatten_payload_strings(payload)).lower()
    return any(hint in flattened for hint in NO_CREDIT_HINTS)


def _credential_label(credential: SnovCredential) -> str:
    return credential.client_id[-6:] if len(credential.client_id) >= 6 else credential.client_id


@dataclass(slots=True)
class SnovConfig:
    client_id: str = ''
    client_secret: str = ''
    timeout: float = 30.0
    retry_delay: float = 10.0
    max_retries: int = 5
    credentials: tuple[SnovCredential, ...] = ()
    no_credit_cooldown_seconds: float = 3600.0


class SnovCredentialPool:
    """Snov Key 轮询与额度冷却池。"""

    def __init__(
        self,
        credentials: tuple[SnovCredential, ...],
        *,
        no_credit_cooldown_seconds: float,
    ) -> None:
        self._credentials = list(credentials)
        self._cooldown_seconds = max(float(no_credit_cooldown_seconds), 60.0)
        self._blocked_until = {item.client_id: 0.0 for item in self._credentials}
        self._next_index = 0
        self._lock = threading.Lock()

    def acquire_candidates(self) -> list[SnovCredential]:
        with self._lock:
            now = time.monotonic()
            ordered = self._credentials[self._next_index :] + self._credentials[: self._next_index]
            self._next_index = (self._next_index + 1) % max(len(self._credentials), 1)
            available = [
                item for item in ordered if self._blocked_until.get(item.client_id, 0.0) <= now
            ]
        if available:
            return available
        raise SnovNoCreditError('所有 Snov Key 当前都处于额度冷却期。')

    def mark_no_credit(self, credential: SnovCredential) -> None:
        with self._lock:
            self._blocked_until[credential.client_id] = time.monotonic() + self._cooldown_seconds


class SnovClient:
    """Snov API 客户端。"""

    def __init__(
        self,
        config: SnovConfig,
        *,
        credential_pool: SnovCredentialPool | None = None,
    ) -> None:
        self.config = config
        self.session = cffi_requests.Session(impersonate='chrome110')
        credentials = config.credentials or load_snov_credentials_from_env(
            config.client_id,
            config.client_secret,
        )
        self._credential_pool = credential_pool or SnovCredentialPool(
            credentials,
            no_credit_cooldown_seconds=config.no_credit_cooldown_seconds,
        )
        self._access_tokens: dict[str, str] = {}

    def _request_token(self, credential: SnovCredential) -> str:
        for _ in range(self.config.max_retries):
            try:
                response = self.session.post(
                    'https://api.snov.io/v1/oauth/access_token',
                    data={
                        'grant_type': 'client_credentials',
                        'client_id': credential.client_id,
                        'client_secret': credential.client_secret,
                    },
                    timeout=self.config.timeout,
                )
            except Exception:
                time.sleep(self.config.retry_delay)
                continue
            if response.status_code == 429:
                time.sleep(self.config.retry_delay)
                continue
            if _is_no_credit_response(response):
                raise SnovNoCreditError(f'Snov Key {_credential_label(credential)} 额度不足。')
            response.raise_for_status()
            token = str(response.json().get('access_token', '')).strip()
            if token:
                self._access_tokens[credential.client_id] = token
                return token
        raise SnovRateLimitError('Snov token 连续 429')

    def _token(self, credential: SnovCredential) -> str:
        token = self._access_tokens.get(credential.client_id, '').strip()
        if token:
            return token
        return self._request_token(credential)

    def _post_with_retry(self, url: str, data: dict, credential: SnovCredential):
        saw_429 = False
        for _ in range(self.config.max_retries):
            try:
                response = self.session.post(url, data=data, timeout=self.config.timeout)
            except Exception:
                time.sleep(self.config.retry_delay)
                continue
            if response.status_code == 401:
                data['access_token'] = self._request_token(credential)
                continue
            if response.status_code == 429:
                saw_429 = True
                time.sleep(self.config.retry_delay)
                continue
            if _is_no_credit_response(response):
                raise SnovNoCreditError(f'Snov Key {_credential_label(credential)} 额度不足。')
            return response
        if saw_429:
            raise SnovRateLimitError(f'Snov 连续 429: {url}')
        raise RuntimeError(f'Snov POST 请求失败: {url}')

    def _get_with_retry(self, url: str, params: dict, credential: SnovCredential):
        saw_429 = False
        for _ in range(self.config.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=self.config.timeout)
            except Exception:
                time.sleep(self.config.retry_delay)
                continue
            if response.status_code == 401:
                params['access_token'] = self._request_token(credential)
                continue
            if response.status_code == 429:
                saw_429 = True
                time.sleep(self.config.retry_delay)
                continue
            if _is_no_credit_response(response):
                raise SnovNoCreditError(f'Snov Key {_credential_label(credential)} 额度不足。')
            return response
        if saw_429:
            raise SnovRateLimitError(f'Snov 连续 429: {url}')
        raise RuntimeError(f'Snov GET 请求失败: {url}')

    def _run_with_failover(self, runner):
        last_error: Exception | None = None
        for credential in self._credential_pool.acquire_candidates():
            try:
                return runner(credential)
            except SnovNoCreditError as exc:
                self._credential_pool.mark_no_credit(credential)
                last_error = exc
                continue
            except SnovRateLimitError as exc:
                last_error = exc
                continue
        if last_error is not None:
            raise last_error
        raise SnovNoCreditError('没有可用的 Snov Key。')

    def get_domain_emails(self, domain: str) -> list[str]:
        if not is_valid_domain(domain) or is_excluded_company_domain(domain):
            return []

        def _runner(credential: SnovCredential) -> list[str]:
            start = self._post_with_retry(
                'https://api.snov.io/v2/domain-search/domain-emails/start',
                {'access_token': self._token(credential), 'domain': domain},
                credential,
            )
            if start.status_code in {400, 404, 422}:
                return []
            start.raise_for_status()
            result_url = str(start.json().get('links', {}).get('result', '')).strip()
            if not result_url:
                return []
            result = self._get_with_retry(
                result_url,
                {'access_token': self._token(credential)},
                credential,
            )
            if result.status_code in {400, 404, 422}:
                return []
            result.raise_for_status()
            items = result.json().get('data', [])
            emails = [str(item.get('email', '')).strip().lower() for item in items if isinstance(item, dict)]
            return merge_emails([], emails)

        return list(self._run_with_failover(_runner))
