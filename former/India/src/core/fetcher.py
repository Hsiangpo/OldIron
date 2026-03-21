from __future__ import annotations

import random
import threading
import time
from typing import Dict, Optional

import cloudscraper


class FetchError(RuntimeError):
    pass


class CloudflareChallengeError(FetchError):
    pass


class TooManyRequestsError(FetchError):
    pass


def _is_challenge_page(text: str) -> bool:
    lower = text.lower()
    if "just a moment" in lower or "checking your browser" in lower:
        return True
    if "cf-chl" in lower:
        return True
    if "cf-turnstile" in lower or "cf_turnstile" in lower:
        return True
    if "challenge-platform" in lower and "ray id" in lower:
        return True
    if "attention required!" in lower and "cloudflare" in lower:
        return True
    return False


class Fetcher:
    def __init__(
        self,
        cookies: Optional[Dict[str, str]] = None,
        user_agent: Optional[str] = None,
        timeout: int = 30,
        min_delay: float = 0.1,
        max_delay: float = 0.3,
        max_retries: int = 3,
        backoff_base: float = 1.5,
    ) -> None:
        self._cookies = cookies or {}
        self._user_agent = user_agent
        self._timeout = timeout
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._local = threading.local()

    def get(self, url: str, headers: Optional[Dict[str, str]] = None) -> str:
        for attempt in range(1, self._max_retries + 1):
            self._sleep_jitter()
            try:
                session = self._get_session()
                resp = session.get(url, headers=headers, timeout=self._timeout)
                text = resp.text
                if resp.status_code == 429:
                    raise TooManyRequestsError("HTTP 429")
                if resp.status_code >= 500:
                    raise FetchError(f"HTTP {resp.status_code}")
                if _is_challenge_page(text):
                    raise CloudflareChallengeError("Cloudflare challenge detected")
                return text
            except CloudflareChallengeError:
                if attempt >= self._max_retries:
                    raise
            except TooManyRequestsError:
                if attempt >= self._max_retries:
                    raise
            except Exception as exc:
                if attempt >= self._max_retries:
                    raise FetchError(str(exc)) from exc
            self._backoff(attempt)
        raise FetchError("Exhausted retries")

    def _get_session(self):
        if getattr(self._local, "session", None) is None:
            session = cloudscraper.create_scraper()
            if self._user_agent:
                session.headers.update({"User-Agent": self._user_agent})
            if self._cookies:
                session.cookies.update(self._cookies)
            session.headers.update(
                {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "keep-alive",
                }
            )
            self._local.session = session
        return self._local.session

    def update_cookies(self, cookies: Dict[str, str]) -> None:
        self._cookies = cookies or {}
        self._local = threading.local()

    def _sleep_jitter(self) -> None:
        if self._max_delay <= 0:
            return
        delay = random.uniform(self._min_delay, self._max_delay)
        time.sleep(delay)

    def _backoff(self, attempt: int) -> None:
        sleep_for = self._backoff_base ** attempt + random.uniform(0, 0.5)
        time.sleep(sleep_for)
