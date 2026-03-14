from __future__ import annotations

from dataclasses import dataclass


DEFAULT_BASE_URL = "https://www.zaubacorp.com"
DEFAULT_LISTING_FIRST = (
    "https://www.zaubacorp.com/companies-list/status-Active-company.html"
)
DEFAULT_LISTING_TEMPLATE = (
    "https://www.zaubacorp.com/companies-list/status-Active/p-{page}-company.html"
)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/143.0.0.0 Safari/537.36"
)


@dataclass
class CrawlerConfig:
    start_page: int = 1
    end_page: int | None = None
    concurrency: int = 24
    timeout: int = 30
    min_delay: float = 0.1
    max_delay: float = 0.3
    max_retries: int = 3
    backoff_429_min: float = 20.0
    backoff_429_max: float = 30.0
    backoff_cf_min: float = 60.0
    backoff_cf_max: float = 120.0
    output_dir: str = "output"
    cookies_file: str | None = None
    user_agent: str = DEFAULT_USER_AGENT
    resume: bool = True
    max_challenge_failures: int = 5
    commit_every: int = 50
