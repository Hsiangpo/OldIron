from __future__ import annotations

SITE_DEADLINE_SAFETY_SECONDS = 8.0
REQUEST_SLOT_WAIT_FLOOR_SECONDS = 0.5
REQUEST_SLOT_WAIT_CAP_SECONDS = 6.0
REQUEST_SLOT_WAIT_MULTIPLIER = 0.75
DISCOVERY_HOMEPAGE_TIMEOUT_CAP_SECONDS = 6.0
PAGE_FETCH_REQUEST_TIMEOUT_CAP_SECONDS = 12.0
COMMON_PROBE_REQUEST_TIMEOUT_SECONDS = 3.0
COMMON_PROBE_BATCH_WAIT_CAP_SECONDS = 6.0
COMMON_PROBE_SLOT_WAIT_SECONDS = 2.0
COMMON_PROBE_TOTAL_WAIT_CAP_SECONDS = 12.0


def cap_page_fetch_timeout(config_timeout_seconds: float, remaining_seconds: float) -> float:
    base_timeout = max(float(config_timeout_seconds or 0.0), 0.05)
    remaining = max(float(remaining_seconds or 0.0), 0.01)
    return max(min(base_timeout, remaining, PAGE_FETCH_REQUEST_TIMEOUT_CAP_SECONDS), 0.05)
