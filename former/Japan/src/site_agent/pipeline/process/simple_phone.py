from __future__ import annotations

from typing import Any

from gmap_agent.crawler import GoogleMapsSearcher, SearchConfig
from gmap_agent.http_client import HttpClient, HttpConfig


_CID_KEYS = ("cid", "place_cid", "google_cid")
_DUMMY_PB = "!4m12!1m3!1d0!2d139.767!3d35.681!2m3!1f0!2f0!3f0!3m2!1i80!2i80!4f13.1!7i20"


def _pick_cid(raw: dict[str, Any] | None) -> str | None:
    if not isinstance(raw, dict):
        return None
    for key in _CID_KEYS:
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


class SimplePhoneResolver:
    def __init__(
        self,
        *,
        hl: str = "ja",
        gl: str = "jp",
        timeout: int = 20,
        retries: int = 2,
        backoff_seconds: float = 1.0,
    ) -> None:
        self._searcher = GoogleMapsSearcher(
            HttpClient(
                HttpConfig(
                    hl=hl,
                    gl=gl,
                    timeout=timeout,
                    max_retries=retries,
                    backoff_seconds=backoff_seconds,
                )
            ),
            SearchConfig(pb_template=_DUMMY_PB, query=""),
        )

    def resolve_from_raw(self, raw: dict[str, Any] | None) -> str | None:
        cid = _pick_cid(raw)
        if not cid:
            return None
        try:
            phone = self._searcher.fetch_place_phone(cid)
        except Exception:
            return None
        if not isinstance(phone, str):
            return None
        phone_text = phone.strip()
        return phone_text or None
