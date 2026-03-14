from __future__ import annotations

import html
import re
import threading
import urllib.parse
from dataclasses import dataclass

from ..http_client import HttpClient
from ..models import PlaceRecord
from ..parsing import extract_phone_from_preview_text, parse_places, parse_tbm_map_payload


_PREVIEW_PLACE_HREF_RE = re.compile(r'<link[^>]+href="([^"]*?/maps/preview/place\?[^"]+)"', re.I)


@dataclass
class SearchConfig:
    pb_template: str
    query: str
    base_url: str = "https://www.google.com"
    page_size: int | None = None


class GoogleMapsSearcher:
    def __init__(self, http: HttpClient, search: SearchConfig) -> None:
        self.http = http
        self.search = search
        self._phone_cache: dict[str, str | None] = {}
        self._phone_lock = threading.Lock()

    def search_places(self, center_lat: float, center_lng: float, query: str | None = None) -> list[PlaceRecord]:
        pb = replace_lat_lng(self.search.pb_template, center_lat, center_lng)
        pb = replace_page_size(pb, self.search.page_size)
        params = {
            "tbm": "map",
            "hl": self.http.config.hl,
            "gl": self.http.config.gl,
            "q": query or self.search.query,
            "pb": pb,
        }
        base = self.search.base_url.rstrip("/")
        url = f"{base}/search?{urllib.parse.urlencode(params)}"
        text = self.http.get(url)
        payload = parse_tbm_map_payload(text)
        return parse_places(payload, source="google_maps")

    def fetch_place_phone(self, cid: str) -> str | None:
        cid_text = (cid or "").strip()
        if not cid_text:
            return None
        with self._phone_lock:
            if cid_text in self._phone_cache:
                return self._phone_cache[cid_text]
        phone = self._fetch_place_phone_uncached(cid_text)
        with self._phone_lock:
            self._phone_cache[cid_text] = phone
        return phone

    def _fetch_place_phone_uncached(self, cid: str) -> str | None:
        cid_decimal = _cid_to_decimal(cid)
        if not cid_decimal:
            return None
        base = self.search.base_url.rstrip("/")
        maps_url = f"{base}/maps?cid={cid_decimal}&hl={self.http.config.hl}"
        if self.http.config.gl:
            maps_url += f"&gl={self.http.config.gl}"
        place_page_text = self.http.get(maps_url)
        preview_url = _extract_preview_place_url(place_page_text, base_url=base)
        if not preview_url:
            return None
        preview_text = self.http.get(preview_url)
        return extract_phone_from_preview_text(preview_text)


def replace_lat_lng(pb: str, lat: float, lng: float) -> str:
    pattern = re.compile(r"!2d(-?\d+(?:\.\d+)?)!3d(-?\d+(?:\.\d+)?)")
    repl = f"!2d{lng}!3d{lat}"
    if not pattern.search(pb):
        return pb
    return pattern.sub(repl, pb, count=1)


def replace_page_size(pb: str, page_size: int | None) -> str:
    if not isinstance(page_size, int) or page_size <= 0:
        return pb
    pattern = re.compile(r"!7i\d+")
    if not pattern.search(pb):
        return pb
    return pattern.sub(f"!7i{page_size}", pb, count=1)


def _cid_to_decimal(cid: str) -> str | None:
    parts = cid.split(":", 1)
    if len(parts) != 2:
        return None
    value = parts[1].strip().lower()
    if value.startswith("0x"):
        value = value[2:]
    if not value or not re.fullmatch(r"[0-9a-f]+", value):
        return None
    try:
        return str(int(value, 16))
    except ValueError:
        return None


def _extract_preview_place_url(page_text: str, *, base_url: str) -> str | None:
    if not isinstance(page_text, str) or not page_text.strip():
        return None
    match = _PREVIEW_PLACE_HREF_RE.search(page_text)
    if match is None:
        return None
    href = html.unescape(match.group(1).strip())
    if href.startswith(("http://", "https://")):
        return href
    if href.startswith("/"):
        return base_url + href
    return None
