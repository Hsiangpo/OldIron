from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from ..models import PlaceRecord
from ..utils import flatten_strings, get_nested, is_url, looks_like_domain, normalize_url


XSSI_PREFIX = ")]}'}"  # Google JSON guard
APP_STATE_RE = re.compile(r"APP_INITIALIZATION_STATE=(\[.*?\]);", re.S)

GOOGLE_HOST_HINTS = (
    "google.",
    "gstatic.",
    "googleusercontent.",
    "googleapis.",
    "g.page",
    "goo.gl",
)
SOCIAL_HOSTS = (
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "youtube.com",
    "tiktok.com",
)

_PHONE_RE = re.compile(
    r"(?:\+81[-\s()]*(?:0[-\s()]*)?[1-9]\d{0,3}[-\s()]*\d{1,4}[-\s()]*\d{3,4}|"
    r"0[1-9]\d{0,3}[-\s()]*\d{1,4}[-\s()]*\d{3,4})"
)
_PHONE_HINT_RE = re.compile(r"(?:\+81|(?:^|\b)0\d{1,4}[-\s()]*\d{1,4}[-\s()]*\d{3,4})")


def strip_xssi(text: str) -> str:
    if text.startswith(XSSI_PREFIX):
        parts = text.split("\n", 1)
        return parts[1] if len(parts) > 1 else ""
    return text


def parse_json_text(text: str) -> Any:
    text = text.strip()
    if text.endswith("/*\"\"*/"):
        text = text[: -len("/*\"\"*/")]
    text = strip_xssi(text)
    return json.loads(text)


def _find_embedded_json(obj: Any) -> str | None:
    if isinstance(obj, str):
        candidate = obj.strip()
        if candidate.startswith(XSSI_PREFIX):
            return strip_xssi(candidate)
        if candidate.startswith("[") and candidate.endswith("]") and len(candidate) > 100:
            return candidate
        return None
    if isinstance(obj, list):
        for item in obj:
            found = _find_embedded_json(item)
            if found:
                return found
    return None


def parse_tbm_map_payload(text: str) -> Any:
    try:
        outer = parse_json_text(text)
    except json.JSONDecodeError:
        idx = text.find("[")
        outer = json.loads(strip_xssi(text[idx:])) if idx != -1 else []
    if isinstance(outer, list) and outer:
        if isinstance(outer[0], list) and len(outer[0]) > 1 and isinstance(outer[0][1], str):
            return parse_json_text(outer[0][1])
    embedded = _find_embedded_json(outer)
    if embedded:
        return parse_json_text(embedded)
    return outer


def extract_phone_from_preview_text(text: str) -> str | None:
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        payload = parse_json_text(text)
    except json.JSONDecodeError:
        idx = text.find("[")
        if idx == -1:
            return None
        try:
            payload = json.loads(strip_xssi(text[idx:]))
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, list):
        return None
    block_phone = _extract_phone_from_preview_payload(payload)
    if block_phone:
        return block_phone
    return _extract_phone(payload)


def _looks_like_place_entry(entry: Any) -> bool:
    if not isinstance(entry, list) or len(entry) < 2:
        return False
    details = entry[1]
    if not isinstance(details, list):
        return False
    cid = get_nested(details, [10])
    name = get_nested(details, [11])
    return isinstance(cid, str) and ":" in cid and isinstance(name, str)


def _find_place_entries(payload: Any) -> list:
    candidates: list[list] = []

    def walk(obj: Any) -> None:
        if isinstance(obj, list):
            if obj and all(_looks_like_place_entry(item) for item in obj if isinstance(item, list)):
                if sum(1 for item in obj if _looks_like_place_entry(item)) >= 3:
                    candidates.append(obj)
            for item in obj:
                walk(item)

    walk(payload)
    if candidates:
        return max(candidates, key=len)
    if isinstance(payload, list) and len(payload) > 64 and isinstance(payload[64], list):
        return payload[64]
    return []


def _extract_status(details: list) -> str | None:
    strings = [s for s in flatten_strings(details) if s]
    for key in (
        "正在营业",
        "營業中",
        "已结束营业",
        "已結束營業",
        "营业",
        "營業",
        "Open",
        "Closed",
        "Permanently closed",
    ):
        for s in strings:
            if key in s:
                return s
    return None


def _parse_review_count(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.replace(",", ""))
        except ValueError:
            return None
    return None


def _extract_review_count_from_strings(details: list) -> int | None:
    patterns = (
        re.compile(r"([\d,]+)\s*条评论"),
        re.compile(r"([\d,]+)\s*條評論"),
        re.compile(r"([\d,]+)\s*reviews?", re.IGNORECASE),
    )
    for text in flatten_strings(details):
        if not isinstance(text, str):
            continue
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                try:
                    return int(match.group(1).replace(",", ""))
                except ValueError:
                    continue
    return None


def _unwrap_google_url(url: str) -> str:
    parsed = urlparse(url)
    if "google." not in parsed.netloc:
        return url
    if parsed.path not in ("/url", "/search"):
        return url
    params = parse_qs(parsed.query)
    for key in ("q", "url"):
        candidate = params.get(key, [])
        if candidate:
            return candidate[0]
    return url


def _is_blocked_host(host: str) -> bool:
    host = host.lower()
    if any(hint in host for hint in GOOGLE_HOST_HINTS):
        return True
    if any(host.endswith(social) for social in SOCIAL_HOSTS):
        return True
    return False


def _extract_website(details: list) -> str | None:
    candidates: list[str] = []
    for value in flatten_strings(details):
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text:
            continue
        if is_url(text):
            candidate = normalize_url(_unwrap_google_url(text))
        elif looks_like_domain(text):
            candidate = normalize_url(text)
        else:
            continue
        if not candidate:
            continue
        parsed = urlparse(candidate)
        if not parsed.netloc or _is_blocked_host(parsed.netloc):
            continue
        candidates.append(candidate)
    return candidates[0] if candidates else None


def _normalize_phone(candidate: str) -> str | None:
    if not isinstance(candidate, str):
        return None
    match = _PHONE_RE.search(candidate)
    if not match:
        return None
    raw = match.group(0).strip()
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("81") and len(digits) in (11, 12):
        digits = "0" + digits[2:]
    if not digits.startswith("0") or digits.startswith("00"):
        return None
    if len(digits) not in (10, 11):
        return None
    if len(set(digits)) == 1:
        return None
    return raw if re.search(r"\D", raw) else digits


def _extract_phone(details: list) -> str | None:
    candidates: list[str] = []
    for value in flatten_strings(details):
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text:
            continue
        lower = text.lower()
        if lower.startswith("tel:"):
            phone = _normalize_phone(text[4:])
            if phone:
                return phone
            continue
        if "電話" not in text and "电话" not in text and "tel" not in lower and not _PHONE_HINT_RE.search(text):
            continue
        phone = _normalize_phone(text)
        if phone:
            candidates.append(phone)
    return candidates[0] if candidates else None


def _extract_phone_from_preview_payload(payload: Any) -> str | None:
    candidates: list[str] = []

    def add_candidate(value: Any) -> None:
        if not isinstance(value, str):
            return
        phone = _normalize_phone(value)
        if phone:
            candidates.append(phone)

    def walk(node: Any) -> None:
        if not isinstance(node, list):
            return
        if len(node) >= 2 and isinstance(node[0], str):
            add_candidate(node[0])
            variants = node[1]
            if isinstance(variants, list):
                for item in variants:
                    if isinstance(item, list) and item and isinstance(item[0], str):
                        add_candidate(item[0])
        if len(node) >= 4 and isinstance(node[3], str):
            add_candidate(node[3])
        for child in node:
            walk(child)

    walk(payload)
    return candidates[0] if candidates else None


def parse_places(payload: Any, source: str | None = None) -> list[PlaceRecord]:
    entries = _find_place_entries(payload)
    places: list[PlaceRecord] = []
    for entry in entries:
        if not _looks_like_place_entry(entry):
            continue
        details = entry[1]
        cid = get_nested(details, [10])
        name = get_nested(details, [11])
        rating = get_nested(details, [4, 7])
        review_count = get_nested(details, [4, 8])
        status = _extract_status(details)
        review_count = _parse_review_count(review_count)
        if review_count is None:
            review_count = _extract_review_count_from_strings(details)
        if isinstance(rating, str):
            try:
                rating = float(rating)
            except ValueError:
                rating = None
        website = _extract_website(details)
        phone = _extract_phone(details)
        places.append(
            PlaceRecord(
                cid=str(cid) if cid is not None else "",
                name=name if isinstance(name, str) else None,
                website=website,
                phone=phone,
                rating=rating if isinstance(rating, (int, float)) else None,
                review_count=review_count if isinstance(review_count, int) else None,
                status=status,
                source=source,
            )
        )
    return places
