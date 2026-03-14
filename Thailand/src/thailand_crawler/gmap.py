"""Google Maps 官网与企业信息补齐。"""

from __future__ import annotations

import json
import random
import re
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests

from thailand_crawler.models import CompanyRecord
from thailand_crawler.snov import extract_domain
from thailand_crawler.snov import is_excluded_company_domain
from thailand_crawler.snov import is_valid_domain


XSSI_PREFIX = ")]}'"
GOOGLE_HOST_HINTS = (
    "google.",
    "gstatic.",
    "googleusercontent.",
    "googleapis.",
    "g.page",
    "goo.gl",
)
SOCIAL_HOST_HINTS = (
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "tiktok.com",
)
INFO_HOST_HINTS = (
    "wikipedia.org",
    "wikidata.org",
    "wikimedia.org",
    "mapcarta.com",
)
THAI_COMPANY_TOKENS = (
    "บริษัท",
    "จำกัด",
    "มหาชน",
    "หจก",
    "บจก",
    "ห้างหุ้นส่วน",
)
THAI_ADDRESS_TOKENS = (
    "ถนน",
    "หมู่ที่",
    "แขวง",
    "เขต",
    "จังหวัด",
    "ตำบล",
    "อำเภอ",
    "ซอย",
    "ชั้น",
    "อาคาร",
)
CORP_SUFFIX_PATTERNS = (
    r"public\s+company\s+limited",
    r"limited\s+partnership",
    r"company\s+limited",
    r"co\.?\s*,?\s*ltd\.?",
    r"ltd\.?",
    r"limited",
)
CORP_TOKENS = (
    "company limited",
    "company",
    "limited partnership",
    "limited",
    "public company limited",
    "public company",
    "co.",
    "co,",
    "co ",
    "inc.",
    "inc,",
    "inc ",
    "corp.",
    "corp ",
    "ltd.",
    "ltd ",
)
PHONE_PATTERN = re.compile(r"(?:\+?66|0)[0-9\s\-]{7,}")
THAI_TEXT_PATTERN = re.compile(r"[฀-๿]")
MIN_CANDIDATE_SCORE = 45
GENERIC_DOMAIN_TOKENS = {
    'engineering', 'construction', 'development', 'asset', 'assets', 'property', 'properties',
    'service', 'services', 'group', 'thai', 'thailand', 'asia', 'home', 'enterprise', 'enterprises',
    'solution', 'solutions', 'design', 'system', 'systems', 'global', 'intertrade', 'trading',
    'corp', 'corporation', 'project', 'projects', 'energy', 'tech', 'technology', 'plaza', 'land',
}
DEFAULT_SEARCH_PB = (
    "!1m18!1m12!1m3!1d3952.2689056812584!2d100.5018!3d13.7563!2m3!1f0!2f0!3f0!3m2!1i1024!2i768"
    "!4f13.1!3m3!1m2!1s0x30e29f0d0f4df0ed:0x8d0f8b0b0a2bd1a0!2sBangkok!5e0!3m2!1sen!2sth!4v1"
)
MAP_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://www.google.com/maps?hl=en&gl=th",
}


@dataclass(slots=True)
class GoogleMapsConfig:
    hl: str = "en"
    gl: str = "th"
    base_url: str = "https://www.google.com"
    pb_template: str = DEFAULT_SEARCH_PB
    min_delay: float = 0.4
    max_delay: float = 0.9
    long_rest_interval: int = 150
    long_rest_seconds: float = 5.0
    timeout: float = 30.0


@dataclass(slots=True)
class GoogleMapsPlaceResult:
    company_name_en: str = ""
    company_name_th: str = ""
    phone: str = ""
    website: str = ""
    score: int = 0


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _contains_thai_text(value: str) -> bool:
    return bool(THAI_TEXT_PATTERN.search(str(value or "")))


def strip_company_suffix(name: str) -> str:
    value = _normalize_text(name)
    lowered = value.lower()
    suffixes = [
        ' public company limited',
        ' limited partnership',
        ' company limited',
        ' co., ltd.',
        ' co., ltd',
        ' co. ltd.',
        ' co. ltd',
        ' co ltd',
        ' ltd.',
        ' ltd',
        ' limited',
    ]
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if lowered.endswith(suffix):
                lowered = lowered[:-len(suffix)].rstrip(' ,()-')
                changed = True
                break
    return _normalize_text(lowered).upper() if lowered else _normalize_text(name)


def build_gmap_query(record: CompanyRecord) -> str:
    short_name = strip_company_suffix(record.company_name)
    parts = [short_name, record.city, record.region, record.country or "Thailand"]
    return _normalize_text(" ".join(part for part in parts if part))


def build_gmap_queries(record: CompanyRecord) -> list[str]:
    short_name = strip_company_suffix(record.company_name)
    full_name = _normalize_text(record.company_name)
    suffix = [record.city, record.region, record.country or "Thailand"]
    queries: list[str] = []
    for name in (short_name, full_name):
        query = _normalize_text(" ".join(part for part in [name, *suffix] if part))
        if query and query not in queries:
            queries.append(query)
    return queries


def clean_homepage(raw_url: str) -> str:
    value = _normalize_text(raw_url)
    if not value or value.startswith("mailto:"):
        return ""
    if value.startswith("www."):
        value = f"https://{value}"
    if value.startswith("//"):
        value = f"https:{value}"
    if not value.startswith(("http://", "https://")):
        if re.fullmatch(r"[A-Za-z0-9.-]+\.[A-Za-z]{2,63}(?:/.*)?", value):
            value = f"https://{value}"
        else:
            return ""
    domain = extract_domain(value)
    if not domain or not is_valid_domain(domain):
        return ""
    if is_excluded_company_domain(domain):
        return ""
    if any(hint in domain for hint in GOOGLE_HOST_HINTS):
        return ""
    if any(domain.endswith(hint) for hint in SOCIAL_HOST_HINTS):
        return ""
    if any(domain.endswith(hint) for hint in INFO_HOST_HINTS):
        return ""
    if value.startswith("http://"):
        value = "https://" + value[len("http://"):]
    return value.rstrip("/")


def _normalize_phone(value: str) -> str:
    raw = _normalize_text(value)
    if not raw:
        return ""
    lowered = raw.lower()
    if not ("+66" in lowered or "tel:" in lowered or " " in raw or "-" in raw):
        return ""
    text = raw.replace("tel:", "")
    text = re.sub(r"[^0-9+\s-]", "", text)
    text = _normalize_text(text)
    if not text:
        return ""
    digits = re.sub(r"\D", "", text)
    if len(digits) not in {9, 10, 11, 12}:
        return ""
    if digits.startswith('000'):
        return ""
    if text.startswith("+66"):
        return text
    if text.startswith("66"):
        return "+" + text
    if text.startswith("0") and (" " in raw or "-" in raw or "tel:" in lowered):
        return text
    return ""


class GoogleMapsClient:
    """Google Maps 协议搜索客户端。"""

    def __init__(self, config: GoogleMapsConfig | None = None) -> None:
        self.config = config or GoogleMapsConfig()
        self.session = cffi_requests.Session(impersonate="chrome110")
        self._request_count = 0

    def _sleep(self) -> None:
        time.sleep(random.uniform(self.config.min_delay, self.config.max_delay))
        self._request_count += 1
        if self._request_count % self.config.long_rest_interval == 0:
            time.sleep(self.config.long_rest_seconds)

    def _search_raw(self, query: str, max_retries: int = 4) -> str:
        params = {
            "tbm": "map",
            "hl": self.config.hl,
            "gl": self.config.gl,
            "q": query,
            "pb": self.config.pb_template,
        }
        url = f"{self.config.base_url.rstrip('/')}/search?{urllib.parse.urlencode(params)}"
        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                response = self.session.get(url, headers=MAP_HEADERS, timeout=self.config.timeout)
            except Exception as exc:
                if attempt == max_retries:
                    raise RuntimeError(f"Google Maps 请求失败: {query}") from exc
                time.sleep(min((2**attempt) + random.uniform(0, 1.0), 20))
                continue
            if response.status_code == 429:
                time.sleep(2**attempt * 5)
                continue
            response.raise_for_status()
            return response.text or ""
        raise RuntimeError(f"Google Maps 请求失败: {query}")

    def search_company_profile(self, query: str, company_name: str = "") -> GoogleMapsPlaceResult:
        text = self._search_raw(query)
        payload = _parse_tbm_map_payload(text)
        target_name = _normalize_text(company_name or query)
        candidates = _extract_place_candidates(payload, target_name)
        picked = _select_best_candidate(candidates, target_name)
        if picked is None:
            return GoogleMapsPlaceResult()
        return GoogleMapsPlaceResult(
            company_name_en=_normalize_text(picked.get("name", "")),
            company_name_th=_normalize_text(picked.get("company_name_th", "")),
            phone=_normalize_phone(picked.get("phone", "")),
            website=clean_homepage(picked.get("website", "")),
            score=int(picked.get("score", 0) or 0),
        )

    def search_official_website(self, query: str, company_name: str = "") -> str:
        return self.search_company_profile(query, company_name=company_name).website


def _strip_xssi(text: str) -> str:
    if text.startswith(XSSI_PREFIX):
        parts = text.split("\n", 1)
        return parts[1] if len(parts) > 1 else ""
    return text


def _parse_json_text(text: str) -> Any:
    cleaned = (text or "").strip()
    if cleaned.endswith("/*\"\"*/"):
        cleaned = cleaned[: -len("/*\"\"*/")]
    return json.loads(_strip_xssi(cleaned))


def _find_embedded_json(node: Any) -> str | None:
    if isinstance(node, str):
        candidate = node.strip()
        if candidate.startswith("[") and candidate.endswith("]"):
            return candidate
        return None
    if isinstance(node, list):
        for child in node:
            found = _find_embedded_json(child)
            if found:
                return found
    return None


def _parse_tbm_map_payload(text: str) -> Any:
    try:
        outer = _parse_json_text(text)
    except json.JSONDecodeError:
        index = text.find("[")
        if index == -1:
            return []
        outer = json.loads(_strip_xssi(text[index:]))
    if isinstance(outer, list) and outer and isinstance(outer[0], list) and len(outer[0]) > 1:
        item = outer[0][1]
        if isinstance(item, str):
            return _parse_json_text(item)
    embedded = _find_embedded_json(outer)
    return _parse_json_text(embedded) if embedded else outer


def _looks_like_domain(value: str) -> bool:
    text = value.strip().lower()
    if not text or " " in text or "@" in text or "_" in text:
        return False
    if text.startswith(("http://", "https://")):
        return True
    return "." in text


def _unwrap_google_url(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    for key in ("q", "url"):
        values = params.get(key, [])
        if values:
            return values[0]
    return url


def _flatten_strings(node: Any) -> list[str]:
    values: list[str] = []
    if isinstance(node, str):
        values.append(node)
    elif isinstance(node, list):
        for child in node:
            values.extend(_flatten_strings(child))
    return values


def _extract_website(node: Any) -> str:
    for item in _flatten_strings(node):
        text = item.strip()
        if not text:
            continue
        if not (text.startswith(("http://", "https://", "//", "www.")) or _looks_like_domain(text)):
            continue
        cleaned = clean_homepage(_unwrap_google_url(text))
        if cleaned:
            return cleaned
    return ""


def _looks_like_name(value: str) -> bool:
    text = _normalize_text(value)
    if len(text) < 3 or len(text) > 180:
        return False
    if text.startswith(("http://", "https://", "www.")):
        return False
    if text.startswith(("0ah", "0x", "ChI", "/geo/", "tel:")):
        return False
    if PHONE_PATTERN.fullmatch(text.replace(" ", "").replace("-", "")):
        return False
    return any(char.isalpha() for char in text) or _contains_thai_text(text)


def _normalize_name_for_match(text: str) -> str:
    value = _normalize_text(text).lower()
    for token in CORP_TOKENS:
        value = value.replace(token, "")
    return re.sub(r"[^0-9a-zก-๙]+", "", value)


def _company_tokens(text: str) -> list[str]:
    lowered = _normalize_text(text).lower()
    for token in CORP_TOKENS:
        lowered = lowered.replace(token, " ")
    pieces = [part for part in re.split(r"[^0-9a-zก-๙]+", lowered) if part]
    unique: list[str] = []
    for part in pieces:
        if len(part) < 2:
            continue
        if part not in unique:
            unique.append(part)
    return unique


def _name_match_score(query_name: str, candidate_name: str) -> int:
    query = _normalize_name_for_match(query_name)
    candidate = _normalize_name_for_match(candidate_name)
    if not query or not candidate:
        return 0
    if query == candidate:
        return 100
    if query in candidate or candidate in query:
        return 70
    if query[:5] and query[:5] in candidate:
        return 45
    return 0


def _domain_match_score(query_name: str, website: str) -> int:
    domain = extract_domain(website)
    if not domain:
        return 0
    host = domain.lower()
    compact = _normalize_name_for_match(query_name)
    label = host.split(".", 1)[0]
    if compact and (compact == label or label == compact):
        return 100
    if compact and (compact.startswith(label) or label.startswith(compact)):
        return 80
    best = 0
    for token in _company_tokens(query_name):
        if len(token) < 4 or token in GENERIC_DOMAIN_TOKENS:
            continue
        if token == label:
            best = max(best, 95)
            continue
        if token in host or host.startswith(token + "."):
            best = max(best, 80)
    return best


def _candidate_score(query_name: str, candidate: dict[str, str]) -> int:
    return max(
        _name_match_score(query_name, candidate.get("name", "")),
        _domain_match_score(query_name, candidate.get("website", "")),
    )


def _thai_name_score(value: str) -> int:
    text = _normalize_text(value)
    if not _contains_thai_text(text):
        return -10_000
    if len(text) < 3 or len(text) > 160:
        return -10_000
    if '#' in text or 'http' in text.lower() or 'www.' in text.lower():
        return -10_000
    if re.search(r"[A-Za-z]{4,}", text):
        return -10_000
    if not any(token in text for token in THAI_COMPANY_TOKENS):
        return -10_000
    score = 120
    if any(token in text for token in THAI_ADDRESS_TOKENS):
        score -= 80
    if re.search(r"\d{3,}", text):
        score -= 40
    if text.startswith(("+", "0")):
        score -= 60
    score += min(len(text), 80) // 8
    return score


def _extract_candidate_name(node: Any, query_name: str) -> str:
    best_name = ""
    best_score = -1
    for item in _flatten_strings(node):
        if not _looks_like_name(item):
            continue
        score = _name_match_score(query_name, item)
        if score > best_score:
            best_name = _normalize_text(item)
            best_score = score
    return best_name


def _extract_candidate_thai_name(node: Any) -> str:
    best_name = ""
    best_score = -10_000
    for item in _flatten_strings(node):
        score = _thai_name_score(item)
        if score > best_score:
            best_name = _normalize_text(item)
            best_score = score
    return best_name if best_score > 0 else ""


def _extract_candidate_phone(node: Any) -> str:
    best_phone = ""
    best_score = -1
    for item in _flatten_strings(node):
        candidate = _normalize_phone(item)
        if not candidate:
            continue
        score = 0
        if candidate.startswith("+66"):
            score = 30
        elif candidate.startswith("0") and " " in candidate:
            score = 20
        else:
            score = 10
        if score > best_score:
            best_phone = candidate
            best_score = score
    return best_phone


def _find_place_entries(payload: Any) -> list[list[Any]]:
    matched: list[list[Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            strings = _flatten_strings(node)
            has_place_id = any("0x" in item and ":0x" in item for item in strings)
            has_signal = bool(_extract_website(node) or _extract_candidate_phone(node) or any(_looks_like_name(item) for item in strings))
            if has_place_id and has_signal:
                matched.append(node)
            for child in node:
                walk(child)

    walk(payload)
    return matched


def _select_best_candidate(candidates: list[dict[str, str]], query_name: str) -> dict[str, str] | None:
    best: dict[str, str] | None = None
    best_score = -1
    for candidate in candidates:
        website = clean_homepage(candidate.get("website", ""))
        company_name_th = _normalize_text(candidate.get("company_name_th", ""))
        phone = _normalize_phone(candidate.get("phone", ""))
        score = _candidate_score(query_name, {**candidate, "website": website})
        enriched = {**candidate, "website": website, "company_name_th": company_name_th, "phone": phone}
        if not website and not company_name_th and not phone:
            continue
        if company_name_th and score < MIN_CANDIDATE_SCORE:
            company_name_th = ''
            enriched["company_name_th"] = ''
        if score > best_score:
            best = {**enriched, "score": str(score)}
            best_score = score
    if best is None or best_score < MIN_CANDIDATE_SCORE:
        return None
    if not best.get("website") and not best.get("company_name_th") and not best.get("phone"):
        return None
    return best


def _extract_place_candidates(payload: Any, query_name: str) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for entry in _find_place_entries(payload):
        name = _extract_candidate_name(entry, query_name)
        website = _extract_website(entry)
        phone = _extract_candidate_phone(entry)
        company_name_th = _extract_candidate_thai_name(entry)
        if name or company_name_th or website or phone:
            candidates.append(
                {
                    "name": name,
                    "company_name_th": company_name_th,
                    "website": website,
                    "phone": phone,
                }
            )
    candidates.sort(key=lambda item: _candidate_score(query_name, item), reverse=True)
    return candidates
