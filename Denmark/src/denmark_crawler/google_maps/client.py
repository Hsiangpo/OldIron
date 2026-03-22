"""Google Maps 协议客户端：按公司英文名补官网与电话。"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
import urllib.parse
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from urllib.parse import parse_qs, urlparse, urlunparse

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

XSSI_PREFIX = ")]}'"
GOOGLE_HOST_HINTS = (
    "google.",
    "gstatic.",
    "googleusercontent.",
    "googleapis.",
    "g.page",
    "goo.gl",
    "localguideprogram",
    "maps.app",
)
SOCIAL_HOSTS = (
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "youtube.com",
    "tiktok.com",
    "blog.naver.com",
)
INFO_HOSTS = (
    "wikipedia.org",
    "wikidata.org",
    "wikimedia.org",
)
FOREIGN_TLDS = (
    ".hk",
    ".com.hk",
    ".in",
    ".my",
    ".cn",
    ".sg",
)
FOREIGN_PHONE_PREFIXES = (
    "+852",
    "+91",
    "+86",
    "+60",
    "+63",
    "+254",
)
FOREIGN_URL_MARKERS = (
    "hong-kong",
    "hongkong",
    "/locations/cn/",
    "/hk/",
    ".hk/",
)
KOREAN_COMPANY_TOKENS = (
    "주식회사",
    "(주)",
    "㈜",
    "유한회사",
    "합자회사",
    "건설",
    "산업",
    "엔지니어링",
    "전기",
    "개발",
    "테크",
    "기업",
)
GENERIC_KOREAN_NAME_TOKENS = {
    "건설회사",
    "회사",
    "공사업체",
    "사업체",
    "건설",
    "산업",
    "엔지니어링",
    "전기",
    "개발",
    "기업",
}
BLOCKED_KOREAN_NAME_PHRASES = (
    "휴업/폐업",
    "존재하지 않음 또는 중복으로 표시",
    "법적 문제 신고",
    "현재 이 유형의 장소에 대한 게시가 사용 중지됨",
    "다른 사용자에게 도움이 될",
    "후기를 공유해 주세요",
    "비즈니스에 대한 소유권 주장",
    "이름 또는 기타 세부정보 변경",
    "전문 기업입니다",
)
KOREAN_ADDRESS_TOKENS = (
    "특별시",
    "광역시",
    "시",
    "군",
    "구",
    "읍",
    "면",
    "동",
    "로",
    "길",
    "번지",
    "아파트",
    "센터",
    "타워",
)
CORP_TOKENS = (
    "주식회사",
    "(주)",
    "㈜",
    "(유)",
    "유한회사",
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
PHONE_PATTERN = re.compile(r"(?:\+\d{1,3}|0)[0-9\s\-]{7,}")
KOREAN_TEXT_PATTERN = re.compile(r"[가-힣]")
MIN_CANDIDATE_SCORE = 45

DEFAULT_SEARCH_PB = (
    "!1z!4m8!1m3!1d100000!2d-0.1278!3d51.5074!3m2!1i1024!2i768!4f13.1!7i20!10b1!12m50!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1!6m2"
    "2!49b1!63m0!66b1!74i150000!85b1!91b1!114b1!149b1!206b1!212b1!213b1!223b1!227b1!232b1!233b1!239b1!244b1!246b1!250b1!253b1!258b1!263b1!10b1!12b1!13b1!14b1!16b1!17"
    "m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e"
    "3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8!1m7!1m2!1m1!1e2!2m2!1i195!2i19"
    "5!3i20!22m5!1s_udLaffGBaTl2roP3dzD4A0!7e81!14m1!3s_udLaffGBaTl2roP3dzD4A0!15i9937!24m107!1m28!13m9!2b1!3b1!4b1!6i1!8b1!9b1!14b1!20b1!25b1!18m17!3b1!4b1!5b1!6b1!"
    "13b1!14b1!17b1!21b1!22b1!27m1!1b0!28b0!32b1!33m1!1b1!34b1!36e2!10m1!8e3!11m1!3e1!14m1!3b0!17b1!20m2!1e3!1e6!24b1!25b1!26b1!27b1!29b1!30m1!2b1!36b1!37b1!39m3!2m2"
    "!2i1!3i1!43b1!52b1!54m1!1b1!55b1!56m1!1b1!61m2!1m1!1e1!65m5!3m4!1m3!1m2!1i224!2i298!72m22!1m8!2b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!4b1!8m10!1m6!4m1!1e1!4m1!1e3!4m1!"
    "1e4!3sother_user_google_review_posts__and__hotel_and_vr_partner_review_posts!6m1!1e1!9b1!89b1!98m3!1b1!2b1!3b1!103b1!113b1!114m3!1b1!2m1!1b1!117b1!122m1!1b1!126"
    "b1!127b1!26m4!2m3!1i80!2i92!4i8!30m28!1m6!1m2!1i0!2i0!2m2!1i530!2i768!1m6!1m2!1i974!2i0!2m2!1i1024!2i768!1m6!1m2!1i0!2i0!2m2!1i1024!2i20!1m6!1m2!1i0!2i748!2m2!1"
    "i1024!2i768!34m19!2b1!3b1!4b1!6b1!8m6!1b1!3b1!4b1!5b1!6b1!7b1!9b1!12b1!14b1!20b1!23b1!25b1!26b1!31b1!37m1!1e81!42b1!49m10!3b1!6m2!1b1!2b1!7m2!1e3!2b1!8b1!9b1!10"
    "e2!50m3!2e2!3m1!3b1!61b1!67m5!7b1!10b1!14b1!15m1!1b0!69i761"
)

MAP_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-GB,en;q=0.9,en-US;q=0.8",
    "referer": "https://www.google.com/maps?hl=en&gl=gb",
    "priority": "u=1, i",
    "sec-ch-ua": '"Chromium";v="131", "Google Chrome";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-arch": '"arm"',
    "sec-ch-ua-bitness": '"64"',
    "sec-ch-ua-full-version-list": '"Chromium";v="131.0.6778.265", "Google Chrome";v="131.0.6778.265", "Not_A Brand";v="24.0.0.0"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-model": '""',
    "sec-ch-ua-platform": '"macOS"',
    "sec-ch-ua-platform-version": '"15.3.0"',
    "sec-ch-ua-wow64": "?0",
    "sec-ch-prefers-color-scheme": "light",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-browser-channel": "stable",
    "x-browser-year": "2026",
}


def _default_google_maps_proxy() -> str:
    return os.getenv("GOOGLE_MAPS_PROXY_URL", "").strip() or "socks5h://127.0.0.1:7897"


@dataclass(slots=True)
class GoogleMapsConfig:
    hl: str = "en"
    gl: str = "gb"
    base_url: str = "https://www.google.com"
    pb_template: str = DEFAULT_SEARCH_PB
    min_delay: float = 1.0
    max_delay: float = 2.0
    long_rest_interval: int = 100
    long_rest_seconds: float = 15.0
    timeout: float = 30.0
    proxy_url: str = field(default_factory=_default_google_maps_proxy)


@dataclass(slots=True)
class GoogleMapsPlaceResult:
    """Google Maps 地点结果。"""

    company_name: str = ""
    phone: str = ""
    website: str = ""
    score: int = 0


class GoogleMapsClient:
    """Google Maps 协议查询客户端。"""

    def __init__(self, config: GoogleMapsConfig | None = None) -> None:
        self.config = config or GoogleMapsConfig()
        self._request_count = 0
        self.session = self._build_session()
        self._warm_up()

    def _build_session(self) -> cffi_requests.Session:
        session = cffi_requests.Session(impersonate="chrome")
        session.trust_env = False
        proxy = str(self.config.proxy_url or "").strip()
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}
        return session

    def _warm_up(self) -> None:
        """先访问 Google Maps 首页拿 cookie，模拟真实浏览器行为。"""
        try:
            self.session.get(
                f"{self.config.base_url}/maps?hl={self.config.hl}&gl={self.config.gl}",
                headers={"accept": "text/html", "accept-language": "en-GB,en;q=0.9"},
                timeout=self.config.timeout,
            )
        except Exception:
            pass  # warm up 失败不影响后续

    def _reset_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._build_session()
        self._warm_up()

    def _sleep(self) -> None:
        delay = random.uniform(self.config.min_delay, self.config.max_delay)
        time.sleep(delay)
        self._request_count += 1
        if (
            self.config.long_rest_interval > 0
            and self._request_count % self.config.long_rest_interval == 0
        ):
            logger.info(
                "Google Maps 已请求 %d 次，休息 %.0fs",
                self._request_count,
                self.config.long_rest_seconds,
            )
            time.sleep(self.config.long_rest_seconds)

    def _search_raw(self, query: str, max_retries: int = 4) -> str:
        params = {
            "tbm": "map",
            "hl": self.config.hl,
            "gl": self.config.gl,
            "q": query,
            "pb": self.config.pb_template,
        }
        base = self.config.base_url.rstrip("/")
        url = f"{base}/search?{urllib.parse.urlencode(params)}"

        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                resp = self.session.get(url, headers=MAP_HEADERS, timeout=self.config.timeout)
            except Exception as exc:
                err_text = str(exc)
                logger.warning("Google Maps 请求异常 (第%d次): %s — %s", attempt, query, err_text)
                if re.search(r"curl: \((28|35|56)\)", err_text):
                    self._reset_session()
                if attempt == max_retries:
                    raise RuntimeError(f"Google Maps 请求失败: {query}") from exc
                time.sleep(min(2**attempt + random.uniform(0, 1.0), 20))
                continue

            if resp.status_code == 429:
                wait = 2**attempt * 10 + random.uniform(5, 15)
                logger.warning("Google Maps 429 限流，等待 %.0fs (第%d次)", wait, attempt)
                self._reset_session()  # 换 TLS 指纹
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                if attempt == max_retries:
                    raise RuntimeError(f"Google Maps 服务端错误 {resp.status_code}: {query}")
                time.sleep(min(2**attempt + random.uniform(0, 1.0), 20))
                continue
            if resp.status_code >= 400:
                raise RuntimeError(f"Google Maps HTTP {resp.status_code}: {query}")

            text = resp.text or ""
            if _looks_like_rate_limited_page(text):
                wait = 2**attempt * 5
                logger.warning("Google Maps 疑似触发验证页，等待 %ds (第%d次)", wait, attempt)
                time.sleep(wait)
                continue
            return text

        raise RuntimeError(f"Google Maps 请求失败: {query}")

    def search_official_website(self, company_name: str) -> str:
        query = _normalize_text(company_name)
        if not query:
            return ""
        text = self._search_raw(query)
        payload = _parse_tbm_map_payload(text)
        candidates = _extract_place_candidates(payload, query)
        if not candidates:
            return ""
        return _pick_best_website(query, candidates)

    def search_company_profile(
        self,
        query: str,
        company_name: str = "",
    ) -> GoogleMapsPlaceResult:
        normalized_query = _normalize_text(query)
        if not normalized_query:
            return GoogleMapsPlaceResult()
        text = self._search_raw(normalized_query)
        payload = _parse_tbm_map_payload(text)
        candidates = _extract_place_candidates(payload, company_name or normalized_query)
        picked = _pick_best_candidate(candidates, company_name or normalized_query)
        if picked is None:
            return GoogleMapsPlaceResult()
        return GoogleMapsPlaceResult(
            company_name=str(picked.get("name", "")),
            phone=picked["phone"],
            website=picked["website"],
            score=picked["score"],
        )

    def close(self) -> None:
        self.session.close()


def _looks_like_rate_limited_page(text: str) -> bool:
    lower = (text or "").lower()
    if not lower:
        return False
    markers = (
        "our systems have detected unusual traffic",
        "unusual traffic from your computer network",
        "/sorry/",
        "please show you're not a robot",
    )
    return any(marker in lower for marker in markers)


def _strip_xssi(text: str) -> str:
    if text.startswith(XSSI_PREFIX):
        parts = text.split("\n", 1)
        return parts[1] if len(parts) > 1 else ""
    return text


def _parse_json_text(text: str) -> Any:
    cleaned = (text or "").strip()
    if cleaned.endswith("/*\"\"*/"):
        cleaned = cleaned[: -len("/*\"\"*/")]
    cleaned = _strip_xssi(cleaned)
    return json.loads(cleaned)


def _find_embedded_json(obj: Any) -> str | None:
    if isinstance(obj, str):
        candidate = obj.strip()
        if candidate.startswith(XSSI_PREFIX):
            return _strip_xssi(candidate)
        if candidate.startswith("[") and candidate.endswith("]") and len(candidate) > 100:
            return candidate
        return None
    if isinstance(obj, list):
        for item in obj:
            found = _find_embedded_json(item)
            if found:
                return found
    return None


def _parse_tbm_map_payload(text: str) -> Any:
    try:
        outer = _parse_json_text(text)
    except json.JSONDecodeError:
        index = text.find("[")
        outer = json.loads(_strip_xssi(text[index:])) if index != -1 else []
    if isinstance(outer, list) and outer:
        if isinstance(outer[0], list) and len(outer[0]) > 1 and isinstance(outer[0][1], str):
            return _parse_json_text(outer[0][1])
    embedded = _find_embedded_json(outer)
    if embedded:
        return _parse_json_text(embedded)
    return outer


def _get_nested(data: Any, path: list[int], default: Any = None) -> Any:
    current = data
    for index in path:
        if not isinstance(current, list) or index >= len(current):
            return default
        current = current[index]
    return current


def _looks_like_place_entry(entry: Any) -> bool:
    if not isinstance(entry, list) or len(entry) < 2:
        return False
    details = entry[1]
    if not isinstance(details, list):
        return False
    cid = _get_nested(details, [10])
    name = _get_nested(details, [11])
    return isinstance(cid, str) and ":" in cid and isinstance(name, str)


def _find_place_entries(payload: Any) -> list[list[Any]]:
    matched: list[list[Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            strings = _flatten_strings(node)
            has_place_id = any("0x" in item and ":0x" in item for item in strings)
            has_signal = bool(
                _extract_website(node)
                or _extract_phone(node)
                or any(_normalize_text(item) for item in strings)
            )
            if has_place_id and has_signal:
                matched.append(node)
            for child in node:
                walk(child)

    walk(payload)
    if matched:
        return matched
    if isinstance(payload, list) and len(payload) > 64 and isinstance(payload[64], list):
        return payload[64]
    return []


def _flatten_strings(obj: Any) -> list[str]:
    values: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, str):
            values.append(node)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(obj)
    return values


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
    lower_host = host.lower()
    if any(hint in lower_host for hint in GOOGLE_HOST_HINTS):
        return True
    if any(lower_host.endswith(item) for item in SOCIAL_HOSTS):
        return True
    if any(lower_host.endswith(item) for item in INFO_HOSTS):
        return True
    return False


def _normalize_url(value: str) -> str:
    text = re.sub(r"\s+", "", (value or "").strip())
    if not text:
        return ""
    if text.startswith("//"):
        text = "https:" + text
    if not text.startswith(("http://", "https://")):
        text = "https://" + text
    parsed = urlparse(text)
    host = (parsed.netloc or "").strip()
    if not host or " " in host:
        return ""
    scheme = "https" if parsed.scheme == "http" else parsed.scheme
    cleaned = parsed._replace(scheme=scheme, netloc=host, fragment="")
    return urlunparse(cleaned).rstrip("/")


def _looks_like_domain(value: str) -> bool:
    text = value.strip().lower()
    if not text or " " in text or "@" in text:
        return False
    if "_" in text:
        return False
    if "/" in text:
        return False
    # 域名必须是纯 ASCII（过滤中文、韩文等垃圾文本含 "......" 的情况）
    if not text.isascii():
        return False
    return "." in text


def _extract_website(details: list[Any]) -> str:
    for value in _flatten_strings(details):
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text:
            continue
        if text.startswith(("http://", "https://", "//")):
            candidate = _normalize_url(_unwrap_google_url(text))
        elif _looks_like_domain(text):
            candidate = _normalize_url(text)
        else:
            continue
        if not candidate:
            continue
        parsed = urlparse(candidate)
        host = (parsed.netloc or "").lower()
        if not host or _is_blocked_host(host):
            continue
        # 域名必须包含至少一个 . 才合法（过滤 https://localguideprogram 等垃圾）
        if "." not in host:
            continue
        return candidate
    return ""


def _normalize_phone(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    if not PHONE_PATTERN.fullmatch(text):
        return ""
    digits = re.sub(r"\D", "", text)
    if len(digits) < 9 or len(digits) > 13:
        return ""
    return text


def _extract_phone(details: list[Any]) -> str:
    for value in _flatten_strings(details):
        phone = _normalize_phone(value)
        if phone:
            return phone
    return ""


def _extract_place_candidates(payload: Any, query_name: str) -> list[dict[str, str | int]]:
    entries = _find_place_entries(payload)
    out: list[dict[str, str | int]] = []
    for entry in entries:
        name = _extract_candidate_name(entry, query_name)
        company_name_local = _extract_candidate_local_name(entry)
        website = _extract_website(entry)
        phone = _extract_phone(entry)
        if name or company_name_local or website or phone:
            out.append(
                {
                    "name": name,
                    "company_name_local": company_name_local,
                    "website": website,
                    "phone": phone,
                    "score": 0,
                }
            )
    out.sort(key=lambda item: _candidate_score(query_name, item), reverse=True)
    return out


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _contains_korean_text(value: str) -> bool:
    return bool(KOREAN_TEXT_PATTERN.search(str(value or "")))


def _normalize_name_for_match(text: str) -> str:
    value = _normalize_text(text).lower()
    for token in CORP_TOKENS:
        value = value.replace(token, "")
    value = re.sub(r"[^0-9a-z가-힣]+", "", value)
    return value


def _company_tokens(text: str) -> list[str]:
    lowered = _normalize_text(text).lower()
    for token in CORP_TOKENS:
        lowered = lowered.replace(token, " ")
    pieces = [part for part in re.split(r"[^0-9a-z가-힣]+", lowered) if part]
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
    if query[:4] and query[:4] in candidate:
        return 45
    return 0


def _domain_match_score(query_name: str, website: str) -> int:
    parsed = urlparse(website if "://" in website else f"https://{website}")
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return 0
    compact = _normalize_name_for_match(query_name)
    label = host.split(".", 1)[0]
    if compact and (compact == label or label == compact):
        return 100
    if compact and (compact.startswith(label) or label.startswith(compact)):
        return 80
    best = 0
    for token in _company_tokens(query_name):
        if len(token) < 4:
            continue
        if token == label:
            best = max(best, 95)
            continue
        if token in host or host.startswith(token + "."):
            best = max(best, 80)
    return best


def _candidate_score(query_name: str, candidate: dict[str, str | int]) -> int:
    name_score = _name_match_score(query_name, str(candidate.get("name", "")))
    domain_score = _domain_match_score(query_name, str(candidate.get("website", "")))
    base = max(name_score, domain_score)
    website = str(candidate.get("website", ""))
    phone = str(candidate.get("phone", ""))
    parsed = urlparse(website if "://" in website else f"https://{website}")
    host = (parsed.netloc or parsed.path).strip().lower()
    lower_url = website.lower()
    if any(host.endswith(suffix) for suffix in FOREIGN_TLDS):
        base -= 60
    if any(marker in lower_url for marker in FOREIGN_URL_MARKERS):
        base -= 40
    if any(phone.startswith(prefix) for prefix in FOREIGN_PHONE_PREFIXES):
        base -= 60 if domain_score < 80 else 40
    return base


def _local_name_score(value: str) -> int:
    text = _normalize_text(value)
    if not _contains_korean_text(text):
        return -10_000
    if len(text) < 2 or len(text) > 120:
        return -10_000
    if "http" in text.lower() or "www." in text.lower():
        return -10_000
    if any(phrase in text for phrase in BLOCKED_KOREAN_NAME_PHRASES):
        return -10_000
    if re.search(r"(입니다|해주세요|공유해\s*주세요)[.!?]?$", text):
        return -10_000
    if len(text) >= 30 and any(token in text for token in ("제공하는", "솔루션", "도움이 될", "후기", "위치한", "운영하고 있다", "발전소로")):
        return -10_000
    if "대한민국" in text and len(text) >= 25:
        return -10_000
    compact = re.sub(r"[^가-힣]+", "", text)
    if compact in GENERIC_KOREAN_NAME_TOKENS:
        return -10_000
    if compact.endswith(("시는", "군은", "구는")):
        return -10_000
    text = re.sub(r"\((소유자|사업주)\)", "", text).strip()
    score = 40
    if any(token in text for token in KOREAN_COMPANY_TOKENS):
        score += 50
    if any(text.endswith(token) for token in ("시", "군", "구")) and len(text) <= 4:
        score -= 80
    if any(token in text for token in KOREAN_ADDRESS_TOKENS) and re.search(r"\d{2,}", text):
        score -= 50
    if re.search(r"[A-Za-z]{4,}", text):
        score -= 20
    score += min(len(text), 50) // 5
    return score


def _extract_candidate_name(node: Any, query_name: str) -> str:
    best_name = ""
    best_score = -1
    for item in _flatten_strings(node):
        text = _normalize_text(item)
        if not text:
            continue
        if text.startswith(("http://", "https://", "www.", "tel:")):
            continue
        score = _name_match_score(query_name, text)
        if score > best_score:
            best_name = text
            best_score = score
    return best_name


def _extract_candidate_local_name(node: Any) -> str:
    best_name = ""
    best_score = -10_000
    for item in _flatten_strings(node):
        score = _local_name_score(item)
        if score > best_score:
            best_name = _normalize_text(item)
            best_score = score
    return best_name if best_score > 0 else ""


def _pick_best_website(
    query_name: str,
    candidates: list[dict[str, str | int]],
) -> str:
    if not candidates:
        return ""
    picked = _pick_best_candidate(candidates, query_name)
    if picked is None:
        return ""
    return picked["website"]


def _pick_best_candidate(
    candidates: list[dict[str, str | int]],
    query_name: str,
) -> dict[str, str | int] | None:
    if not candidates:
        return None
    scored: list[dict[str, str | int]] = []
    for candidate in candidates:
        score = _candidate_score(query_name, candidate)
        scored.append(
            {
                "name": str(candidate.get("name", "")),
                "company_name_local": str(candidate.get("company_name_local", "")),
                "website": str(candidate.get("website", "")),
                "phone": str(candidate.get("phone", "")),
                "score": score,
            }
        )
    scored.sort(key=lambda item: int(item["score"]), reverse=True)
    if int(scored[0]["score"]) < MIN_CANDIDATE_SCORE:
        return None
    return scored[0]

