"""英国 DNB 客户端与解析逻辑。"""

from __future__ import annotations

import html
import json
import random
import re
import time
from typing import Any
from urllib.parse import quote

from curl_cffi import requests as cffi_requests

from england_crawler.dnb.browser_cookie import DnbCookieProvider
from england_crawler.dnb.models import CompanyRecord
from england_crawler.dnb.models import Segment
from england_crawler.snov.client import extract_domain


PLACEHOLDER_PATTERN = re.compile(r"^[?\s\t\r\n]+$")
BASE_URL = "https://www.dnb.com"
HTML_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
}
JSON_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "accept-language": "en-US,en;q=0.9",
}
RETRYABLE_TRANSPORT_PATTERNS = (
    r"curl: \((28|35|56|92)\)",
    r"HTTP/2 stream \d+ was not closed cleanly",
)


class RateLimitConfig:
    """DNB 请求限速配置。"""

    def __init__(
        self,
        min_delay: float = 0.4,
        max_delay: float = 1.0,
        long_rest_interval: int = 200,
        long_rest_seconds: float = 8.0,
    ) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.long_rest_interval = long_rest_interval
        self.long_rest_seconds = long_rest_seconds


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip()


def _clean_optional_text(value: str) -> str:
    cleaned = _clean_text(value)
    if cleaned.lower() in {"none", "null", "n/a", "na"}:
        return ""
    if not cleaned or PLACEHOLDER_PATTERN.fullmatch(cleaned):
        return ""
    return cleaned


def _normalize_website(raw_value: str) -> str:
    value = _clean_optional_text(raw_value)
    if not value:
        return ""
    if value.startswith("www."):
        return f"https://{value}"
    if value.startswith(("http://", "https://")):
        return value
    return f"https://{value}"


def _format_location(value: str, fallback: str) -> str:
    cleaned = _clean_optional_text(value)
    if cleaned:
        return cleaned.replace("\xa0", " ")
    return _clean_optional_text(fallback).title()


def _iter_cookie_pairs(cookie_header: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for raw_part in cookie_header.split(";"):
        part = raw_part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = name.strip()
        if not name:
            continue
        pairs.append((name, value.strip()))
    return pairs


def _sleep_retry_backoff(attempt: int, cap_seconds: float = 20.0) -> None:
    time.sleep(min((2**attempt) + random.uniform(0, 1.0), cap_seconds))


def _is_retryable_transport_error(message: str) -> bool:
    return bool(re.search("|".join(RETRYABLE_TRANSPORT_PATTERNS), message, flags=re.I))


def build_listing_payload(segment: Segment, page_number: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pageNumber": page_number,
        "industryPath": segment.industry_path,
        "countryIsoTwoCode": segment.country_iso_two_code,
    }
    if segment.region_name:
        payload["regionName"] = segment.region_name
    if segment.city_name:
        payload["cityName"] = segment.city_name
    return payload


def build_listing_page_path(segment: Segment) -> str:
    parts = [segment.industry_path, segment.country_iso_two_code]
    if segment.region_name:
        parts.append(segment.region_name)
    if segment.city_name:
        parts.append(segment.city_name)
    return "/business-directory/company-information.{}.html".format(".".join(parts))


def build_company_profile_api_params(company_name_url: str) -> dict[str, str]:
    path = f"{BASE_URL}//business-directory/company-profiles.{quote(company_name_url, safe='._-()')}"
    return {"path": path, "language": "en", "country": "us"}


def _segment_from_href(
    industry_path: str,
    href: str,
    expected_count: int,
) -> Segment | None:
    tokens = [token.strip().lower() for token in str(href).split(".") if token.strip()]
    if not tokens:
        return None
    country = tokens[0]
    region = tokens[1] if len(tokens) > 1 else ""
    city = tokens[2] if len(tokens) > 2 else ""
    segment_type = "city" if city else ("region" if region else "country")
    return Segment(
        industry_path=industry_path,
        country_iso_two_code=country,
        region_name=region,
        city_name=city,
        expected_count=expected_count,
        segment_type=segment_type,
    )


def extract_child_segments(
    *,
    industry_path: str,
    payload: dict[str, Any],
    country_iso_two_code: str,
) -> list[Segment]:
    """从 companyInformationGeos 提取地理下级切片。"""
    out: list[Segment] = []
    seen: set[str] = set()
    for geo in payload.get("companyInformationGeos", []) or []:
        href = str(geo.get("href", "")).strip()
        segment = _segment_from_href(
            industry_path=industry_path,
            href=href,
            expected_count=_parse_count(geo.get("quantity", 0)),
        )
        if segment is None or segment.country_iso_two_code != country_iso_two_code:
            continue
        if segment.segment_id in seen:
            continue
        seen.add(segment.segment_id)
        out.append(segment)
    return out


def extract_related_industry_segments(
    *,
    parent_segment: Segment,
    payload: dict[str, Any],
) -> list[Segment]:
    """提取英国 construction 根节点下可见的行业子切片。"""
    if parent_segment.industry_path != "construction":
        return []
    related = payload.get("relatedIndustries", {})
    if not isinstance(related, dict):
        return []
    out: list[Segment] = []
    seen: set[str] = set()
    for slug in related.values():
        industry_path = _clean_text(str(slug or "")).lower()
        if (
            not industry_path
            or industry_path == parent_segment.industry_path
            or industry_path in seen
        ):
            continue
        seen.add(industry_path)
        out.append(
            Segment(
                industry_path=industry_path,
                country_iso_two_code=parent_segment.country_iso_two_code,
                region_name=parent_segment.region_name,
                city_name=parent_segment.city_name,
                expected_count=0,
                segment_type=parent_segment.segment_type,
            )
        )
    return out


def parse_company_listing(payload: dict[str, Any]) -> list[CompanyRecord]:
    rows: list[CompanyRecord] = []
    for item in payload.get("companyInformationCompany", []) or []:
        if not isinstance(item, dict):
            continue
        address = item.get("primaryAddress", {})
        if not isinstance(address, dict):
            address = {}
        street_address = address.get("streetAddress", {})
        if not isinstance(street_address, dict):
            street_address = {}
        country = address.get("addressCountry", {})
        if not isinstance(country, dict):
            country = {}
        locality = address.get("addressLocality", {})
        if not isinstance(locality, dict):
            locality = {}
        region = address.get("addressRegion", {})
        if not isinstance(region, dict):
            region = {}
        rows.append(
            CompanyRecord(
                duns=_clean_text(str(item.get("duns", ""))),
                company_name_en_dnb=_clean_text(str(item.get("primaryName", ""))),
                company_name_url=_clean_text(str(item.get("companyNameUrl", ""))),
                address=_clean_text(str(street_address.get("line1", ""))),
                city=_format_location(
                    str(item.get("addressLocalityNameFormatted", "")),
                    str(locality.get("name", "")),
                ),
                region=_format_location(
                    str(item.get("addressRegionNameFormatted", "")),
                    str(region.get("name", "")),
                ),
                country=_clean_text(
                    str(country.get("countryName", item.get("addressCountryName", "")))
                ),
                postal_code=_clean_text(str(address.get("postalCode", ""))),
                sales_revenue=_clean_text(str(item.get("salesRevenue", ""))),
            )
        )
    return rows


def parse_company_profile(record: CompanyRecord, payload: dict[str, Any]) -> CompanyRecord:
    overview_raw = payload.get("overview", {}) if isinstance(payload, dict) else {}
    overview = overview_raw if isinstance(overview_raw, dict) else {}
    updated = CompanyRecord.from_dict(record.to_dict())
    updated.key_principal = _clean_optional_text(str(overview.get("keyPrincipal", "")))
    updated.phone = _clean_optional_text(str(overview.get("phone", "")))
    updated.dnb_website = _normalize_website(str(overview.get("website", "")))
    updated.website = updated.dnb_website
    updated.domain = extract_domain(updated.website)
    updated.company_name_resolved = updated.company_name_en_dnb
    return updated


def _parse_count(value: object) -> int:
    text = str(value or "").replace(",", "").strip()
    return int(text) if text.isdigit() else 0


class DnbClient:
    """DNB 协议客户端。"""

    def __init__(
        self,
        rate_config: RateLimitConfig | None = None,
        cookie_header: str = "",
        cookie_provider: DnbCookieProvider | None = None,
    ) -> None:
        self.rate_config = rate_config or RateLimitConfig()
        self.cookie_provider = cookie_provider
        self.session = self._build_session()
        self._request_count = 0
        self._warmed_paths: set[str] = set()
        self.cookie_header = cookie_header.strip()
        if self.cookie_provider and not self.cookie_header:
            self.cookie_header = self.cookie_provider.get(force_refresh=True).strip()
        self._has_seed_cookies = bool(_iter_cookie_pairs(self.cookie_header))
        self._seed_cookie_jar()

    def _build_session(self) -> cffi_requests.Session:
        session = cffi_requests.Session(impersonate="chrome110")
        session.trust_env = False
        return session

    def _refresh_cookie_header(self, *, force_refresh: bool) -> None:
        if not self.cookie_provider:
            return
        refreshed = self.cookie_provider.get(force_refresh=force_refresh).strip()
        if refreshed:
            self.cookie_header = refreshed
            self._has_seed_cookies = bool(_iter_cookie_pairs(self.cookie_header))

    def _reset_session(self, *, refresh_cookie: bool = False) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self._refresh_cookie_header(force_refresh=refresh_cookie)
        self.session = self._build_session()
        self._seed_cookie_jar()
        self._warmed_paths.clear()

    def _seed_cookie_jar(self) -> None:
        if not self.cookie_header:
            return
        self.session.headers.pop("Cookie", None)
        for name, value in _iter_cookie_pairs(self.cookie_header):
            self.session.cookies.set(name, value)

    def _build_transport_failure_message(self, url: str) -> str:
        message = f"D&B 请求失败: {url}"
        if self._has_seed_cookies:
            return (
                f"{message} | 当前会话可能已过期或被上游重置，"
                "请优先刷新 9222 浏览器中的 DNB cookies（DNB_COOKIE_HEADER）。"
            )
        return message

    def _sleep(self) -> None:
        delay = random.uniform(self.rate_config.min_delay, self.rate_config.max_delay)
        time.sleep(delay)
        self._request_count += 1
        if self._request_count % self.rate_config.long_rest_interval == 0:
            time.sleep(self.rate_config.long_rest_seconds)

    def _warm_page(self, path: str) -> None:
        if path in self._warmed_paths:
            return
        if self._has_seed_cookies:
            self._warmed_paths.add(path)
            return
        url = f"{BASE_URL}{path}"
        for attempt in range(1, 7):
            self._sleep()
            try:
                response = self.session.get(url, headers=HTML_HEADERS, timeout=30)
            except Exception as exc:
                if attempt == 6:
                    raise RuntimeError(f"D&B 页面预热失败: {url}") from exc
                self._reset_session(refresh_cookie=True)
                _sleep_retry_backoff(attempt)
                continue
            if response.status_code in {403, 429}:
                if attempt == 6:
                    raise RuntimeError(f"D&B 页面预热返回 {response.status_code}: {url}")
                self._reset_session(refresh_cookie=True)
                _sleep_retry_backoff(attempt)
                continue
            if response.status_code >= 500:
                if attempt == 6:
                    raise RuntimeError(f"D&B 页面预热服务端错误 {response.status_code}: {url}")
                _sleep_retry_backoff(attempt)
                continue
            response.raise_for_status()
            self._warmed_paths.add(path)
            return

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        headers: dict[str, str],
        referer_path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        max_retries: int = 6,
    ) -> dict[str, Any]:
        self._warm_page(referer_path)
        url = f"{BASE_URL}{path}"
        request_headers = {
            **headers,
            "referer": f"{BASE_URL}{referer_path}",
            "origin": BASE_URL,
        }
        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                if method == "POST":
                    response = self.session.post(
                        url,
                        headers=request_headers,
                        json=json_body,
                        timeout=30,
                    )
                else:
                    response = self.session.get(
                        url,
                        headers=request_headers,
                        params=params,
                        timeout=30,
                    )
            except Exception as exc:
                err_text = str(exc)
                if attempt == max_retries:
                    if _is_retryable_transport_error(err_text):
                        raise RuntimeError(self._build_transport_failure_message(url)) from exc
                    raise RuntimeError(f"D&B 请求失败: {url}") from exc
                self._reset_session(refresh_cookie=True)
                if _is_retryable_transport_error(err_text):
                    _sleep_retry_backoff(attempt, 30.0)
                else:
                    _sleep_retry_backoff(attempt, 20.0)
                self._warm_page(referer_path)
                continue
            if response.status_code in {403, 429}:
                if attempt == max_retries:
                    raise RuntimeError(f"D&B 返回 {response.status_code}: {url}")
                self._reset_session(refresh_cookie=True)
                self._warm_page(referer_path)
                _sleep_retry_backoff(attempt, 20.0)
                continue
            if response.status_code >= 500:
                if attempt == max_retries:
                    raise RuntimeError(f"D&B 服务端错误 {response.status_code}: {url}")
                _sleep_retry_backoff(attempt, 20.0)
                continue
            response.raise_for_status()
            return json.loads(response.text)
        raise RuntimeError(f"D&B 请求失败: {url}")

    def fetch_company_listing_page(
        self,
        segment: Segment,
        page_number: int = 1,
    ) -> dict[str, Any]:
        referer_path = build_listing_page_path(segment)
        return self._request_json(
            method="POST",
            path="/business-directory/api/companyinformation",
            headers=JSON_HEADERS,
            referer_path=referer_path,
            json_body=build_listing_payload(segment, page_number),
        )

    def fetch_company_profile(self, company_name_url: str) -> dict[str, Any]:
        encoded_company_name_url = quote(company_name_url, safe="._-()")
        referer_path = f"/business-directory/company-profiles.{encoded_company_name_url}.html"
        return self._request_json(
            method="GET",
            path="/business-directory/api/companyprofile",
            headers={
                "accept": "application/json, text/plain, */*",
                "accept-language": "en-US,en;q=0.9",
            },
            referer_path=referer_path,
            params=build_company_profile_api_params(company_name_url),
        )
