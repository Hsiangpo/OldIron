"""D&B 客户端与解析逻辑。"""

from __future__ import annotations

import html
import json
import random
import re
import time
from dataclasses import dataclass
from typing import Any

from curl_cffi import requests as cffi_requests

from thailand_crawler.models import CompanyRecord
from thailand_crawler.models import Segment
from thailand_crawler.snov import extract_domain


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


@dataclass(slots=True)
class RateLimitConfig:
    min_delay: float = 0.4
    max_delay: float = 1.0
    long_rest_interval: int = 200
    long_rest_seconds: float = 8.0


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip()


def _clean_optional_text(value: str) -> str:
    cleaned = _clean_text(value)
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
    cleaned = _clean_text(value)
    if cleaned:
        return cleaned.replace("\xa0", " ")
    return _clean_text(fallback).title()


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
    path = f"https://www.dnb.com//business-directory/company-profiles.{company_name_url}"
    return {"path": path, "language": "en", "country": "us"}


def parse_company_listing(payload: dict[str, Any]) -> list[CompanyRecord]:
    rows: list[CompanyRecord] = []
    for item in payload.get("companyInformationCompany", []):
        if not isinstance(item, dict):
            continue
        address = item.get("primaryAddress", {})
        street_address = address.get("streetAddress", {}) if isinstance(address, dict) else {}
        country = address.get("addressCountry", {}) if isinstance(address, dict) else {}
        locality = address.get("addressLocality", {}) if isinstance(address, dict) else {}
        region = address.get("addressRegion", {}) if isinstance(address, dict) else {}
        rows.append(
            CompanyRecord(
                duns=_clean_text(str(item.get("duns", ""))),
                company_name=_clean_text(str(item.get("primaryName", ""))),
                company_name_url=_clean_text(str(item.get("companyNameUrl", ""))),
                address=_clean_text(str(street_address.get("line1", ""))),
                city=_format_location(str(item.get("addressLocalityNameFormatted", "")), str(locality.get("name", ""))),
                region=_format_location(str(item.get("addressRegionNameFormatted", "")), str(region.get("name", ""))),
                country=_clean_text(str(country.get("countryName", item.get("addressCountryName", "")))),
                postal_code=_clean_text(str(address.get("postalCode", ""))),
                sales_revenue=_clean_text(str(item.get("salesRevenue", ""))),
            )
        )
    return rows


def parse_company_profile(record: CompanyRecord, payload: dict[str, Any]) -> CompanyRecord:
    overview_raw = payload.get("overview", {}) if isinstance(payload, dict) else {}
    overview = overview_raw if isinstance(overview_raw, dict) else {}
    website = _normalize_website(str(overview.get("website", "")))
    phone = _clean_optional_text(str(overview.get("phone", "")))
    key_principal = _clean_optional_text(str(overview.get("keyPrincipal", "")))
    updated = CompanyRecord.from_dict(record.to_dict())
    updated.website = website
    updated.domain = extract_domain(website)
    updated.phone = phone
    updated.key_principal = key_principal
    updated.trade_style_name = _clean_optional_text(str(overview.get("tradeStyleName", "")))
    updated.formatted_revenue = _clean_optional_text(str(overview.get("formattedRevenue", "")))
    return updated


class DnbClient:
    """D&B 协议客户端。"""

    def __init__(self, rate_config: RateLimitConfig | None = None, cookie_header: str = "") -> None:
        self.rate_config = rate_config or RateLimitConfig()
        self.session = self._build_session()
        self._request_count = 0
        self._warmed_paths: set[str] = set()
        self.cookie_header = cookie_header.strip()
        if self.cookie_header:
            self.session.headers["Cookie"] = self.cookie_header

    def _build_session(self) -> cffi_requests.Session:
        session = cffi_requests.Session(impersonate="chrome110")
        session.trust_env = False
        return session

    def _reset_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._build_session()
        self._warmed_paths.clear()

    def _sleep(self) -> None:
        delay = random.uniform(self.rate_config.min_delay, self.rate_config.max_delay)
        time.sleep(delay)
        self._request_count += 1
        if self._request_count % self.rate_config.long_rest_interval == 0:
            time.sleep(self.rate_config.long_rest_seconds)

    def _warm_page(self, path: str) -> None:
        if path in self._warmed_paths:
            return
        if self.cookie_header:
            self._warmed_paths.add(path)
            return
        self._sleep()
        response = self.session.get(f"{BASE_URL}{path}", headers=HTML_HEADERS, timeout=30)
        response.raise_for_status()
        self._warmed_paths.add(path)

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        headers: dict[str, str],
        referer_path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        max_retries: int = 4,
    ) -> dict[str, Any]:
        self._warm_page(referer_path)
        url = f"{BASE_URL}{path}"
        request_headers = {**headers, "referer": f"{BASE_URL}{referer_path}", "origin": BASE_URL}
        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                if method == "POST":
                    response = self.session.post(url, headers=request_headers, json=json_body, timeout=30)
                else:
                    response = self.session.get(url, headers=request_headers, params=params, timeout=30)
            except Exception as exc:
                if attempt == max_retries:
                    raise RuntimeError(f"D&B 请求失败: {url}") from exc
                self._reset_session()
                self._warm_page(referer_path)
                time.sleep(min((2**attempt) + random.uniform(0, 1.0), 20))
                continue
            if response.status_code in {403, 429}:
                if attempt == max_retries:
                    raise RuntimeError(f"D&B 返回 {response.status_code}: {url}")
                self._reset_session()
                self._warm_page(referer_path)
                time.sleep(min((2**attempt) + random.uniform(0, 1.0), 20))
                continue
            if response.status_code >= 500:
                if attempt == max_retries:
                    raise RuntimeError(f"D&B 服务端错误 {response.status_code}: {url}")
                time.sleep(min((2**attempt) + random.uniform(0, 1.0), 20))
                continue
            response.raise_for_status()
            return json.loads(response.text)
        raise RuntimeError(f"D&B 请求失败: {url}")

    def fetch_company_listing_page(self, segment: Segment, page_number: int = 1) -> dict[str, Any]:
        referer_path = build_listing_page_path(segment)
        return self._request_json(
            method="POST",
            path="/business-directory/api/companyinformation",
            headers=JSON_HEADERS,
            referer_path=referer_path,
            json_body=build_listing_payload(segment, page_number),
        )

    def fetch_company_profile(self, company_name_url: str) -> dict[str, Any]:
        referer_path = f"/business-directory/company-profiles.{company_name_url}.html"
        return self._request_json(
            method="GET",
            path="/business-directory/api/companyprofile",
            headers={"accept": "application/json, text/plain, */*", "accept-language": "en-US,en;q=0.9"},
            referer_path=referer_path,
            params=build_company_profile_api_params(company_name_url),
        )
