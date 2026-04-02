"""Proff 调 Go 后端的轻量客户端。"""

from __future__ import annotations

from dataclasses import dataclass

import requests

from oldiron_core.fc_email.client import HtmlPageResult
from oldiron_core.google_maps import GoogleMapsPlaceResult


@dataclass(slots=True)
class GoBackendHealth:
    ok: bool
    service: str = ""


class GoGMapClient:
    """GMap Go 服务客户端。"""

    def __init__(self, base_url: str, *, timeout_seconds: float = 30.0) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_seconds = max(float(timeout_seconds), 5.0)
        self._session = requests.Session()

    def health(self) -> GoBackendHealth:
        response = self._session.get(f"{self.base_url}/healthz", timeout=min(self.timeout_seconds, 5.0))
        response.raise_for_status()
        payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        return GoBackendHealth(ok=str(payload.get("status", "")) == "ok", service=str(payload.get("service", "")))

    def search_company_profile(self, query: str, company_name: str = "") -> GoogleMapsPlaceResult:
        response = self._session.post(
            f"{self.base_url}/v1/search/company-profile",
            json={"query": query, "company_name": company_name},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return GoogleMapsPlaceResult(
            company_name=str(payload.get("company_name", "")).strip(),
            phone=str(payload.get("phone", "")).strip(),
            website=str(payload.get("website", "")).strip(),
            score=int(payload.get("score", 0) or 0),
        )


class GoFirecrawlService:
    """Firecrawl Go 传输客户端。"""

    def __init__(self, base_url: str, *, timeout_seconds: float = 120.0) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_seconds = max(float(timeout_seconds), 10.0)
        self._session = requests.Session()

    def health(self) -> GoBackendHealth:
        response = self._session.get(f"{self.base_url}/healthz", timeout=min(self.timeout_seconds, 5.0))
        response.raise_for_status()
        payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        return GoBackendHealth(ok=str(payload.get("status", "")) == "ok", service=str(payload.get("service", "")))

    def map_site(self, *, homepage: str, domain: str = "", limit: int = 200, include_subdomains: bool = False) -> list[str]:
        response = self._session.post(
            f"{self.base_url}/v1/map-site",
            json={
                "homepage": homepage,
                "domain": domain,
                "limit": limit,
                "include_subdomains": include_subdomains,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return [str(item).strip() for item in payload.get("links", []) if str(item).strip()]

    def scrape_html_pages(self, urls: list[str]) -> list[HtmlPageResult]:
        response = self._session.post(
            f"{self.base_url}/v1/scrape-html-pages",
            json={"urls": urls},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        pages: list[HtmlPageResult] = []
        for item in payload.get("pages", []) or []:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url", "")).strip()
            html = str(item.get("html", "")).strip()
            if url and html:
                pages.append(HtmlPageResult(url=url, html=html))
        return pages
