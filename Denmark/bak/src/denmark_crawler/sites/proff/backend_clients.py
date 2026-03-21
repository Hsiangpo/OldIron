"""Proff 调 Go 后端的轻量客户端。"""

from __future__ import annotations

from dataclasses import dataclass

import requests

from denmark_crawler.fc_email.email_service import EmailDiscoveryResult
from denmark_crawler.google_maps import GoogleMapsPlaceResult


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
    """Firecrawl Go 服务客户端。"""

    def __init__(self, base_url: str, *, timeout_seconds: float = 120.0) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_seconds = max(float(timeout_seconds), 10.0)
        self._session = requests.Session()

    def health(self) -> GoBackendHealth:
        response = self._session.get(f"{self.base_url}/healthz", timeout=min(self.timeout_seconds, 5.0))
        response.raise_for_status()
        payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        return GoBackendHealth(ok=str(payload.get("status", "")) == "ok", service=str(payload.get("service", "")))

    def discover_emails(self, *, company_name: str, homepage: str, domain: str = "") -> EmailDiscoveryResult:
        response = self._session.post(
            f"{self.base_url}/v1/discover-emails",
            json={"company_name": company_name, "homepage": homepage, "domain": domain},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return EmailDiscoveryResult(
            emails=[str(item).strip().lower() for item in payload.get("emails", []) if str(item).strip()],
            evidence_url=str(payload.get("evidence_url", "")).strip(),
            evidence_quote=str(payload.get("evidence_quote", "")).strip(),
            contact_form_only=bool(payload.get("contact_form_only")),
            selected_urls=[str(item).strip() for item in payload.get("selected_urls", []) if str(item).strip()],
            retry_after_seconds=float(payload.get("retry_after_seconds", 0.0) or 0.0),
        )
