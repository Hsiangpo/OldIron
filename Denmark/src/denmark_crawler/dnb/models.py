"""丹麦 DNB 数据模型。"""

from __future__ import annotations

import json
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field


@dataclass(slots=True)
class Segment:
    """稳定抓取切片。"""

    industry_path: str
    country_iso_two_code: str
    region_name: str = ""
    city_name: str = ""
    expected_count: int = 0
    segment_type: str = "country"

    @property
    def segment_id(self) -> str:
        return "|".join(
            [
                self.industry_path.strip(),
                self.country_iso_two_code.strip(),
                self.region_name.strip(),
                self.city_name.strip(),
            ]
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, payload: dict) -> "Segment":
        return cls(
            industry_path=str(payload.get("industry_path", "")).strip(),
            country_iso_two_code=str(payload.get("country_iso_two_code", "")).strip(),
            region_name=str(payload.get("region_name", "")).strip(),
            city_name=str(payload.get("city_name", "")).strip(),
            expected_count=int(payload.get("expected_count", 0) or 0),
            segment_type=str(payload.get("segment_type", "country")).strip() or "country",
        )


@dataclass(slots=True)
class CompanyRecord:
    """丹麦 DNB 公司记录。"""

    duns: str = ""
    company_name_en_dnb: str = ""
    company_name_url: str = ""
    address: str = ""
    region: str = ""
    city: str = ""
    country: str = ""
    postal_code: str = ""
    sales_revenue: str = ""
    key_principal: str = ""
    dnb_website: str = ""
    website: str = ""
    domain: str = ""
    website_source: str = ""
    company_name_en_gmap: str = ""
    company_name_en_site: str = ""
    company_name_resolved: str = ""
    site_evidence_url: str = ""
    site_evidence_quote: str = ""
    site_confidence: float = 0.0
    phone: str = ""
    emails: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def to_delivery_dict(self) -> dict:
        """导出为丹麦统一交付兼容格式。"""
        return {
            "duns": self.duns,
            "company_name": self.company_name_resolved or self.company_name_en_dnb,
            "ceo": self.key_principal,
            "homepage": self.website or self.dnb_website,
            "emails": self.emails,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "CompanyRecord":
        emails = payload.get("emails", [])
        if not isinstance(emails, list):
            emails = [str(emails).strip()] if str(emails).strip() else []
        return cls(
            duns=str(payload.get("duns", "")).strip(),
            company_name_en_dnb=str(
                payload.get("company_name_en_dnb", payload.get("company_name", ""))
            ).strip(),
            company_name_url=str(payload.get("company_name_url", "")).strip(),
            address=str(payload.get("address", "")).strip(),
            region=str(payload.get("region", "")).strip(),
            city=str(payload.get("city", "")).strip(),
            country=str(payload.get("country", "")).strip(),
            postal_code=str(payload.get("postal_code", "")).strip(),
            sales_revenue=str(payload.get("sales_revenue", "")).strip(),
            key_principal=str(
                payload.get("key_principal", payload.get("ceo", ""))
            ).strip(),
            dnb_website=str(payload.get("dnb_website", payload.get("homepage", ""))).strip(),
            website=str(payload.get("website", payload.get("homepage", ""))).strip(),
            domain=str(payload.get("domain", "")).strip(),
            website_source=str(payload.get("website_source", "")).strip(),
            company_name_en_gmap=str(payload.get("company_name_en_gmap", "")).strip(),
            company_name_en_site=str(payload.get("company_name_en_site", "")).strip(),
            company_name_resolved=str(
                payload.get("company_name_resolved", payload.get("company_name", ""))
            ).strip(),
            site_evidence_url=str(payload.get("site_evidence_url", "")).strip(),
            site_evidence_quote=str(payload.get("site_evidence_quote", "")).strip(),
            site_confidence=float(payload.get("site_confidence", 0.0) or 0.0),
            phone=str(payload.get("phone", "")).strip(),
            emails=[str(item).strip() for item in emails if str(item).strip()],
        )

