"""数据模型定义。"""

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
    def from_dict(cls, payload: dict) -> Segment:
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
    """公司主记录。"""

    duns: str = ""
    company_name: str = ""
    company_name_url: str = ""
    address: str = ""
    region: str = ""
    city: str = ""
    country: str = ""
    postal_code: str = ""
    sales_revenue: str = ""
    website: str = ""
    domain: str = ""
    key_principal: str = ""
    phone: str = ""
    trade_style_name: str = ""
    formatted_revenue: str = ""
    emails: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, payload: dict) -> CompanyRecord:
        emails = payload.get("emails", [])
        if not isinstance(emails, list):
            emails = [str(emails).strip()] if str(emails).strip() else []
        return cls(
            duns=str(payload.get("duns", "")).strip(),
            company_name=str(payload.get("company_name", "")).strip(),
            company_name_url=str(payload.get("company_name_url", "")).strip(),
            address=str(payload.get("address", "")).strip(),
            region=str(payload.get("region", "")).strip(),
            city=str(payload.get("city", "")).strip(),
            country=str(payload.get("country", "")).strip(),
            postal_code=str(payload.get("postal_code", "")).strip(),
            sales_revenue=str(payload.get("sales_revenue", "")).strip(),
            website=str(payload.get("website", "")).strip(),
            domain=str(payload.get("domain", "")).strip(),
            key_principal=str(payload.get("key_principal", "")).strip(),
            phone=str(payload.get("phone", "")).strip(),
            trade_style_name=str(payload.get("trade_style_name", "")).strip(),
            formatted_revenue=str(payload.get("formatted_revenue", "")).strip(),
            emails=[str(item).strip() for item in emails if str(item).strip()],
        )

