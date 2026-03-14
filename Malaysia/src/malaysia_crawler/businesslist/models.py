"""BusinessList 数据模型。"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field


@dataclass(slots=True)
class BusinessListCompany:
    company_id: int
    company_url: str
    company_name: str
    registration_code: str
    address: str
    contact_numbers: list[str] = field(default_factory=list)
    website_href: str = ""
    website_url: str = ""
    contact_email: str = ""
    company_manager: str = ""
    employees: list[dict[str, str]] = field(default_factory=list)
