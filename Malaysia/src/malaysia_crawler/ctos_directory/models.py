"""CTOS 公共目录数据模型。"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field


@dataclass(slots=True)
class CTOSCompanyItem:
    company_name: str
    registration_no: str
    detail_path: str
    detail_url: str


@dataclass(slots=True)
class CTOSDirectoryPage:
    prefix: str
    current_page: int
    next_page: int | None
    companies: list[CTOSCompanyItem] = field(default_factory=list)


@dataclass(slots=True)
class CTOSCompanyDetail:
    detail_url: str
    company_name: str
    company_registration_no: str
    new_registration_no: str
    nature_of_business: str
    date_of_registration: str
    state: str

