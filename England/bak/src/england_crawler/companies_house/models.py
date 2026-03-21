"""Companies House 站点数据模型。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CompanyTask:
    comp_id: str
    company_name: str
    company_number: str
    homepage: str
    domain: str
    retries: int
