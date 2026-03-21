"""丹麦 Virk 数据模型。"""

from __future__ import annotations

import json
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field


@dataclass(slots=True)
class VirkSearchCompany:
    """Virk 搜索结果中的公司行。"""

    cvr: str
    company_name: str
    address: str = ""
    city: str = ""
    postal_code: str = ""
    status: str = ""
    company_form: str = ""
    main_industry: str = ""
    start_date: str = ""
    phone: str = ""
    emails: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class VirkCompanyRecord:
    """Virk 详情合并后的公司记录。"""

    cvr: str
    company_name: str
    address: str = ""
    city: str = ""
    postal_code: str = ""
    country: str = "Denmark"
    status: str = ""
    company_form: str = ""
    main_industry: str = ""
    start_date: str = ""
    phone: str = ""
    emails: list[str] = field(default_factory=list)
    representative: str = ""
    representative_role: str = ""
    legal_owner: str = ""
    website: str = ""
    domain: str = ""
    website_source: str = ""
    gmap_company_name: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

