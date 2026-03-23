"""Virk 数据模型。"""

from __future__ import annotations

import json
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field


@dataclass(slots=True)
class VirkSearchSegment:
    """搜索分段任务（Kommune × Virksomhedsform，粒度足够小保证 ≤3000 条）。"""

    kommune_kode: str
    kommune_navn: str
    virksomhedsform_kode: str = ""
    virksomhedsform_navn: str = ""


@dataclass(slots=True)
class VirkCompany:
    """从搜索 + 详情 API 合并出的公司记录。"""

    cvr: str
    company_name: str
    address: str = ""
    postcode: str = ""
    city: str = ""
    phone: str = ""
    email: str = ""
    industry_code: str = ""
    industry_name: str = ""
    company_type: str = ""
    status: str = ""
    start_date: str = ""
    kommune: str = ""
    # 详情 API 补充
    representative: str = ""
    representative_role: str = ""
    purpose: str = ""
    registered_capital: str = ""
    homepage: str = ""
    owners_json: str = "[]"
    # 来源标记
    source_segment: str = ""
    source_page: int = 0

    def emails(self) -> list[str]:
        value = str(self.email or "").strip().lower()
        return [value] if value else []

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
