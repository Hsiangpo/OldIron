"""Proff 数据模型。"""

from __future__ import annotations

import json
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field


@dataclass(slots=True)
class ProffSearchTask:
    """Proff 搜索页任务。"""

    query: str
    page: int
    retries: int = 0


@dataclass(slots=True)
class ProffCompany:
    """Proff 搜索结果中的公司记录。"""

    orgnr: str
    company_name: str
    representative: str = ""
    representative_role: str = ""
    address: str = ""
    homepage: str = ""
    email: str = ""
    phone: str = ""
    source_query: str = ""
    source_page: int = 0
    source_url: str = ""
    raw_payload: dict[str, object] = field(default_factory=dict)

    def emails(self) -> list[str]:
        value = str(self.email or "").strip().lower()
        return [value] if value else []

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
