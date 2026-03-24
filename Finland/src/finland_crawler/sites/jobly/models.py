"""Jobly 数据模型。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass(slots=True)
class JoblyJob:
    """从 Jobly.fi 列表页+详情页解析出的职位记录。"""

    job_id: str
    url: str = ""
    title: str = ""
    company_name: str = ""
    city: str = ""
    email: str = ""
    phone: str = ""
    representative: str = ""
    homepage: str = ""
    description: str = ""
    source_page: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
