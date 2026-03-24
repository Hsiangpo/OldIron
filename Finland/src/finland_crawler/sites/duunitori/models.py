"""Duunitori 数据模型。

SSR 站点，需 HTML 解析提取数据。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass(slots=True)
class DuunitoriJob:
    """从列表页 + 详情页解析出的职位记录。"""

    job_id: str                    # URL slug 作为唯一标识
    url: str = ""
    title: str = ""
    company_name: str = ""
    city: str = ""
    salary: str = ""
    publish_date: str = ""
    end_date: str = ""
    # 从详情页提取
    email: str = ""
    phone: str = ""
    representative: str = ""
    homepage: str = ""
    description: str = ""
    # 来源
    source_page: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
