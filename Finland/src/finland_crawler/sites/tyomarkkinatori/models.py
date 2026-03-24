"""Työmarkkinatori 数据模型。

职位详情 API 直接返回结构化 recruiting 信息（邮箱/电话/联系人）。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass(slots=True)
class TmtJobPosting:
    """从搜索 + 详情 API 合并出的职位记录。"""

    job_id: str
    title: str = ""
    company_name: str = ""
    business_id: str = ""          # 芬兰 Y-tunnus，如 "2048364-4"
    address: str = ""
    postcode: str = ""
    city: str = ""
    region: str = ""
    # 联系人信息（从详情 API recruiting 字段提取）
    email: str = ""
    phone: str = ""
    representative: str = ""
    # 公司官网（从 externalLinks 提取）
    homepage: str = ""
    # 职位元数据
    work_time: str = ""            # FULLTIME / PARTTIME
    employment_type: str = ""      # EMPLOYMENT / INTERNSHIP
    duration: str = ""             # PERMANENT / TEMPORARY
    salary_info: str = ""
    industry_code: str = ""
    publish_date: str = ""
    end_date: str = ""
    application_url: str = ""
    description: str = ""
    # 来源
    source_page: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
