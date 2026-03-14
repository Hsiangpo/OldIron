"""数据模型定义。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict


@dataclass
class CompanyRecord:
    """单条公司记录 — 仅保留用户需要的 4 个核心字段。"""

    comp_id: str = ""           # 内部ID（不输出，仅用于关联）
    company_name: str = ""      # 公司名称
    ceo: str = ""               # 代表者/法人（只保留一个）
    homepage: str = ""          # 公司官网（可空）
    emails: list[str] = field(default_factory=list)  # 所有邮箱

    def to_output_dict(self) -> dict:
        """输出用字典（不含 comp_id）。"""
        return {
            "company_name": self.company_name,
            "ceo": self.ceo,
            "homepage": self.homepage,
            "emails": self.emails,
        }

    def to_json_line(self) -> str:
        """序列化为 JSONL 单行（含 comp_id 用于断点）。"""
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: dict) -> CompanyRecord:
        """从字典创建。"""
        return cls(
            comp_id=str(d.get("comp_id", "")),
            company_name=str(d.get("company_name", d.get("comp_name", ""))),
            ceo=str(d.get("ceo", d.get("ceo_name", ""))),
            homepage=str(d.get("homepage", "")),
            emails=list(d.get("emails", [])),
        )
