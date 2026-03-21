"""数据模型定义。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict


@dataclass
class CompanyRecord:
    """单条公司记录。"""

    comp_id: str = ""           # 站点内唯一ID（用于断点/关联）
    company_name: str = ""      # 公司名称 (Nama Badan Usaha)
    ceo: str = ""               # 法人/负责人 (Nama Pimpinan)
    emails: list[str] = field(default_factory=list)  # 邮箱
    homepage: str = ""          # 官网（如有）
    detail_path: str = ""       # 详情页路径或URL
    address: str = ""           # 地址
    province: str = ""          # 省份
    city: str = ""              # 市/县
    registration_no: str = ""   # 注册号
    qualification: str = ""     # 资质等级

    def to_output_dict(self) -> dict:
        """输出用字典（交付字段）。"""
        return {
            "company_name": self.company_name,
            "ceo": self.ceo,
            "emails": "; ".join(self.emails) if self.emails else "",
            "homepage": self.homepage,
        }

    def to_json_line(self) -> str:
        """序列化为 JSONL 单行。"""
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: dict) -> CompanyRecord:
        """从字典创建。"""
        return cls(
            comp_id=str(d.get("comp_id", "")),
            company_name=str(d.get("company_name", "")),
            ceo=str(d.get("ceo", "")),
            emails=list(d.get("emails", [])),
            homepage=str(d.get("homepage", "")),
            detail_path=str(d.get("detail_path", "")),
            address=str(d.get("address", "")),
            province=str(d.get("province", "")),
            city=str(d.get("city", "")),
            registration_no=str(d.get("registration_no", "")),
            qualification=str(d.get("qualification", "")),
        )
