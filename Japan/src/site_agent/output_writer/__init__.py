from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable

from ..models import ExtractionResult


def ensure_run_dir(base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def write_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_json(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, records: Iterable[ExtractionResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records_list = list(records)
    if not records_list:
        return
    # 内部字段名到中文表头的映射
    field_to_header = {
        "input_name": "输入名称",
        "website": "网站",
        "company_name": "公司名称",
        "representative": "代表人",
        "capital": "注册资金",
        "employees": "公司人数",
        "phone": "座机",
        "email": "邮箱",
        "emails": "邮箱列表",
        "email_count": "邮箱数量",
        "company_name_source_url": "公司名称来源",
        "representative_source_url": "代表人来源",
        "capital_source_url": "注册资金来源",
        "employees_source_url": "公司人数来源",
        "phone_source_url": "座机来源",
        "email_source_url": "邮箱来源",
        "notes": "备注",
        "status": "状态",
        "error": "错误信息",
        "extracted_at": "提取时间",
    }
    fieldnames = list(field_to_header.keys())
    headers = list(field_to_header.values())
    write_header = not path.exists()
    with path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(headers)
        for record in records_list:
            emails = ""
            if isinstance(record.emails, list):
                emails = ";".join([e for e in record.emails if isinstance(e, str) and e.strip()])
            writer.writerow([
                record.input_name,
                record.website,
                record.company_name,
                record.representative,
                record.capital,
                record.employees,
                record.phone,
                record.email,
                emails,
                record.email_count,
                record.company_name_source_url,
                record.representative_source_url,
                record.capital_source_url,
                record.employees_source_url,
                record.phone_source_url,
                record.email_source_url,
                record.notes,
                record.status,
                record.error,
                record.extracted_at,
            ])


def write_checkpoint(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)
