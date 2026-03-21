"""Companies House 输入源读取。"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from england_crawler.companies_house.client import normalize_company_name
from england_crawler.companies_house.input_xlsx import iter_company_names_from_xlsx


HEADER_NAMES = {"COMPANYNAME", "COMPANY NAME"}


def iter_company_names_from_text(path: str | Path, limit: int = 0) -> Iterator[str]:
    """按行读取公司名，自动跳过空行、表头与标准化重复项。"""
    source = Path(path)
    seen: set[str] = set()
    yielded = 0
    for raw_line in source.read_text(encoding="utf-8-sig").splitlines():
        raw_name = str(raw_line or "").strip()
        if not raw_name:
            continue
        normalized = normalize_company_name(raw_name)
        if not normalized or normalized in HEADER_NAMES or normalized in seen:
            continue
        seen.add(normalized)
        yield raw_name
        yielded += 1
        if limit > 0 and yielded >= limit:
            return


def iter_company_names_from_source(path: str | Path, limit: int = 0) -> Iterator[str]:
    """根据文件类型自动读取公司名。"""
    source = Path(path)
    suffix = source.suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        yield from iter_company_names_from_xlsx(source, limit=limit)
        return
    yield from iter_company_names_from_text(source, limit=limit)


def load_company_names_from_source(path: str | Path, limit: int = 0) -> list[str]:
    """一次性加载公司名列表。"""
    return list(iter_company_names_from_source(path, limit=limit))
