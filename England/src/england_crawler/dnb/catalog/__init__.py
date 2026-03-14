"""英国 DNB 行业目录。"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from england_crawler.dnb.models import Segment


def _catalog_path() -> Path:
    return Path(__file__).with_name("naics_catalog.json")


def _slug_from_href(href: str) -> str:
    marker = "/business-directory/industry-analysis."
    value = str(href or "").strip()
    if marker not in value or not value.endswith(".html"):
        return ""
    return value.split(marker, 1)[1][:-5].strip().lower()


@lru_cache(maxsize=1)
def load_naics_catalog() -> list[dict[str, object]]:
    """加载本地 NAICS 行业目录快照。"""
    raw = json.loads(_catalog_path().read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else []


def list_industry_paths() -> list[str]:
    """返回大类与小类的完整行业路径列表。"""
    seen: set[str] = set()
    result: list[str] = []
    for category in load_naics_catalog():
        top_level = category.get("top_level", {})
        if isinstance(top_level, dict):
            slug = _slug_from_href(str(top_level.get("href", "")))
            if slug and slug not in seen:
                seen.add(slug)
                result.append(slug)
        subcategories = category.get("subcategories", [])
        if not isinstance(subcategories, list):
            continue
        for item in subcategories:
            if not isinstance(item, dict):
                continue
            slug = _slug_from_href(str(item.get("href", "")))
            if slug and slug not in seen:
                seen.add(slug)
                result.append(slug)
    return result


def build_industry_seed_segments(country_iso_two_code: str) -> list[Segment]:
    """构建指定国家的全站行业种子。"""
    country = str(country_iso_two_code or "").strip().lower()
    return [
        Segment(
            industry_path=industry_path,
            country_iso_two_code=country,
            expected_count=0,
            segment_type="industry",
        )
        for industry_path in list_industry_paths()
    ]


INDUSTRY_CATEGORY_COUNT = len(load_naics_catalog())
INDUSTRY_SUBCATEGORY_COUNT = sum(
    len(category.get("subcategories", []) or [])
    for category in load_naics_catalog()
)
INDUSTRY_PAGE_COUNT = len(list_industry_paths())
