"""韩国 DNB 全站 NAICS 行业目录加载。"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

from korea_crawler.dnb.models import Segment


SLUG_PATTERN = re.compile(r"/business-directory/industry-analysis\.(.+)\.html$")


def _catalog_path() -> Path:
    return Path(__file__).with_name("industry_catalog.json")


def _slug_from_href(href: str) -> str:
    matched = SLUG_PATTERN.search(str(href or "").strip())
    return matched.group(1).strip().lower() if matched else ""


@lru_cache(maxsize=1)
def list_industry_catalog() -> list[dict[str, object]]:
    raw = json.loads(_catalog_path().read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def build_country_industry_segments(country_iso_two_code: str) -> list[Segment]:
    """构建国家级全站行业种子，覆盖大类页与小类页。"""
    country = str(country_iso_two_code or "").strip().lower()
    seen: set[str] = set()
    segments: list[Segment] = []
    for category in list_industry_catalog():
        top_level = category.get("top_level", {})
        if isinstance(top_level, dict):
            top_slug = _slug_from_href(str(top_level.get("href", "")))
            if top_slug and top_slug not in seen:
                seen.add(top_slug)
                segments.append(
                    Segment(
                        industry_path=top_slug,
                        country_iso_two_code=country,
                        expected_count=0,
                        segment_type="industry",
                    )
                )
        for child in category.get("subcategories", []) or []:
            if not isinstance(child, dict):
                continue
            slug = _slug_from_href(str(child.get("href", "")))
            if not slug or slug in seen:
                continue
            seen.add(slug)
            segments.append(
                Segment(
                    industry_path=slug,
                    country_iso_two_code=country,
                    expected_count=0,
                    segment_type="industry",
                )
            )
    return segments


INDUSTRY_CATEGORY_COUNT = len(list_industry_catalog())
INDUSTRY_SUBCATEGORY_COUNT = sum(
    len(category.get("subcategories", []) or [])
    for category in list_industry_catalog()
)
INDUSTRY_PAGE_COUNT = len(build_country_industry_segments("kr"))
