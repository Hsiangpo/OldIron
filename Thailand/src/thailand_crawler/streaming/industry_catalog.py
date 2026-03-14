"""D&B 全站 NAICS 行业目录。"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

from thailand_crawler.models import Segment


SLUG_PATTERN = re.compile(r"industry-analysis\.(.+)\.html$")


def _catalog_path() -> Path:
    return Path(__file__).with_name('industry_catalog.json')


def _extract_slug(href: str) -> str:
    matched = SLUG_PATTERN.search(str(href or '').strip())
    return str(matched.group(1) if matched else '').strip().lower()


@lru_cache(maxsize=1)
def load_industry_catalog() -> list[dict]:
    raw = json.loads(_catalog_path().read_text(encoding='utf-8'))
    return raw if isinstance(raw, list) else []


def iter_industry_slugs() -> list[str]:
    slugs: list[str] = []
    seen: set[str] = set()
    for category in load_industry_catalog():
        nodes = [category.get('top_level')] + list(category.get('subcategories', []))
        for node in nodes:
            if not isinstance(node, dict):
                continue
            slug = _extract_slug(str(node.get('href', '')))
            if not slug or slug in seen:
                continue
            seen.add(slug)
            slugs.append(slug)
    return slugs


def build_country_industry_segments(country_iso_two_code: str) -> list[Segment]:
    country = str(country_iso_two_code or '').strip().lower()
    return [
        Segment(
            industry_path=slug,
            country_iso_two_code=country,
            expected_count=0,
            segment_type='industry',
        )
        for slug in iter_industry_slugs()
    ]


INDUSTRY_CATEGORY_COUNT = len(load_industry_catalog())
INDUSTRY_SUBCATEGORY_COUNT = sum(
    len(category.get('subcategories', []) or [])
    for category in load_industry_catalog()
)
INDUSTRY_PAGE_COUNT = len(iter_industry_slugs())
