"""韩国 DNB 全站行业目录。"""

from .industry_catalog import INDUSTRY_CATEGORY_COUNT
from .industry_catalog import INDUSTRY_PAGE_COUNT
from .industry_catalog import INDUSTRY_SUBCATEGORY_COUNT
from .industry_catalog import build_country_industry_segments
from .industry_catalog import list_industry_catalog

__all__ = [
    "INDUSTRY_CATEGORY_COUNT",
    "INDUSTRY_PAGE_COUNT",
    "INDUSTRY_SUBCATEGORY_COUNT",
    "build_country_industry_segments",
    "list_industry_catalog",
]
