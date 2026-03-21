"""England 交付规格。"""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery import DeliverySpec
from oldiron_core.delivery import extract_domain


FOREIGN_TLDS = (".hk", ".com.hk", ".in", ".my", ".cn", ".sg")
FOREIGN_URL_MARKERS = ("hong-kong", "hongkong", "/locations/cn/", "/hk/", ".hk/")


def looks_suspicious_uk_record(record: dict[str, object]) -> bool:
    """过滤明显不是英国主体的官网错配。"""
    homepage = str(record.get("homepage", "")).strip()
    domain = extract_domain(homepage)
    lower_homepage = homepage.lower()
    if domain and any(domain.endswith(suffix) for suffix in FOREIGN_TLDS):
        return True
    return any(marker in lower_homepage for marker in FOREIGN_URL_MARKERS)


ENGLAND_DELIVERY_SPEC = DeliverySpec(
    country_name="England",
    suspicious_filter=looks_suspicious_uk_record,
)

