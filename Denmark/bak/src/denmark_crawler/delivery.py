"""Denmark 交付包装。"""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

import oldiron_core.delivery.engine as _shared_engine
from denmark_crawler.country_spec import DENMARK_DELIVERY_SPEC
from oldiron_core.delivery import build_delivery_bundle as _build_shared_delivery_bundle
from oldiron_core.delivery import parse_day_label


shutil = _shared_engine.shutil
_load_all_records = _shared_engine._load_all_records


def build_delivery_bundle(data_root: Path, delivery_root: Path, day_label: str) -> dict[str, object]:
    """构建 Denmark 日交付包。"""
    return _build_shared_delivery_bundle(
        data_root=data_root,
        delivery_root=delivery_root,
        day_label=day_label,
        spec=DENMARK_DELIVERY_SPEC,
    )
