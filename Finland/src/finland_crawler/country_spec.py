"""Finland 交付规格。"""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SHARED_ROOT = PROJECT_ROOT / "shared"
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from oldiron_core.delivery import DeliverySpec


FINLAND_DELIVERY_SPEC = DeliverySpec(country_name="Finland")
