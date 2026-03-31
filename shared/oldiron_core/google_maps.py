"""共享 Google Maps 入口。

当前仓库的 Google Maps 协议实现仍在 Denmark 目录。
这里做一层共享包装，避免新国家直接写跨国家 import。
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DENMARK_SRC = ROOT / "Denmark" / "src"
if str(DENMARK_SRC) not in sys.path:
    sys.path.insert(0, str(DENMARK_SRC))

from denmark_crawler.google_maps import GoogleMapsClient  # noqa: E402
from denmark_crawler.google_maps import GoogleMapsConfig  # noqa: E402
from denmark_crawler.google_maps import GoogleMapsPlaceResult  # noqa: E402


__all__ = [
    "GoogleMapsClient",
    "GoogleMapsConfig",
    "GoogleMapsPlaceResult",
]
