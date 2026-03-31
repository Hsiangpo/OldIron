"""共享官网邮箱提取入口。

当前仓库的官网邮箱提取实现仍在 Denmark 目录。
这里做一层共享包装，供新国家统一引用。
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DENMARK_SRC = ROOT / "Denmark" / "src"
if str(DENMARK_SRC) not in sys.path:
    sys.path.insert(0, str(DENMARK_SRC))

from denmark_crawler.fc_email.email_service import EmailDiscoveryResult  # noqa: E402
from denmark_crawler.fc_email.email_service import FirecrawlEmailService  # noqa: E402
from denmark_crawler.fc_email.email_service import FirecrawlEmailSettings  # noqa: E402
from denmark_crawler.fc_email.llm_client import HtmlContactExtraction  # noqa: E402


__all__ = [
    "EmailDiscoveryResult",
    "FirecrawlEmailService",
    "FirecrawlEmailSettings",
    "HtmlContactExtraction",
]
