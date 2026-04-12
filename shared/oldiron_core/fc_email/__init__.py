"""共享官网邮箱提取模块。

这里避免包初始化时就提前导入 curl_cffi / asyncio 相关模块。
这样像 delivery 这类只依赖 ``normalization`` 子模块的流程，
不会被无关的重量级依赖拖入当前运行环境问题。
"""

from __future__ import annotations

from typing import Any


_EXPORT_MAP = {
    "EmailDiscoveryResult": (".email_service", "EmailDiscoveryResult"),
    "FirecrawlClient": (".client", "FirecrawlClient"),
    "FirecrawlClientConfig": (".client", "FirecrawlClientConfig"),
    "FirecrawlDomainCache": (".domain_cache", "FirecrawlDomainCache"),
    "FirecrawlEmailService": (".email_service", "FirecrawlEmailService"),
    "FirecrawlEmailSettings": (".email_service", "FirecrawlEmailSettings"),
    "FirecrawlError": (".client", "FirecrawlError"),
    "HtmlContactExtraction": (".llm_client", "HtmlContactExtraction"),
    "HtmlPageResult": (".client", "HtmlPageResult"),
}

__all__ = [
    "EmailDiscoveryResult",
    "FirecrawlClient",
    "FirecrawlClientConfig",
    "FirecrawlDomainCache",
    "FirecrawlEmailService",
    "FirecrawlEmailSettings",
    "FirecrawlError",
    "HtmlContactExtraction",
    "HtmlPageResult",
]


def __getattr__(name: str) -> Any:
    target = _EXPORT_MAP.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    module = __import__(f"{__name__}{module_name}", fromlist=[attr_name])
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
