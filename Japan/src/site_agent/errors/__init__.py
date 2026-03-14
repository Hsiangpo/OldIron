"""统一的错误类型定义。

提供层次化的异常类型，便于统一处理和日志记录。
"""
from __future__ import annotations


class SiteAgentError(Exception):
    """站点代理基础异常类。"""
    
    def __init__(self, message: str, website: str | None = None, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.website = website
        self.details = details or {}
    
    def __str__(self) -> str:
        if self.website:
            return f"[{self.website}] {self.message}"
        return self.message


class NetworkError(SiteAgentError):
    """网络相关错误（连接超时、DNS 解析失败等）。"""
    pass


class CrawlError(SiteAgentError):
    """页面抓取错误（403、404、反爬等）。"""
    
    def __init__(self, message: str, website: str | None = None, 
                 status_code: int | None = None, url: str | None = None):
        super().__init__(message, website)
        self.status_code = status_code
        self.url = url


class LLMError(SiteAgentError):
    """LLM 调用相关错误（限流、超时、格式解析失败等）。"""
    
    def __init__(self, message: str, website: str | None = None,
                 label: str | None = None, is_retryable: bool = True):
        super().__init__(message, website)
        self.label = label  # 调用类型：选链、抽取、校验等
        self.is_retryable = is_retryable


class ParseError(SiteAgentError):
    """解析错误（HTML、JSON、PDF 等解析失败）。"""
    
    def __init__(self, message: str, website: str | None = None,
                 content_type: str | None = None):
        super().__init__(message, website)
        self.content_type = content_type  # html, json, pdf, etc.


class SnovError(SiteAgentError):
    """Snov 邮箱服务相关错误。"""
    pass


class SnovMaskedEmailError(SnovError):
    """Snov 返回脱敏邮箱（需要刷新 cookie 后重试）。"""
    pass


class SnovRateLimitError(SnovError):
    """Snov API 限流。"""
    pass


class ValidationError(SiteAgentError):
    """数据验证错误。"""
    pass


class ConfigError(SiteAgentError):
    """配置错误（缺少必要配置项等）。"""
    pass
