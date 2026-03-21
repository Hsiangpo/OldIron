"""缓存管理模块。

管理域名邮箱缓存等持久化缓存。
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any
from datetime import datetime


class DomainEmailCache:
    """域名邮箱缓存管理器。
    
    缓存已查询过的域名邮箱结果，避免重复查询。
    使用 JSONL 格式持久化存储。
    """
    
    def __init__(self, cache_path: Path | str | None = None):
        """初始化缓存管理器。
        
        Args:
            cache_path: 缓存文件路径，默认为 output/cache/domain_email_cache.jsonl
        """
        if cache_path is None:
            cache_path = Path("output") / "cache" / "domain_email_cache.jsonl"
        self._path = Path(cache_path)
        self._cache: dict[str, list[str]] | None = None
        self._lock = threading.Lock()
    
    def _load(self) -> dict[str, list[str]]:
        """加载缓存文件。"""
        if self._cache is not None:
            return self._cache
        
        cache: dict[str, list[str]] = {}
        if self._path.exists():
            try:
                with self._path.open("r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(obj, dict):
                            continue
                        domain = obj.get("domain")
                        emails = obj.get("emails")
                        if isinstance(domain, str) and domain.strip() and isinstance(emails, list):
                            cleaned = [e.strip() for e in emails if isinstance(e, str) and e.strip()]
                            if cleaned:
                                cache[domain.strip().lower()] = cleaned
            except Exception:
                cache = {}
        
        self._cache = cache
        return cache
    
    def get(self, domain: str) -> list[str] | None:
        """获取域名的缓存邮箱列表。
        
        Args:
            domain: 域名
        
        Returns:
            邮箱列表，如果未缓存则返回 None
        """
        if not isinstance(domain, str) or not domain.strip():
            return None
        cache = self._load()
        return cache.get(domain.strip().lower())
    
    def store(self, domain: str, emails: list[str]) -> None:
        """存储域名的邮箱列表到缓存。
        
        Args:
            domain: 域名
            emails: 邮箱列表
        """
        if not (isinstance(domain, str) and domain.strip()):
            return
        cleaned = [e.strip() for e in emails if isinstance(e, str) and e.strip()]
        if not cleaned:
            return
        
        domain_key = domain.strip().lower()
        cache = self._load()
        
        with self._lock:
            if domain_key in cache:
                return  # 已存在，不覆盖
            
            cache[domain_key] = cleaned
            
            # 追加写入文件
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "domain": domain_key,
                            "emails": cleaned,
                            "updated_at": datetime.utcnow().isoformat() + "Z",
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
    
    def clear(self) -> None:
        """清空缓存（仅内存，不删除文件）。"""
        with self._lock:
            self._cache = None
    
    def __contains__(self, domain: str) -> bool:
        """检查域名是否在缓存中。"""
        return self.get(domain) is not None


# Backward-compatible alias for legacy cache dict references.
_DOMAIN_EMAIL_CACHE: dict[str, list[str]] | None = None

# 全局单例
_domain_email_cache: DomainEmailCache | None = None


def get_domain_email_cache() -> DomainEmailCache:
    """获取全局域名邮箱缓存实例。"""
    global _domain_email_cache
    if _domain_email_cache is None:
        _domain_email_cache = DomainEmailCache()
    return _domain_email_cache


def get_cached_domain_emails(domain: str) -> list[str] | None:
    """获取缓存的域名邮箱（便捷函数）。"""
    return get_domain_email_cache().get(domain)


def store_domain_emails(domain: str, emails: list[str]) -> None:
    """存储域名邮箱到缓存（便捷函数）。"""
    get_domain_email_cache().store(domain, emails)
