"""规则和启发式方法抽取模块。

包含基于规则的公司信息抽取等不依赖 LLM 的逻辑。
"""
from __future__ import annotations

import re
from typing import Any


def clean_company_value(value: str) -> str | None:
    """清理公司名称候选值。
    
    移除多余空格和常见无关字符。
    
    Args:
        value: 原始候选值
    
    Returns:
        清理后的公司名，或 None 如果无效
    """
    if not isinstance(value, str):
        return None
    
    text = value.strip()
    if not text:
        return None
    
    # 移除多余空格
    text = re.sub(r"[\s　]+", " ", text).strip()
    
    # 公司名称不应太短或太长
    if len(text) < 2 or len(text) > 100:
        return None
    
    return text


def extract_from_structured_data(html: str, url: str) -> dict[str, Any]:
    """从结构化数据（JSON-LD、meta 标签等）中提取信息。
    
    Args:
        html: HTML 内容
        url: 页面 URL
    
    Returns:
        提取到的信息字典
    """
    info: dict[str, Any] = {}
    evidence: dict[str, dict] = {}
    
    # 尝试从 JSON-LD 中提取
    json_ld_info = _extract_from_json_ld(html)
    if json_ld_info:
        for key, value in json_ld_info.items():
            if value and key not in info:
                info[key] = value
                evidence[key] = {"url": url, "source": "json-ld"}
    
    # 尝试从 meta 标签中提取
    meta_info = _extract_from_meta(html)
    if meta_info:
        for key, value in meta_info.items():
            if value and key not in info:
                info[key] = value
                evidence[key] = {"url": url, "source": "meta"}
    
    if evidence:
        info["evidence"] = evidence
    
    return info


def _extract_from_json_ld(html: str) -> dict[str, Any]:
    """从 JSON-LD 中提取公司信息。"""
    import json
    import re
    
    info: dict[str, Any] = {}
    
    # 查找所有 JSON-LD 脚本
    pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
    
    for match in matches:
        try:
            data = json.loads(match)
            if isinstance(data, list):
                for item in data:
                    _extract_org_from_json_ld(item, info)
            elif isinstance(data, dict):
                _extract_org_from_json_ld(data, info)
        except (json.JSONDecodeError, TypeError):
            continue
    
    return info


def _extract_org_from_json_ld(data: dict, info: dict) -> None:
    """从单个 JSON-LD 对象中提取组织信息。"""
    if not isinstance(data, dict):
        return
    
    obj_type = data.get("@type", "")
    if isinstance(obj_type, list):
        obj_type = obj_type[0] if obj_type else ""
    
    # 检查是否为组织类型
    org_types = {"Organization", "Corporation", "LocalBusiness", "Company"}
    if obj_type in org_types or "Organization" in str(obj_type):
        name = data.get("name")
        if isinstance(name, str) and name.strip() and "company_name" not in info:
            cleaned = clean_company_value(name)
            if cleaned:
                info["company_name"] = cleaned
        


def _extract_from_meta(html: str) -> dict[str, Any]:
    """从 meta 标签中提取公司信息。"""
    import re
    
    info: dict[str, Any] = {}
    
    # 查找 og:site_name
    match = re.search(
        r'<meta[^>]*property=["\']og:site_name["\'][^>]*content=["\']([^"\']+)["\']',
        html, re.IGNORECASE
    )
    if match:
        name = match.group(1).strip()
        if name and "company_name" not in info:
            cleaned = clean_company_value(name)
            if cleaned:
                info["company_name"] = cleaned
    
    return info
