"""Prompt 模板加载和渲染工具。

从外部文件加载 prompt 模板，支持变量替换。
"""
from __future__ import annotations

from pathlib import Path
from functools import lru_cache


_PROMPTS_DIR = Path(__file__).parent / "prompts"


@lru_cache(maxsize=32)
def load_prompt(name: str) -> str:
    """加载 prompt 模板文件。
    
    Args:
        name: 模板名称（不含扩展名），如 "verify_representative"
    
    Returns:
        模板内容字符串
    
    Raises:
        FileNotFoundError: 模板文件不存在
    """
    path = _PROMPTS_DIR / f"{name}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Prompt template not found: {path}")
    return path.read_text(encoding="utf-8")


def render_prompt(name: str, **kwargs) -> str:
    """加载并渲染 prompt 模板。
    
    Args:
        name: 模板名称
        **kwargs: 模板变量
    
    Returns:
        渲染后的 prompt 字符串
    """
    template = load_prompt(name)
    return template.format(**kwargs)


def get_verify_representative_prompt(
    context_block: str,
    payload: str,
) -> str:
    """获取代表人校验 prompt。"""
    return render_prompt(
        "verify_representative",
        context_block=context_block,
        payload=payload,
    )


def get_extract_company_info_prompt(
    context_block: str,
    payload: str,
) -> str:
    """获取公司信息抽取 prompt。"""
    return render_prompt(
        "extract_company_info",
        context_block=context_block,
        payload=payload,
    )


def get_select_links_prompt(
    website: str,
    missing_hint: str,
    context_block: str,
    max_select: int,
    links_payload: str,
) -> str:
    """获取链接选择 prompt。"""
    return render_prompt(
        "select_links",
        website=website,
        missing_hint=missing_hint,
        context_block=context_block,
        max_select=max_select,
        links_payload=links_payload,
    )


def get_check_keyword_prompt(
    context_block: str,
    keyword: str,
    payload: str,
) -> str:
    """获取关键词检查 prompt。"""
    return render_prompt(
        "check_keyword",
        context_block=context_block,
        keyword=keyword,
        payload=payload,
    )


def get_select_email_prompt(
    website: str,
    candidates: str,
) -> str:
    """获取邮箱筛选 prompt。"""
    return render_prompt(
        "select_email",
        website=website,
        candidates=candidates,
    )
