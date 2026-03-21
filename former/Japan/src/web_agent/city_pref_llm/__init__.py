from __future__ import annotations

from typing import Any

LLM_PROMPT = """你是一个地名规范化助手，输入是日本的城市/区/町/村名称，输出对应的都道府县全称。只返回都道府县名称，不要其他文字。
示例：札幌市->北海道 大阪市->大阪府 那覇市->沖縄県 東京都->東京都 渋谷区->東京都 福岡市->福岡県
"""

CITY_NORMALIZE_PROMPT = """你是日本市区町村名称规范化助手。输入可能是中文或日文的城市名，请输出日本官方市区町村名称（保留“市/区/町/村”后缀）。只返回规范化后的名称，不要解释。
示例：室兰市->室蘭市 钏路市->釧路市 函馆市->函館市 带广市->帯広市 陆奥市->むつ市 津轻市->つがる市 惠庭市->恵庭市
"""


def build_city2pref_messages(city: str) -> list[dict[str, Any]]:
    city = (city or "").strip()
    return [
        {"role": "system", "content": LLM_PROMPT},
        {"role": "user", "content": city},
    ]


def build_city_normalize_messages(city: str) -> list[dict[str, Any]]:
    city = (city or "").strip()
    return [
        {"role": "system", "content": CITY_NORMALIZE_PROMPT},
        {"role": "user", "content": city},
    ]
