from __future__ import annotations

import re
from typing import Iterable

# 官方 47 都道府県 + 全国
_PREFECTURES_CANONICAL: tuple[str, ...] = (
    "全国",
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
)

# 生成别名：去除后缀、常见写法
_ALIASES: dict[str, str] = {}
for canon in _PREFECTURES_CANONICAL:
    _ALIASES[canon] = canon
    if canon not in ("全国", "東京", "東京都"):
        base = re.sub(r"[都道府県]$", "", canon)
        if base:
            _ALIASES[base] = canon

_ALIASES.update(
    {
        "日本": "全国",
        "Japan": "全国",
        "にほん": "全国",
        "にっぽん": "全国",
        "全域": "全国",
        "全国一": "全国",
        "全国版": "全国",
        "東京": "東京都",
        "大阪": "大阪府",
        "京都": "京都府",
        "札幌": "北海道",
    }
)


def normalize_prefecture(value: str | None) -> str | None:
    """
    将任意写法（如 大阪 / 大阪府 / Osaka Japan）归一到都道府県全名。
    返回 None 表示无法识别。
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    # 清理分隔符
    text = (
        text.replace("，", ",")
        .replace("、", ",")
        .replace("|", ",")
        .replace("／", "/")
        .replace("/", " ")
    )
    for token in ("日本", "Japan"):
        text = text.replace(token, " ")

    parts = [p.strip() for p in re.split(r"[\s,]+", text) if p.strip()]
    head = parts[0] if parts else text

    direct = _ALIASES.get(head)
    if direct:
        return direct

    head2 = re.sub(r"[都道府県]$", "", head)
    direct2 = _ALIASES.get(head2)
    if direct2:
        return direct2

    for key, canon in _ALIASES.items():
        if key and key in text:
            return canon
    return None


def is_supported_prefecture(value: str | None) -> bool:
    canonical = normalize_prefecture(value)
    return bool(canonical and canonical in _PREFECTURES_CANONICAL)


def list_supported_prefectures() -> list[str]:
    return list(_PREFECTURES_CANONICAL)


def infer_prefecture_from_city(
    city: str,
    *,
    nominatim_client,
    fallback_candidates: Iterable[str] | None = None,
) -> str | None:
    """
    仅有城市名时，尝试用 Nominatim geocode，返回规范化都道府県名。
    nominatim_client: callable(q: str) -> dict|None，返回 address 字典。
    """
    if not city or not isinstance(city, str):
        return None
    q = f"{city.strip()} 日本"
    data = nominatim_client(q)
    addr = data.get("address") if isinstance(data, dict) else None
    candidates = []
    for key in ("state", "region", "province"):
        val = addr.get(key) if isinstance(addr, dict) else None
        if isinstance(val, str) and val.strip():
            candidates.append(val.strip())
    if fallback_candidates:
        candidates.extend([c for c in fallback_candidates if isinstance(c, str)])
    for cand in candidates:
        canon = normalize_prefecture(cand)
        if canon:
            return canon
    return None
