from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List


Cookie = Dict[str, str]


def load_cookies(path: str) -> List[Cookie]:
    file_path = Path(path)
    if not file_path.exists():
        return []
    data = json.loads(file_path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "cookies" in data:
        data = data["cookies"]
    if not isinstance(data, list):
        return []
    cookies: List[Cookie] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if "name" in item and "value" in item:
            cookies.append({
                "name": str(item.get("name")),
                "value": str(item.get("value")),
                "domain": str(item.get("domain", "")),
                "path": str(item.get("path", "/")),
                "secure": bool(item.get("secure", False)),
            })
    return cookies


def cookies_to_dict(cookies: Iterable[Cookie]) -> Dict[str, str]:
    return {c["name"]: c["value"] for c in cookies if c.get("name")}
