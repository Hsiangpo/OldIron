"""Cookie 加载与转换工具。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _parse_cookie_json(raw: str) -> dict[str, str]:
    data: Any = json.loads(raw)
    if isinstance(data, dict):
        return {str(k): str(v) for k, v in data.items() if v is not None}
    if isinstance(data, list):
        cookies: dict[str, str] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            value = item.get("value")
            if name and value is not None:
                cookies[str(name)] = str(value)
        return cookies
    raise ValueError("cookie JSON 格式不支持")


def _parse_netscape_cookie(raw: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) >= 7:
            name = fields[5].strip()
            value = fields[6].strip()
            if name:
                cookies[name] = value
    return cookies


def _parse_cookie_text(raw: str) -> dict[str, str]:
    raw = raw.strip()
    if not raw:
        return {}

    if "\t" in raw and ("\n" in raw or raw.startswith("#")):
        parsed = _parse_netscape_cookie(raw)
        if parsed:
            return parsed

    cookies: dict[str, str] = {}
    pairs = raw.split(";")
    for pair in pairs:
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            cookies[key] = value
    return cookies


def cookies_to_header(cookies: dict[str, str]) -> str:
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


def _load_browser_cookie_header(domain: str) -> str:
    try:
        import browser_cookie3  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "未安装 browser-cookie3，无法自动读取浏览器 Cookie。"
        ) from exc

    jar = browser_cookie3.chrome(domain_name=domain)
    cookies = {c.name: c.value for c in jar if c.name and c.value is not None}
    if not cookies:
        raise RuntimeError(f"未在浏览器中读取到 {domain} 的 Cookie。")
    return cookies_to_header(cookies)


def load_cookie_header(
    cookie_header: str | None,
    cookie_file: str | Path | None,
    *,
    domain: str,
    use_browser_cookie: bool,
) -> str:
    """统一加载 Cookie Header。"""
    if cookie_header:
        parsed = _parse_cookie_text(cookie_header)
        return cookies_to_header(parsed)

    if cookie_file:
        content = Path(cookie_file).read_text(encoding="utf-8")
        try:
            parsed = _parse_cookie_json(content)
        except json.JSONDecodeError:
            parsed = _parse_cookie_text(content)
        if not parsed:
            raise RuntimeError(f"Cookie 文件为空或格式不正确: {cookie_file}")
        return cookies_to_header(parsed)

    if use_browser_cookie:
        return _load_browser_cookie_header(domain)

    return ""

