from __future__ import annotations

import httpx


def build_httpx_client(default_headers: dict[str, str], proxy_url: str | None, timeout_seconds: float) -> httpx.Client:
    return httpx.Client(**build_httpx_client_kwargs(default_headers, proxy_url, timeout_seconds))


def build_httpx_client_kwargs(
    default_headers: dict[str, str],
    proxy_url: str | None,
    timeout_seconds: float,
) -> dict[str, object]:
    client_kwargs: dict[str, object] = {
        "follow_redirects": True,
        "headers": dict(default_headers),
        "timeout": timeout_seconds,
        "limits": httpx.Limits(max_connections=128, max_keepalive_connections=32, keepalive_expiry=30.0),
        "trust_env": False,
    }
    if proxy_url:
        client_kwargs["proxy"] = proxy_url
    return client_kwargs
