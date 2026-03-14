from __future__ import annotations

import asyncio
import re
import urllib.parse

_PB_LAT_LNG_PATTERN = re.compile(r"!2d-?\d+(?:\.\d+)?!3d-?\d+(?:\.\d+)?")


async def capture_tbm_map_pb(
    query: str,
    *,
    base_url: str = "https://www.google.com",
    hl: str = "ja",
    gl: str = "jp",
    center_lat: float = 35.681236,
    center_lng: float = 139.767125,
    zoom: int = 11,
    timeout_ms: int = 25000,
) -> str | None:
    """
    通过真实的 Google Maps 页面，抓取 `https://www.google.com/search?tbm=map`
    请求里的 `pb` 参数。

    返回值是“未 urlencode 的 pb 字符串”，可直接传给 gmap_agent 的 --search-pb。
    """
    q = (query or "").strip()
    if not q:
        return None

    try:
        from playwright.async_api import async_playwright  # type: ignore[import-not-found]
    except Exception:
        return None

    loop = asyncio.get_running_loop()
    pb_future: asyncio.Future[str] = loop.create_future()

    def on_request(request) -> None:  # type: ignore[no-untyped-def]
        url = request.url
        if "tbm=map" not in url:
            return
        try:
            parsed = urllib.parse.urlparse(url)
            params = urllib.parse.parse_qs(parsed.query)
            pb_list = params.get("pb")
            if not pb_list:
                return
            pb = urllib.parse.unquote(pb_list[0])
            if not _PB_LAT_LNG_PATTERN.search(pb):
                return
            if not pb_future.done():
                pb_future.set_result(pb)
        except Exception:
            return

    base = (base_url or "https://www.google.com").rstrip("/")
    maps_url = (
        f"{base}/maps/search/{urllib.parse.quote(q)}"
        f"/@{center_lat},{center_lng},{zoom}z?hl={urllib.parse.quote(hl)}&gl={urllib.parse.quote(gl)}"
    )

    timeout_s = max(1.0, timeout_ms / 1000.0)
    async with async_playwright() as pw:
        browser = None
        context = None
        try:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(locale=_derive_locale(hl, gl))
            page = await context.new_page()
            page.on("request", on_request)
            try:
                await page.goto(maps_url, wait_until="domcontentloaded", timeout=timeout_ms)
            except Exception:
                pass
            return await asyncio.wait_for(pb_future, timeout=timeout_s)
        except Exception:
            return None
        finally:
            if context:
                try:
                    await context.close()
                except Exception:
                    pass
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass


def _derive_locale(hl: str, gl: str) -> str:
    lang = (hl or "").strip().replace("_", "-")
    country = (gl or "").strip().upper()
    if not lang:
        return "ja-JP"
    if "-" in lang:
        return lang
    if len(lang) == 2 and len(country) == 2:
        return f"{lang}-{country}"
    return lang
