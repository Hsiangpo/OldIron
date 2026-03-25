"""bizmaps HTTP 客户端。

使用 curl_cffi 发起请求。
URL 结构: /s/prefs/{pref_code}?page=N  (pref_code = 01~47)
API 结构: /arearequest 返回 {pref_code: [cities]} 的地区目录
"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any

from curl_cffi.requests import Session

logger = logging.getLogger("bizmaps.client")

BASE_URL = "https://biz-maps.com"
PER_PAGE = 20  # 每页 20 家公司


class BizmapsClient:
    """biz-maps.com 协议爬虫客户端。"""

    def __init__(
        self,
        *,
        request_delay: float = 1.5,
        max_retries: int = 3,
        proxy: str | None = None,
    ) -> None:
        self._delay = request_delay
        self._max_retries = max_retries
        self._session = Session(impersonate="chrome120")
        self._consecutive_empty = 0

        # 代理配置（日本站点国内需走代理）
        proxy_url = proxy or os.getenv("HTTP_PROXY", "")
        if proxy_url:
            self._session.proxies = {"http": proxy_url, "https": proxy_url}
            logger.info("使用代理: %s", proxy_url)

        # 统一 headers — 与 DevTools 抓包一致
        self._session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": f"{BASE_URL}/",
        })

        # 统计
        self._request_count = 0
        self._error_count = 0

    def fetch_areas(self) -> list[dict[str, Any]]:
        """调 /arearequest 获取全日本 47 都道府县目录。

        返回按 pref_code 排序的列表:
          [{"id": 1, "pref_code": "01", "name": "北海道", "total": 12345}, ...]
        """
        url = f"{BASE_URL}/arearequest"
        resp = self._get_with_retry(url)
        if resp is None:
            return []
        try:
            data = resp.json()
        except Exception:
            logger.error("解析 /arearequest JSON 失败")
            return []

        areas: list[dict[str, Any]] = []
        # 返回格式: {"01": [cities...], "02": [cities...], ...}
        if isinstance(data, dict):
            for pref_code in sorted(data.keys()):
                cities = data[pref_code]
                if not isinstance(cities, list):
                    continue
                total = sum(c.get("total_companies", 0) for c in cities)
                # 提取都道府县名 — 从第一个市的名称推断
                pref_name = _infer_pref_name(pref_code, cities)
                areas.append({
                    "id": int(pref_code),
                    "pref_code": pref_code,
                    "name": pref_name,
                    "total": total,
                })
        return areas

    def fetch_list_page(self, pref_code: str, page: int = 1, ph: str = "") -> str | None:
        """获取指定都道府县指定页的列表页 HTML。

        Args:
            pref_code: 都道府県コード (01~47)
            page: 页码
            ph: 翻页签名 token（从上一页 HTML 中提取）
        """
        url = f"{BASE_URL}/s/prefs/{pref_code}"
        params: dict[str, str] = {}
        if page > 1:
            params["page"] = str(page)
            if ph:
                params["ph"] = ph

        resp = self._get_with_retry(url, params=params)
        if resp is None:
            return None

        html_text = resp.text
        if not html_text or len(html_text) < 500:
            self._consecutive_empty += 1
            logger.warning("页面过短 pref=%s page=%d len=%d", pref_code, page, len(html_text))
            if self._consecutive_empty >= 3:
                logger.error("连续 3 次空页面，停止当前地区")
                return None
        else:
            self._consecutive_empty = 0

        return html_text

    def _get_with_retry(self, url: str, params: dict[str, str] | None = None) -> Any:
        """带重试和限速的 GET 请求。"""
        for attempt in range(self._max_retries):
            try:
                self._polite_delay()
                resp = self._session.get(url, params=params, timeout=30)
                self._request_count += 1

                if resp.status_code == 200:
                    return resp

                if resp.status_code == 429:
                    wait = (2 ** attempt) * 5 + random.uniform(1, 3)
                    logger.warning("429 限流，等待 %.1fs 重试 (%d/%d)", wait, attempt + 1, self._max_retries)
                    time.sleep(wait)
                    continue

                if resp.status_code == 403:
                    self._error_count += 1
                    logger.error("403 禁止 url=%s", url)
                    return None

                if resp.status_code >= 500:
                    wait = (2 ** attempt) * 2 + random.uniform(0.5, 2)
                    logger.warning("%d 服务端错误，等待 %.1fs 重试", resp.status_code, wait)
                    time.sleep(wait)
                    continue

                logger.warning("HTTP %d url=%s", resp.status_code, url)
                return None

            except Exception as exc:
                self._error_count += 1
                wait = (2 ** attempt) * 2 + random.uniform(0.5, 2)
                logger.warning("请求异常: %s, 等待 %.1fs 重试 (%d/%d)", exc, wait, attempt + 1, self._max_retries)
                time.sleep(wait)

        logger.error("重试耗尽 url=%s", url)
        return None

    def _polite_delay(self) -> None:
        """请求间隔：基础延迟 + 随机抖动。"""
        jitter = random.uniform(0.3, 0.8)
        time.sleep(self._delay + jitter)

    @property
    def stats(self) -> dict[str, int]:
        return {"requests": self._request_count, "errors": self._error_count}


# ── 47 都道府県名映射 ──

_PREF_NAMES = {
    "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県",
    "05": "秋田県", "06": "山形県", "07": "福島県", "08": "茨城県",
    "09": "栃木県", "10": "群馬県", "11": "埼玉県", "12": "千葉県",
    "13": "東京都", "14": "神奈川県", "15": "新潟県", "16": "富山県",
    "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
    "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県",
    "25": "滋賀県", "26": "京都府", "27": "大阪府", "28": "兵庫県",
    "29": "奈良県", "30": "和歌山県", "31": "鳥取県", "32": "島根県",
    "33": "岡山県", "34": "広島県", "35": "山口県", "36": "徳島県",
    "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
    "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県",
    "45": "宮崎県", "46": "鹿児島県", "47": "沖縄県",
}


def _infer_pref_name(pref_code: str, cities: list[dict]) -> str:
    """从都道府县代码获取名称。"""
    return _PREF_NAMES.get(pref_code, f"都道府県{pref_code}")
