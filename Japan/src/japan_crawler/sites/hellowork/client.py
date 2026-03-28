"""hellowork HTTP 客户端。

使用 curl_cffi 发起请求，模拟浏览器 TLS 指纹。
搜索 = POST GECA110010.do  /  详情 = GET dispDetailBtn
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from typing import Any

from curl_cffi.requests import Session

logger = logging.getLogger("hellowork.client")

BASE_URL = "https://www.hellowork.mhlw.go.jp/kensaku"
SEARCH_ENDPOINT = f"{BASE_URL}/GECA110010.do"


class HelloworkClient:
    """hellowork.mhlw.go.jp 协议爬虫客户端。"""

    _COMMON_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Origin": "https://www.hellowork.mhlw.go.jp",
        "Referer": f"{SEARCH_ENDPOINT}?action=initDisp&screenId=GECA110010",
    }

    def __init__(
        self,
        *,
        request_delay: float = 0.3,
        max_retries: int = 3,
        proxy: str | None = None,
    ) -> None:
        self._delay = request_delay
        self._max_retries = max_retries
        self._proxy_url = proxy or os.getenv("HTTP_PROXY", "")
        self._local = threading.local()

        # 主 session（用于搜索/翻页，单线程）
        self._session = self._make_session()
        if self._proxy_url:
            logger.info("使用代理: %s", self._proxy_url)

        # 统计
        self._request_count = 0
        self._error_count = 0
        self._stats_lock = threading.Lock()

    def _make_session(self) -> Session:
        """创建一个新的 curl_cffi Session（线程安全：每线程独立）。"""
        sess = Session(impersonate="chrome120")
        if self._proxy_url:
            sess.proxies = {"http": self._proxy_url, "https": self._proxy_url}
        sess.headers.update(self._COMMON_HEADERS)
        return sess

    def _thread_session(self) -> Session:
        """获取当前线程的独立 Session（用于详情页并发）。"""
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = self._make_session()
            self._local.session = sess
        return sess

    def init_session(self) -> bool:
        """访问搜索首页以获取 JSESSIONID cookie。"""
        url = f"{SEARCH_ENDPOINT}?action=initDisp&screenId=GECA110010"
        resp = self._get_with_retry(url)
        if resp and resp.status_code == 200:
            logger.info("Session 初始化成功")
            return True
        logger.error("Session 初始化失败")
        return False

    def search(self, pref_code: str, page: int = 1, per_page: int = 30,
               total_count: int = 0) -> str | None:
        """按都道府県搜索，返回结果页 HTML。

        Args:
            pref_code: 都道府県コード (01~47)
            page: 页码（1-based）
            per_page: 每页件数 (10/30/50)
            total_count: 上一次搜索返回的总件数（翻页时必传）
        """
        data = self._build_search_form(pref_code, page, per_page, total_count)
        resp = self._post_with_retry(SEARCH_ENDPOINT, data=data)
        if resp is None:
            return None
        return resp.text

    def fetch_detail(self, detail_url: str) -> str | None:
        """GET 请求获取详情页 HTML（线程安全，每线程独立 session）。"""
        if not detail_url.startswith("http"):
            detail_url = f"{BASE_URL}/{detail_url.lstrip('./')}"
        resp = self._get_with_retry(detail_url, use_thread_session=True)
        if resp is None:
            return None
        return resp.text

    def _build_search_form(self, pref_code: str, page: int, per_page: int,
                           total_count: int) -> dict[str, str]:
        """构建搜索 POST 表单数据。"""
        form: dict[str, str] = {
            "screenId": "GECA110010",
            "action": "",
            "kjKbnRadioBtn": "1",
            "todohukenHidden": pref_code,
            "ensenHidden": "",
            "roudousijyoHidden": "",
            "freeWordInput": "",
            "nOTKNSKFreeWordInput": "",
            "kSNoJo": "",
            "kSNoGe": "",
            "iNFTeikyoRiyoDantaiID": "",
            "searchClear": "0",
            "kiboSuruSKSU1Hidden": "",
            "kiboSuruSKSU2Hidden": "",
            "kiboSuruSKSU3Hidden": "",
            "hiddenViewedKyujinList": "",
            "CHECKEDKJNOLIST": "",
            "preCheckFlg": "false",
            "codeAssistType": "",
            "codeAssistKind": "",
            "codeAssistCode": "",
            "codeAssistItemCode": "",
            "codeAssistItemName": "",
            "codeAssistDivide": "",
            "maba_vrbs": (
                "infTkRiyoDantaiBtn,searchShosaiBtn,searchBtn,searchNoBtn,"
                "searchClearBtn,searchNoClearBtn,searchNoClearBtn_mobile,"
                "dispDetailBtn,kyujinhyoBtn,checkedKyujinViewBtn,"
                "checkedKyujinhyoIppanBtn,checkedKyujinhyoDsBtn,changeSearchCond"
            ),
        }

        if page == 1:
            # 首次搜索
            form["searchBtn"] = " 検索する"
            form["kyujinkensu"] = "0"
            form["summaryDisp"] = "false"
            form["searchInitDisp"] = "0"
        else:
            # 翻页：用 fwListNaviBtnNext 触发下一页
            form["fwListNaviBtnNext"] = "次へ＞"
            form["fwListNowPage"] = str(page - 1)  # 当前所在页（翻页前的页）
            form["fwListLeftPage"] = str(max(1, page - 4))
            form["fwListNaviCount"] = "7"
            form["fwListNaviDisp"] = str(per_page)
            form["fwListNaviSort"] = "1"
            form["fwListNaviSortTop"] = "1"
            form["fwListNaviDispTop"] = str(per_page)
            form["fwListNaviSortBtm"] = "1"
            form["fwListNaviDispBtm"] = str(per_page)
            form["kyujinkensu"] = str(total_count)
            form["summaryDisp"] = "true"
            form["searchInitDisp"] = "1"

        return form

    # ── 请求执行 ──

    def _post_with_retry(self, url: str, data: dict[str, str]) -> Any:
        """带重试和限速的 POST 请求（用主 session）。"""
        for attempt in range(self._max_retries):
            try:
                self._polite_delay()
                resp = self._session.post(url, data=data, timeout=30)
                with self._stats_lock:
                    self._request_count += 1

                if resp.status_code == 200:
                    if "システムの混雑" in resp.text:
                        wait = (2 ** attempt) * 5 + random.uniform(2, 5)
                        logger.warning("系统繁忙，等待 %.1fs 重试 (%d/%d)", wait, attempt + 1, self._max_retries)
                        time.sleep(wait)
                        continue
                    return resp

                if resp.status_code == 429:
                    wait = (2 ** attempt) * 5 + random.uniform(1, 3)
                    logger.warning("429 限流，等待 %.1fs 重试 (%d/%d)", wait, attempt + 1, self._max_retries)
                    time.sleep(wait)
                    continue

                if resp.status_code == 403:
                    with self._stats_lock:
                        self._error_count += 1
                    logger.error("403 禁止 url=%s — 立即停止", url)
                    return None

                if resp.status_code >= 500:
                    wait = (2 ** attempt) * 3 + random.uniform(1, 3)
                    logger.warning("%d 服务端错误，等待 %.1fs 重试", resp.status_code, wait)
                    time.sleep(wait)
                    continue

                logger.warning("HTTP %d url=%s", resp.status_code, url)
                return None

            except Exception as exc:
                with self._stats_lock:
                    self._error_count += 1
                wait = (2 ** attempt) * 2 + random.uniform(0.5, 2)
                logger.warning("请求异常: %s, 等待 %.1fs 重试 (%d/%d)", exc, wait, attempt + 1, self._max_retries)
                time.sleep(wait)

        logger.error("重试耗尽 url=%s", url)
        return None

    def _get_with_retry(self, url: str, use_thread_session: bool = False) -> Any:
        """带重试和限速的 GET 请求。"""
        sess = self._thread_session() if use_thread_session else self._session
        for attempt in range(self._max_retries):
            try:
                self._polite_delay()
                resp = sess.get(url, timeout=30)
                with self._stats_lock:
                    self._request_count += 1

                if resp.status_code == 200:
                    if "システムの混雑" in resp.text:
                        wait = (2 ** attempt) * 5 + random.uniform(2, 5)
                        logger.warning("系统繁忙，等待 %.1fs 重试 (%d/%d)", wait, attempt + 1, self._max_retries)
                        time.sleep(wait)
                        continue
                    return resp

                if resp.status_code == 429:
                    wait = (2 ** attempt) * 5 + random.uniform(1, 3)
                    logger.warning("429 限流，等待 %.1fs 重试", wait)
                    time.sleep(wait)
                    continue

                if resp.status_code == 403:
                    with self._stats_lock:
                        self._error_count += 1
                    logger.error("403 禁止 url=%s", url)
                    return None

                if resp.status_code >= 500:
                    wait = (2 ** attempt) * 3 + random.uniform(1, 3)
                    logger.warning("%d 服务端错误，等待 %.1fs 重试", resp.status_code, wait)
                    time.sleep(wait)
                    continue

                logger.warning("HTTP %d url=%s", resp.status_code, url)
                return None

            except Exception as exc:
                with self._stats_lock:
                    self._error_count += 1
                wait = (2 ** attempt) * 2 + random.uniform(0.5, 2)
                logger.warning("请求异常: %s, 重试 (%d/%d)", exc, attempt + 1, self._max_retries)
                time.sleep(wait)

        logger.error("重试耗尽 url=%s", url)
        return None

    def _polite_delay(self) -> None:
        """请求间隔：基础延迟 + 随机抖动。"""
        jitter = random.uniform(0.1, 0.3)
        time.sleep(self._delay + jitter)

    @property
    def stats(self) -> dict[str, int]:
        return {"requests": self._request_count, "errors": self._error_count}


# ── 47 都道府県名映射 ──

PREF_NAMES = {
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
