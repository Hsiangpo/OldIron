"""DART Open API 客户端 — 支持多 Key 轮询、限额跟踪和错误重试。"""

from __future__ import annotations

import io
import json
import logging
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from xml.etree import ElementTree

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

# DART 官方限制
DAILY_LIMIT_PER_KEY = 20_000
# 请求间隔（秒），避免瞬时并发
REQUEST_DELAY = 0.3
# 错误重试
MAX_RETRIES = 3
RETRY_DELAY = 5.0

# DART API 错误码
DART_STATUS_OK = "000"
DART_STATUS_NO_DATA = "013"
DART_STATUS_RATE_LIMIT = "020"
DART_STATUS_INVALID_KEY = "010"
DART_STATUS_DISABLED_KEY = "011"


class DartRateLimitError(Exception):
    """DART API 请求超限（错误码 020）。"""
    pass


class DartKeyExhaustedError(Exception):
    """所有 DART API Key 均已用尽当日配额。"""
    pass


@dataclass
class DartKeyState:
    """单个 DART API Key 的状态。"""

    key: str
    used_today: int = 0
    exhausted: bool = False
    last_error: str = ""


@dataclass
class DartKeyPool:
    """DART API Key 轮询池 — 自动切换 Key，跟踪用量。"""

    keys: list[DartKeyState] = field(default_factory=list)
    _current_index: int = 0

    @classmethod
    def from_env_keys(cls, raw_keys: list[str]) -> DartKeyPool:
        """从环境变量中的 Key 列表创建 Pool。"""
        valid = [k.strip() for k in raw_keys if k.strip()]
        if not valid:
            raise ValueError("没有可用的 DART API Key")
        pool = cls(keys=[DartKeyState(key=k) for k in valid])
        logger.info("DART Key Pool: %d 个 Key，日总配额 %d 次",
                     len(pool.keys), len(pool.keys) * DAILY_LIMIT_PER_KEY)
        return pool

    def get_key(self) -> DartKeyState:
        """获取下一个可用的 Key，轮询分配负载。"""
        start = self._current_index
        checked = 0
        while checked < len(self.keys):
            idx = (start + checked) % len(self.keys)
            ks = self.keys[idx]
            if not ks.exhausted:
                self._current_index = (idx + 1) % len(self.keys)
                return ks
            checked += 1
        raise DartKeyExhaustedError(
            f"所有 {len(self.keys)} 个 DART Key 已用尽今日配额 "
            f"(每个 Key {DAILY_LIMIT_PER_KEY} 次)"
        )

    def mark_used(self, ks: DartKeyState) -> None:
        """标记 Key 已使用一次，检查是否耗尽。"""
        ks.used_today += 1
        if ks.used_today >= DAILY_LIMIT_PER_KEY:
            ks.exhausted = True
            logger.warning("DART Key %s...%s 已达日限额 %d",
                           ks.key[:4], ks.key[-4:], DAILY_LIMIT_PER_KEY)

    def mark_exhausted(self, ks: DartKeyState, reason: str) -> None:
        """手动标记 Key 已耗尽（如收到 020 错误码）。"""
        ks.exhausted = True
        ks.last_error = reason
        logger.warning("DART Key %s...%s 标记耗尽: %s",
                       ks.key[:4], ks.key[-4:], reason)

    def total_remaining(self) -> int:
        """估算所有 Key 剩余配额总和。"""
        return sum(
            max(0, DAILY_LIMIT_PER_KEY - ks.used_today)
            for ks in self.keys if not ks.exhausted
        )

    def summary(self) -> str:
        """返回各 Key 使用摘要。"""
        lines = []
        for i, ks in enumerate(self.keys, 1):
            status = "耗尽" if ks.exhausted else "可用"
            lines.append(f"  Key{i} ({ks.key[:4]}...{ks.key[-4:]}): "
                         f"{ks.used_today}/{DAILY_LIMIT_PER_KEY} [{status}]")
        lines.append(f"  剩余总配额: ~{self.total_remaining()}")
        return "\n".join(lines)


class DartClient:
    """DART Open API 客户端 — 封装 corpCode 下载和企业概况查询。"""

    BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(self, key_pool: DartKeyPool) -> None:
        self.key_pool = key_pool
        self.session = cffi_requests.Session(impersonate="chrome110")

    def _get_with_key(self, url: str, params: dict | None = None) -> cffi_requests.Response:
        """带 Key 轮询和重试的 GET 请求。"""
        params = params or {}
        last_exc = None

        for attempt in range(MAX_RETRIES):
            try:
                ks = self.key_pool.get_key()
            except DartKeyExhaustedError:
                raise

            params["crtfc_key"] = ks.key

            try:
                resp = self.session.get(url, params=params, timeout=30)
            except Exception as exc:
                logger.warning("DART 请求失败 (尝试%d): %s", attempt + 1, exc)
                last_exc = exc
                time.sleep(RETRY_DELAY)
                continue

            self.key_pool.mark_used(ks)

            # 检查返回是否为 JSON（非 ZIP 响应）
            content_type = resp.headers.get("Content-Type", "")
            if "json" in content_type or "xml" in content_type:
                try:
                    data = resp.json()
                    status = data.get("status", "")
                    if status == DART_STATUS_RATE_LIMIT:
                        self.key_pool.mark_exhausted(ks, "收到 020 错误码")
                        logger.warning("DART Key %s...%s 收到限额错误，切换下一个 Key",
                                       ks.key[:4], ks.key[-4:])
                        continue
                    if status in (DART_STATUS_INVALID_KEY, DART_STATUS_DISABLED_KEY):
                        self.key_pool.mark_exhausted(ks, f"Key 无效/停用 ({status})")
                        continue
                except (json.JSONDecodeError, ValueError):
                    pass

            time.sleep(REQUEST_DELAY)
            return resp

        if last_exc:
            raise last_exc
        raise RuntimeError("DART 请求重试已耗尽")

    def download_corp_codes(self, output_path: Path) -> list[dict]:
        """
        下载全量公司编号 XML。

        返回 [{corp_code, corp_name, stock_code, modify_date}, ...]
        """
        url = f"{self.BASE_URL}/corpCode.xml"
        logger.info("正在下载 DART 全量 corpCode...")

        ks = self.key_pool.get_key()
        resp = self.session.get(
            url,
            params={"crtfc_key": ks.key},
            timeout=60,
        )
        self.key_pool.mark_used(ks)

        # corpCode.xml 返回 ZIP 二进制
        if resp.status_code != 200:
            # 可能返回 JSON 错误
            try:
                data = resp.json()
                raise RuntimeError(f"DART corpCode 下载失败: {data}")
            except (json.JSONDecodeError, ValueError):
                resp.raise_for_status()

        # 解压 ZIP 内的 XML
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            xml_names = zf.namelist()
            if not xml_names:
                raise RuntimeError("DART corpCode ZIP 内无文件")
            xml_content = zf.read(xml_names[0])

        # 解析 XML
        root = ElementTree.fromstring(xml_content)
        corps: list[dict] = []
        for item in root.findall("list"):
            corp = {
                "corp_code": (item.findtext("corp_code") or "").strip(),
                "corp_name": (item.findtext("corp_name") or "").strip(),
                "stock_code": (item.findtext("stock_code") or "").strip(),
                "modify_date": (item.findtext("modify_date") or "").strip(),
            }
            if corp["corp_code"]:
                corps.append(corp)

        # 保存到 JSONL
        with output_path.open("w", encoding="utf-8") as fp:
            for corp in corps:
                fp.write(json.dumps(corp, ensure_ascii=False) + "\n")

        logger.info("DART corpCode 下载完成: %d 家公司", len(corps))
        return corps

    def get_company_info(self, corp_code: str) -> dict | None:
        """
        查询单个公司的企业概况。

        返回 {corp_name, ceo_nm, hm_url, adres, phn_no, ...} 或 None。
        """
        url = f"{self.BASE_URL}/company.json"
        resp = self._get_with_key(url, params={"corp_code": corp_code})

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError):
            logger.warning("DART company.json 响应非 JSON (corp_code=%s)", corp_code)
            return None

        status = data.get("status", "")
        if status == DART_STATUS_OK:
            return data
        if status == DART_STATUS_NO_DATA:
            return None

        logger.warning("DART company.json 异常 (corp_code=%s): status=%s, message=%s",
                       corp_code, status, data.get("message", ""))
        return None
