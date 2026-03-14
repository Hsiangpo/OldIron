from __future__ import annotations

import re
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import requests

from .prefectures import normalize_prefecture


ZENKEN_BASE_URL = "https://www.houjin-bangou.nta.go.jp/download/zenken/"
TOKEN_FIELD = "jp.go.nta.houjin_bangou.framework.web.common.CNSFWTokenProcessor.request.token"


def _detect_windows_inet_proxy() -> str | None:
    """
    读取 Windows「Internet 选项」代理（WinINet）。
    复用 gmap_agent 的思路：让下载也尽量跟浏览器一致。
    """
    try:
        import sys

        if sys.platform != "win32":
            return None
    except Exception:
        return None

    try:
        import winreg  # type: ignore
    except Exception:
        return None

    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Internet Settings",
        )
        enabled, _ = winreg.QueryValueEx(key, "ProxyEnable")
        server, _ = winreg.QueryValueEx(key, "ProxyServer")
    except OSError:
        return None

    try:
        if int(enabled) != 1:
            return None
    except Exception:
        return None

    if not isinstance(server, str):
        return None
    value = server.strip()
    if not value:
        return None

    picked: str | None = None
    if "=" in value:
        items = [part.strip() for part in value.split(";") if part.strip()]
        by_proto: dict[str, str] = {}
        for item in items:
            if "=" not in item:
                continue
            proto, addr = item.split("=", 1)
            proto = proto.strip().lower()
            addr = addr.strip()
            if not addr:
                continue
            by_proto[proto] = addr
        picked = by_proto.get("https") or by_proto.get("http")
    else:
        picked = value

    if not picked:
        return None
    if picked.startswith("http://") or picked.startswith("https://"):
        return picked
    return "http://" + picked


def _extract_csrf_token(html: str) -> str | None:
    marker = "CNSFWTokenProcessor.request.token"
    idx = html.find(marker)
    if idx < 0:
        return None
    q = chr(34)
    val_idx = html.find("value=" + q, idx)
    if val_idx < 0:
        return None
    start = val_idx + len("value=" + q)
    end = html.find(q, start)
    if end < 0:
        return None
    token = html[start:end].strip()
    return token or None


def _extract_zip_file_nos(html: str, prefecture: str) -> list[str]:
    pref = normalize_prefecture(prefecture) or prefecture.strip()
    if not pref:
        raise ValueError("prefecture 不能为空")
    # 结构：<dt class="mb05">大阪府</dt> ... <ol> <li><a ... doDownload(26164);">zip 20MB</a></li> </ol>
    pat = re.compile(
        rf"<dt[^>]*mb05[^>]*>\s*{re.escape(pref)}\s*</dt>.*?<ol[^>]*>(.*?)</ol>",
        re.S,
    )
    m = pat.search(html)
    if not m:
        raise ValueError(f"未在 NTA 下载页中找到地区：{pref}")
    block = m.group(1)
    # 尽量选择文本包含 zip 的下载项；兜底取第一个 doDownload
    items = re.findall(r"doDownload\((\d+)\);?[^>]*>\s*([^<]*?)\s*</a>", block)
    picked: list[str] = []
    for num, label in items:
        if "zip" in (label or "").lower():
            picked.append(num)
    if picked:
        return picked
    fallback = re.findall(r"doDownload\((\d+)\)", block)
    return [fallback[0]] if fallback else []


def _filename_from_content_disposition(value: str | None) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    # 示例：attachment; filename*=utf-8'jp'27_osaka_all_20251226.zip
    m = re.search(r"filename\*=(?:utf-8'?[^']*')?([^;\s]+)", value, re.I)
    if m:
        name = m.group(1).strip()
        return name.strip("\"'")
    m = re.search(r"filename=([^;\s]+)", value, re.I)
    if m:
        name = m.group(1).strip()
        return name.strip("\"'")
    return None


@dataclass
class ZenkenDownloadResult:
    prefecture: str
    zip_paths: list[Path]


class NtaZenkenDownloader:
    def __init__(
        self,
        *,
        cache_dir: Path,
        base_url: str = ZENKEN_BASE_URL,
        proxy: str | None = None,
        timeout_seconds: int = 90,
        log_sink: Callable[[str], None] | None = None,
    ) -> None:
        self.cache_dir = cache_dir
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout_seconds = max(5, int(timeout_seconds))
        self._log_sink = log_sink
        self._lock = threading.Lock()

        self.session = requests.Session()
        self.session.trust_env = True
        self.session.headers.update(
            {
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        picked_proxy = proxy or _detect_windows_inet_proxy()
        if picked_proxy:
            self.session.proxies.update({"http": picked_proxy, "https": picked_proxy})

    def _log(self, line: str) -> None:
        if not self._log_sink:
            return
        text = (line or "").rstrip("\n")
        if not text:
            return
        with self._lock:
            self._log_sink(text)

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        max_attempts = int(kwargs.pop("max_attempts", 3))
        retry_sleep = float(kwargs.pop("retry_sleep", 2.0))
        last_exc: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                resp = self.session.request(method, url, timeout=self.timeout_seconds, **kwargs)
                if resp.status_code in (429,) or 500 <= resp.status_code <= 599:
                    if attempt >= max_attempts:
                        return resp
                    self._log(
                        f"[法人] NTA 请求返回 {resp.status_code}，{retry_sleep:.0f}s 后重试 ({attempt}/{max_attempts})"
                    )
                    resp.close()
                    time.sleep(retry_sleep)
                    continue
                return resp
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as exc:
                last_exc = exc
                if attempt >= max_attempts:
                    break
                self._log(
                    f"[法人] NTA 请求异常(SSL/连接)：{exc}，{retry_sleep:.0f}s 后重试 ({attempt}/{max_attempts})"
                )
                time.sleep(retry_sleep)
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                if attempt >= max_attempts:
                    break
                self._log(
                    f"[法人] NTA 请求异常：{exc}，{retry_sleep:.0f}s 后重试 ({attempt}/{max_attempts})"
                )
                time.sleep(retry_sleep)

        if last_exc:
            raise last_exc
        raise RuntimeError("NTA 请求失败")

    def download_prefecture_zip(self, prefecture: str) -> ZenkenDownloadResult:
        pref = normalize_prefecture(prefecture) or prefecture.strip()
        if not pref:
            raise ValueError("prefecture 不能为空")

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._log(f"[法人] 正在获取 NTA 下载页：{self.base_url}")
        resp = self._request_with_retry("GET", self.base_url, headers={"Connection": "close"})
        resp.raise_for_status()
        html = resp.text or ""
        token = _extract_csrf_token(html)
        if not token:
            raise RuntimeError("无法解析 NTA 下载页 token（页面结构可能已变更）")

        file_nos = _extract_zip_file_nos(html, pref)
        if not file_nos:
            raise RuntimeError(f"无法解析下载文件编号：{pref}")

        zip_paths: list[Path] = []
        for file_no in file_nos:
            zip_paths.append(self._download_one_zip(token=token, file_no=file_no))

        return ZenkenDownloadResult(prefecture=pref, zip_paths=zip_paths)

    def _download_one_zip(self, *, token: str, file_no: str) -> Path:
        action_url = self.base_url + "index.html"
        data = {TOKEN_FIELD: token, "event": "download", "selDlFileNo": str(file_no)}

        self._log(f"[法人] 请求下载文件：fileNo={file_no}")
        resp = self._request_with_retry(
            "POST",
            action_url,
            data=data,
            stream=True,
            headers={"Connection": "close"},
        )
        resp.raise_for_status()

        filename = _filename_from_content_disposition(resp.headers.get("content-disposition"))
        if not filename:
            filename = f"zenken_{file_no}.zip"
        target = self.cache_dir / filename
        if target.exists() and target.stat().st_size > 1024:
            self._log(f"[法人] 已命中缓存：{target.name} ({target.stat().st_size} bytes)")
            return target

        tmp = target.with_suffix(target.suffix + ".tmp")
        written = 0
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                f.write(chunk)
                written += len(chunk)
        tmp.replace(target)
        self._log(f"[法人] 下载完成：{target.name} ({written} bytes)")
        return target
