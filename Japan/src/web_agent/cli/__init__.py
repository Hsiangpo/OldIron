from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from datetime import datetime

from site_agent.snov_client import _fetch_cdp_cookies, _get_cdp_ws_url
from ..service import (
    JobService,
    TERMINAL_STATUSES,
    ensure_prefecture_docs,
    match_prefecture_display,
    update_city_progress,
    update_pref_progress,
)
from ..store import iter_job_dirs
from ..store import normalize_job_suffix, read_json


DEFAULT_WEB_LLM_BASE_URL = "https://api.gpteamservices.com/v1"


def _print_ts(message: str, *, end: str = "\n", flush: bool = True) -> None:
    text = str(message or "")
    if not text:
        print(text, end=end, flush=flush)
        return
    prefix = ""
    while text.startswith("\n"):
        prefix += "\n"
        text = text[1:]
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{stamp} {text}" if text else ""
    print(f"{prefix}{line}", end=end, flush=flush)


def main() -> None:
    args = _parse_args()
    os.environ.setdefault("PYTHONUTF8", "1")
    # 打包版会把 Chromium 放在 playwright/driver/package/.local-browsers 下。
    # 强制 Playwright 使用该目录，避免依赖用户机器的缓存目录。
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
    if isinstance(args.city, str) and args.city.strip():
        cmd = args.city.strip().lower()
        if cmd in {"snov-login", "snov_login"}:
            _run_snov_login()
            return
        if cmd in {"snov-export", "snov_export", "snov-cookie", "snov_cookie"}:
            _run_snov_export()
            return
    if args.city:
        try:
            asyncio.run(_run_location_job(args.city, args.mode))
        except KeyboardInterrupt:
            _print_ts("\n[提示] 已中断（Ctrl+C/窗口关闭）。可重新运行同城命令自动续跑。")
        return
    _print_ts("[错误] 未提供城市参数。示例：python -m web_agent 东京都")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Official Site Agent CLI")
    parser.add_argument(
        "city",
        nargs="?",
        help="城市名（如：札幌市）。或特殊命令：snov-login / snov-export",
    )
    parser.add_argument("mode", nargs="?", help="续跑模式：失败/半成/代表人/simple（可选）")
    return parser.parse_args()


def _load_llm_api_key() -> str:
    key = (os.environ.get("LLM_API_KEY") or "").strip()
    if key:
        return key
    key_path = Path(os.environ.get("WEB_AGENT_LLM_KEY_FILE") or "docs/llm_key.txt")
    if key_path.exists():
        content = key_path.read_text(encoding="utf-8", errors="ignore").strip()
        if content:
            return content.splitlines()[0].strip()
    return ""


def _snov_profile_dir() -> Path:
    raw = os.environ.get("SNOV_PROFILE_DIR") or "output/snov_profile"
    try:
        return Path(raw).expanduser().resolve()
    except Exception:
        return Path(raw)


def _snov_cookie_file() -> Path:
    return Path(os.environ.get("SNOV_EXTENSION_COOKIE_FILE") or "output/snov_extension_cookies.json")


def _find_chrome_path() -> str | None:
    env_path = (os.environ.get("CHROME_PATH") or os.environ.get("GOOGLE_CHROME_BINARY") or "").strip()
    if env_path and Path(env_path).exists():
        return env_path
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        str(Path.home() / "AppData/Local/Google/Chrome/Application/chrome.exe"),
    ]
    for path in candidates:
        if path and Path(path).exists():
            return path
    return shutil.which("chrome") or shutil.which("chrome.exe")


def _run_snov_login() -> None:
    chrome = _find_chrome_path()
    if not chrome:
        _print_ts("[错误] 未找到 Chrome，可设置 CHROME_PATH 指向 chrome.exe")
        return
    profile_dir = _snov_profile_dir()
    profile_dir.mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get("SNOV_CDP_PORT") or 9222)
    url = "https://app.snov.io/login"
    args = [
        chrome,
        f"--user-data-dir={profile_dir}",
        f"--remote-debugging-port={port}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-popup-blocking",
        "--new-window",
        url,
    ]
    subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _print_ts("[Snov] 已启动专用浏览器，请在该窗口登录 app.snov.io 后再执行 snov-export")
    _print_ts(f"[Snov] Profile: {profile_dir}")
    _print_ts(f"[Snov] CDP 端口: {port}")


def _run_snov_export() -> None:
    chrome = _find_chrome_path()
    if not chrome:
        _print_ts("[错误] 未找到 Chrome，可设置 CHROME_PATH 指向 chrome.exe")
        return
    profile_dir = _snov_profile_dir()
    profile_dir.mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get("SNOV_CDP_PORT") or 9222)
    host = os.environ.get("SNOV_CDP_HOST") or "127.0.0.1"

    ws_url = None
    try:
        ws_url = _get_cdp_ws_url(host, port, timeout=6)
    except Exception:
        ws_url = None

    proc = None
    if not ws_url:
        args = [
            chrome,
            f"--user-data-dir={profile_dir}",
            f"--remote-debugging-port={port}",
            "--headless=new",
            "--disable-gpu",
            "--no-first-run",
            "--no-default-browser-check",
            "about:blank",
        ]
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                ws_url = _get_cdp_ws_url(host, port, timeout=2)
            except Exception:
                ws_url = None
            if ws_url:
                break
            time.sleep(0.2)

    if not ws_url:
        if proc:
            proc.terminate()
        _print_ts("[错误] 无法连接 9222 CDP，请先执行 snov-login 并保持浏览器打开")
        return

    cookies = _fetch_cdp_cookies(ws_url, ["https://app.snov.io/", "https://app.snov.io"])
    if proc:
        proc.terminate()
    if not cookies:
        _print_ts("[错误] 未读取到 app.snov.io 的 cookies，请确认在 snov-login 打开的窗口里已登录")     
        _print_ts("[提示] 请保持该窗口打开，并在地址栏打开 https://app.snov.io/ 后重试 snov-export")
        return
    cookie_map = {}
    for item in cookies:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if isinstance(name, str) and isinstance(value, str) and name.strip() and value.strip():
            cookie_map[name] = value
    if not cookie_map:
        _print_ts("[错误] cookies 为空，请确认登录成功")
        return
    out_path = _snov_cookie_file()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cookies": cookie_map,
        "profile_dir": str(profile_dir),
        "cdp_host": host,
        "cdp_port": port,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    selector = cookie_map.get("selector") or ""
    token = cookie_map.get("token") or ""
    fingerprint = cookie_map.get("fingerprint") or ""
    _print_ts(f"[Snov] Cookies 已导出：{out_path}")
    if selector and token:
        _print_ts("[Snov] 已包含 selector/token，可直接用于扩展接口")
    if fingerprint:
        _print_ts("[Snov] 已包含 fingerprint")


def _find_latest_job_id_for_target(jobs_dir: Path, target: str) -> str | None:
    suffix = normalize_job_suffix(target)
    if not suffix or not jobs_dir.exists():
        return None
    best_id = None
    best_created = ""
    for job_dir in iter_job_dirs(jobs_dir):
        name = job_dir.name
        if not name.endswith(f"_{suffix}"):
            continue
        job = read_json(job_dir / "job.json") or {}
        created = job.get("created_at") if isinstance(job, dict) else None
        created_key = created or name
        if created_key >= best_created:
            best_created = created_key
            best_id = name
    return best_id


async def _run_location_job(location: str, mode: str | None = None) -> None:
    llm_key = _load_llm_api_key()
    jobs_dir = Path(os.environ.get("WEB_JOBS_DIR") or "output/web_jobs")
    keywords_dir = Path(os.environ.get("WEB_KEYWORDS_DIR") or "output/web_keywords")
    service = JobService(jobs_dir=jobs_dir, keywords_dir=keywords_dir)
    ensure_prefecture_docs()
    pref_display = match_prefecture_display(location)
    is_prefecture = isinstance(pref_display, str) and pref_display.strip()
    target_name = pref_display if is_prefecture else location
    max_sites = None
    if isinstance(mode, str) and mode.strip().isdigit():
        max_sites = int(mode.strip())
        mode = None
    env_max_sites = (os.environ.get("WEB_AGENT_MAX_SITES") or os.environ.get("WEB_AGENT_TARGET_SITES") or "").strip()
    if max_sites is None and env_max_sites.isdigit():
        max_sites = int(env_max_sites)
    resume_mode = None
    simple_mode = False
    if isinstance(mode, str) and mode.strip():
        normalized = mode.strip().lower()
        if normalized in {"失败", "failed"}:
            resume_mode = "failed"
        elif normalized in {"半成", "partial"}:
            resume_mode = "partial"
        elif normalized in {"代表人", "rep", "representative"}:
            resume_mode = "representative"
        elif normalized in {"simple", "简版", "简单"}:
            simple_mode = True
    # 默认以“公司名+代表人+邮箱”为成功标准；电话继续提取但不作为成功硬条件。
    base_fields = ["company_name", "representative", "capital", "employees", "email"]
    if resume_mode == "representative":
        base_fields = ["company_name", "representative", "email"]
    if simple_mode:
        base_fields = ["company_name", "phone"]
    site_payload = {"concurrency": 16, "llm_concurrency": 16, "firecrawl_extract_enabled": False}
    if simple_mode:
        # simple 路线仅使用地图结果，目标字段为公司名+座机。
        site_payload.update(
            {
                "simple_mode": True,
                "require_phone": True,
                "use_llm": False,
                "skip_email": True,
            }
        )
    payload = {
        "source": "registry",
        "registry_enrich": True,
        "registry": {
            "location": pref_display if is_prefecture else "全国",
            "city": target_name if not is_prefecture else None,
            "prefecture": pref_display if is_prefecture else None,
            "max_records": 0,
            "company_only": True,
            "active_only": True,
            "latest_only": True,
        },
        "gmap": {"concurrency": 16},
        "site": site_payload,
        "fields": base_fields,
        "llm_model": "gpt-5.1-codex-mini",
        "llm_base_url": (os.environ.get("LLM_BASE_URL") or DEFAULT_WEB_LLM_BASE_URL).strip(),
        "llm_reasoning_effort": "medium",
        "llm_warmup": False,
        "infinite_retry": True,
        "llm_api_key": llm_key,
        "use_llm": False,
        "job_group": "prefecture" if is_prefecture else "city",
    }
    if simple_mode:
        payload["mode"] = "simple"
        payload["parallel_pipeline"] = True
        payload["resume_mode"] = ""
        payload.setdefault("site", {})["resume_mode"] = ""
    if isinstance(max_sites, int) and max_sites > 0:
        payload.setdefault("site", {})["max_sites"] = max_sites
    else:
        # 续跑时显式清空历史 max_sites，避免旧任务残留导致只处理前 N 条。
        payload.setdefault("site", {})["max_sites"] = None
    if resume_mode:
        payload["resume_mode"] = resume_mode
        payload.setdefault("site", {})["resume_mode"] = resume_mode
    job_id = None
    try:
        force_new = (os.environ.get("WEB_AGENT_FORCE_NEW") or "").strip().lower() in {"1", "true", "yes"}
        existing_job_id = None if force_new else _find_latest_job_id_for_target(jobs_dir, target_name)
        if existing_job_id:
            existing_job = service.load_job(existing_job_id) or {}
            if isinstance(existing_job, dict) and existing_job.get("status") == "running":
                existing_req = existing_job.get("request") if isinstance(existing_job.get("request"), dict) else {}
                existing_fields = existing_req.get("fields") if isinstance(existing_req, dict) else None
                payload_fields = payload.get("fields")
                fields_changed = isinstance(existing_fields, list) and existing_fields != payload_fields
                force_resume_running = bool(resume_mode) or bool(simple_mode) or fields_changed
                paths = service.get_job_paths(existing_job_id)
                stale = True
                if paths.log_path.exists():
                    try:
                        mtime = paths.log_path.stat().st_mtime
                        stale = (time.time() - mtime) > 300
                    except Exception:
                        stale = True
                if stale or force_resume_running:
                    job = await service.resume_job(existing_job_id, payload)
                    job_id = existing_job_id
                    mode = "续跑"
                else:
                    job_id = existing_job_id
                    mode = "继续监控"
            else:
                job = await service.resume_job(existing_job_id, payload)
                job_id = existing_job_id
                mode = "续跑"
        else:
            job = await service.create_job(payload)
            job_id = job.get("id")
            mode = "已创建"
        if not job_id:
            _print_ts("[错误] 创建任务失败")
            return
        paths = service.get_job_paths(job_id)
        _print_ts(f"[任务] {mode}：{job_id}")
        _print_ts(f"[日志] {paths.log_path}")
        _print_ts(f"[输出] {paths.job_dir}")
        if is_prefecture and pref_display:
            update_pref_progress(pref_display, 0)
        else:
            update_city_progress(target_name, 0)

        pos = 0
        if mode in {"续跑", "继续监控"} and paths.log_path.exists():
            with contextlib.suppress(OSError):
                pos = paths.log_path.stat().st_size
        truncated_notice = False
        while True:
            if paths.log_path.exists():
                try:
                    size = paths.log_path.stat().st_size
                except OSError:
                    size = None
                if size is not None and size < pos:
                    pos = 0
                    if not truncated_notice:
                        print("\n[系统] 日志已裁剪，重置读取游标。\n", end="", flush=True)
                        truncated_notice = True
                with paths.log_path.open("r", encoding="utf-8-sig", errors="replace") as f:
                    f.seek(pos)
                    # 避免 job.log 过大时一次性 read() 占用过多内存导致 MemoryError。
                    while True:
                        chunk = f.read(64 * 1024)
                        if not chunk:
                            break
                        print(chunk, end="", flush=True)
                    pos = f.tell()
            job_state = service.load_job(job_id) or {}
            status = job_state.get("status")
            if status in TERMINAL_STATUSES:
                if status == "succeeded":
                    if is_prefecture and pref_display:
                        update_pref_progress(pref_display, 1)
                    else:
                        update_city_progress(target_name, 1)
                _print_ts(f"\n[完成] 状态={status} 输出目录={paths.job_dir}")
                break
            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        if job_id:
            await service.cancel_job(job_id)
        _print_ts("\n[提示] 任务已中断，可重新运行同城命令自动续跑。")
        return
