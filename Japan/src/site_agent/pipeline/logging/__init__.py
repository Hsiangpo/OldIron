from __future__ import annotations

import asyncio
import re
import time
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Callable

from ...utils import canonical_site_key, normalize_url

_LOG_SINK: ContextVar[Callable[[str], None] | None] = ContextVar(
    "site_agent_log_sink", default=None
)
_LOG_TS_RE = re.compile(r"^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(?:\\s|$)")

# In-flight Snov prefetch tasks keyed by canonical website.
_SNOV_PREFETCH_TASKS: dict[str, tuple[asyncio.Task[list[str]], float | None]] = {}


def _resolve_input_name(site: Any) -> str | None:
    input_name = getattr(site, "input_name", None)
    if isinstance(input_name, str) and input_name.strip():
        return input_name.strip()
    return None


def set_log_sink(sink: Callable[[str], None] | None) -> Any:
    """Set per-task log sink (used by web/packaged runner)."""
    return _LOG_SINK.set(sink)


def reset_log_sink(token: Any) -> None:
    _LOG_SINK.reset(token)


def _now_local_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _with_timestamp(line: str) -> str:
    text = str(line or "").strip()
    if not text:
        return text
    if _LOG_TS_RE.match(text):
        return text
    return f"{_now_local_ts()} {text}"


def _print_ts(line: str, *, flush: bool = True) -> None:
    print(_with_timestamp(line), flush=flush)


def _prefetch_key(website: str) -> str | None:
    if not isinstance(website, str):
        return None
    key = canonical_site_key(website)
    if not key:
        normalized = normalize_url(website)
        key = normalized.lower() if normalized else None
    return key


def register_snov_prefetch_task(
    website: str, task: asyncio.Task[list[str]], started_at: float | None = None
) -> None:
    key = _prefetch_key(website)
    if not key:
        return
    _SNOV_PREFETCH_TASKS[key] = (task, started_at)


def take_snov_prefetch_task(
    website: str,
) -> tuple[asyncio.Task[list[str]], float | None] | None:
    key = _prefetch_key(website)
    if not key:
        return None
    return _SNOV_PREFETCH_TASKS.pop(key, None)


def drop_snov_prefetch_task(website: str) -> None:
    key = _prefetch_key(website)
    if not key:
        return
    _SNOV_PREFETCH_TASKS.pop(key, None)


def _timing_start(memory: dict[str, Any], key: str) -> float | None:
    if not isinstance(memory, dict):
        return None
    if not isinstance(key, str) or not key.strip():
        return None
    return time.perf_counter()


def _timing_end(memory: dict[str, Any], key: str, started: float | None) -> None:
    if started is None:
        return
    if not isinstance(memory, dict):
        return
    timings = memory.setdefault("timings", {})
    entry = timings.setdefault(key, {"count": 0, "total": 0.0})
    entry["count"] = int(entry.get("count", 0)) + 1
    entry["total"] = float(entry.get("total", 0.0)) + (time.perf_counter() - started)


def _log_timing_summary(website: str, memory: dict[str, Any]) -> None:
    timings = memory.get("timings")
    if not isinstance(timings, dict) or not timings:
        return
    started = memory.get("site_started_at")
    total = time.perf_counter() - started if isinstance(started, float) else None
    items: list[tuple[float, int, str]] = []
    for key, entry in timings.items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            continue
        total_sec = float(entry.get("total", 0.0))
        count = int(entry.get("count", 0))
        items.append((total_sec, count, key))
    items.sort(reverse=True)
    parts = [f"{key}={sec:.2f}s/{count}" for sec, count, key in items[:6]]
    prefix = f"总耗时={total:.2f}s" if total is not None else None
    if parts and prefix:
        summary = f"{prefix} | " + " | ".join(parts)
    elif parts:
        summary = " | ".join(parts)
    elif prefix:
        summary = prefix
    else:
        return
    _log(website, f"耗时统计：{summary}")


def _log(website: str, message: str) -> None:
    line = f"[官网] {website} | {message}"
    sink = _LOG_SINK.get()
    if sink:
        sink(line)
        return
    _print_ts(line, flush=True)


def _humanize_exception(exc: Exception) -> str:
    message = str(exc or "").strip()
    if not message:
        return "未知原因"
    human = _humanize_crawl_error(message)
    if human != "未知原因":
        return human
    head = message.splitlines()[0].strip() if message.splitlines() else message
    if len(head) > 120:
        head = head[:120] + "…"
    return head


def _humanize_crawl_error(error: str | None) -> str:
    msg = (error or "").strip()
    if not msg:
        return "未知原因"
    lower = msg.lower()
    if "timeout" in lower or "exceeded" in lower:
        return "加载超时"
    if "err_http_response_code_failure" in lower:
        return "网页响应异常（可能被网站限制访问）"
    if (
        "err_name_not_resolved" in lower
        or "name or service not known" in lower
        or "dns" in lower
    ):
        return "域名解析失败"
    if "err_connection_refused" in lower or "connection refused" in lower:
        return "连接被拒绝"
    if "err_connection_timed_out" in lower or "connection timed out" in lower:
        return "连接超时"
    if "ssl" in lower or "certificate" in lower:
        return "安全连接失败（SSL）"
    if "remote end closed connection" in lower or "connection aborted" in lower:
        return "网站主动断开连接"
    if "403" in lower or "forbidden" in lower:
        return "访问被拒绝（403）"
    if "404" in lower or "not found" in lower:
        return "页面不存在（404）"
    if "429" in lower:
        return "访问频率过高（429）"
    if "401" in lower or "unauthorized" in lower or "incorrect api key" in lower:
        return "LLM 密钥无效或无权限"
    if "5" in lower and "server error" in lower:
        return "网站服务器异常（5xx）"
    head = msg.splitlines()[0].strip() if msg.splitlines() else msg
    if len(head) > 80:
        head = head[:80] + "…"
    return head


def _field_name_zh(field: str) -> str:
    mapping = {
        "company_name": "公司名称",
        "representative": "代表人",
        "email": "邮箱",
        "phone": "座机",
        "capital": "注册资金",
        "employees": "公司人数",
    }
    return mapping.get(field, field)


def _format_fields_zh(fields: list[str]) -> str:
    if not fields:
        return "-"
    return "、".join(_field_name_zh(f) for f in fields)


def _format_field_log(
    field: str,
    value: Any,
    evidence: dict[str, Any] | None,
    *,
    prefix: str = "规则",
) -> str:
    evidence_item = evidence.get(field, {}) if isinstance(evidence, dict) else {}
    source = evidence_item.get("source") if isinstance(evidence_item, dict) else None
    quote = evidence_item.get("quote") if isinstance(evidence_item, dict) else None
    if prefix in {"规则", "AI"}:
        if isinstance(quote, str) and quote == "input_name":
            prefix = "规则"
        elif isinstance(source, str) and source == "rule":
            prefix = "规则"
        elif isinstance(source, str) and source in {"json-ld", "meta"}:
            prefix = "元信息"
        elif isinstance(source, str) and source == "firecrawl":
            prefix = "Firecrawl"
        else:
            prefix = "规则"
    url = evidence_item.get("url") if isinstance(evidence_item, dict) else None
    label = _field_name_zh(field)
    if isinstance(value, str) and value.strip():
        if isinstance(url, str) and url.strip():
            return f"{prefix} 找到{label}：{value.strip()}（来源：{url.strip()}）"
        return f"{prefix} 找到{label}：{value.strip()}"
    return f"{prefix} 未找到{label}"


def _log_extracted_info(website: str, info: dict[str, Any] | None) -> None:
    if not isinstance(info, dict):
        _log(website, "未能从当前页面抽取到有效信息")
        return
    evidence_obj = info.get("evidence")
    evidence: dict[str, Any] = evidence_obj if isinstance(evidence_obj, dict) else {}
    company = info.get("company_name")
    rep = info.get("representative")
    capital = info.get("capital")
    employees = info.get("employees")
    email = info.get("email")
    phone = info.get("phone")
    _log(website, _format_field_log("company_name", company, evidence))
    _log(website, _format_field_log("representative", rep, evidence))
    if isinstance(capital, str) and capital.strip():
        _log(website, _format_field_log("capital", capital, evidence, prefix="规则"))
    if isinstance(employees, str) and employees.strip():
        _log(
            website, _format_field_log("employees", employees, evidence, prefix="规则")
        )
    _log(website, _format_field_log("email", email, evidence, prefix="邮箱采集"))
    if isinstance(phone, str) and phone.strip():
        _log(website, _format_field_log("phone", phone, evidence, prefix="座机采集"))

