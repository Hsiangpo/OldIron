from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from ..runner import _append_log as _append_job_log_raw

_LOG_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\s|$)")


def _now_local_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _stamp_log_text(text: str) -> str:
    if not text:
        return text
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    for line in lines:
        content = line.rstrip("\r\n")
        newline = line[len(content) :]
        if not content:
            out.append(line)
            continue
        if _LOG_TS_RE.match(content):
            out.append(line)
            continue
        out.append(f"{_now_local_ts()} {content}{newline}")
    return "".join(out)


def _append_job_log(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamped = _stamp_log_text(text)
    if stamped and not stamped.endswith(("\n", "\r")):
        stamped += "\n"
    _append_job_log_raw(path, stamped)

