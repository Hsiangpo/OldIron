# -*- coding: utf-8 -*-
"""buffettcode 导出：每个行业一个 CSV（老板要求"一个行业一个文件"）。"""
from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

from .store import Store

log = logging.getLogger("buffettcode.export")

_BASE = "https://www.buffett-code.com"
_HEADER = ["company_name", "representative", "website", "address", "capital", "detail_url"]


def _safe(name: str) -> str:
    return re.sub(r"[^0-9A-Za-z぀-ヿ一-鿿]+", "_", name).strip("_")[:40]


def _na(v: str | None) -> str:
    """N/A 等缺失标记归一为空。"""
    s = (v or "").strip()
    return "" if s.upper() in ("N/A", "NA", "-", "—", "ー", "−") else s


def export_per_industry(store: Store, out_dir: str) -> dict:
    """把每个行业 fetched 的公司写成独立 CSV，返回 {行业: 条数}。"""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    summary: dict[str, int] = {}
    for iid, name, _cnt in store.iter_done_industries():
        rows = store.companies_for_industry(iid)
        if not rows:
            continue
        fpath = out / f"{iid}_{_safe(name)}.csv"
        full = 0
        with open(fpath, "w", encoding="utf-8-sig", newline="") as fp:
            w = csv.writer(fp)
            w.writerow(_HEADER)
            for company_name, rep, website, address, capital, path in rows:
                rep, website, address, capital = _na(rep), _na(website), _na(address), _na(capital)
                if company_name and rep and website:
                    full += 1
                w.writerow([company_name or "", rep, website, address, capital, _BASE + path])
        summary[f"{iid} {name}"] = len(rows)
        log.info("导出 %s：%d 条（三件套齐全 %d 条）", fpath.name, len(rows), full)
    return summary
