from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from .nta_zenken import NtaZenkenDownloader
from .prefectures import normalize_prefecture


_COMPANY_FORMS: tuple[str, ...] = (
    "株式会社",
    "有限会社",
    "合同会社",
    "合名会社",
    "合資会社",
)
_COMPANY_FORM_RE = re.compile("|".join(re.escape(x) for x in _COMPANY_FORMS))

_CITY_CN_TO_JP_TRANS = str.maketrans(
    {
        "县": "県",
        "广": "広",
        "岛": "島",
        "见": "見",
        "钏": "釧",
        "呗": "唄",
        "张": "張",
        "泻": "潟",
        "冈": "岡",
        "宫": "宮",
        "库": "庫",
        "马": "馬",
        "兰": "蘭",
        "贺": "賀",
        "泽": "沢",
        "爱": "愛",
        "长": "長",
        "东": "東",
        "德": "徳",
        "关": "関",
        "荣": "栄",
        "冲": "沖",
        "儿": "児",
        "滨": "浜",
        "鸟": "鳥",
        "叶": "葉",
        "绳": "縄",
        "龙": "竜",
        "网": "網",
        "带": "帯",
        "别": "別",
        "泷": "滝",
        "濑": "瀬",
        "馆": "館",
        "边": "辺",
    }
)
_CITY_JP_TO_CN_TRANS = str.maketrans({v: k for k, v in _CITY_CN_TO_JP_TRANS.items()})


def _city_variants(value: str) -> list[str]:
    base = (value or "").strip()
    if not base:
        return []
    variants = {base}
    variants.add(base.translate(_CITY_CN_TO_JP_TRANS))
    variants.add(base.translate(_CITY_JP_TO_CN_TRANS))
    if not base.endswith(("市", "区", "町", "村")):
        for suffix in ("市", "区", "町", "村"):
            with_suffix = f"{base}{suffix}"
            variants.add(with_suffix)
            variants.add(with_suffix.translate(_CITY_CN_TO_JP_TRANS))
            variants.add(with_suffix.translate(_CITY_JP_TO_CN_TRANS))
    return [v for v in variants if v]


@dataclass
class ExportStats:
    read_rows: int = 0
    exported_rows: int = 0
    skipped_non_latest: int = 0
    skipped_closed: int = 0
    skipped_non_kind_301: int = 0
    skipped_non_company_form: int = 0


def export_companies(
    *,
    location: str,
    city_filter: str | None = None,
    output_dir: Path,
    cache_dir: Path,
    company_only: bool = True,
    active_only: bool = True,
    latest_only: bool = True,
    max_records: int | None = None,
    log_sink: Callable[[str], None] | None = None,
) -> dict:
    """
    从「国税庁 法人番号公表サイト」下载（都道府县）全件数据，导出公司名录（JSONL/CSV）。

    说明：
    - 这里的“公司名录”默认指：kind=301 且名称包含 株式会社/有限会社/合同会社/合名会社/合資会社
    - 不包含官网/邮箱/代表人（需要二阶段 enrichment）
    """
    pref = normalize_prefecture(location)
    if not pref:
        raise ValueError(f"无法识别地区（请用都道府县，如：大阪府）：{location}")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_jsonl = output_dir / "output.jsonl"
    out_csv = output_dir / "output.csv"
    out_meta = output_dir / "meta.json"

    stats = ExportStats()

    def log(line: str) -> None:
        if not log_sink:
            return
        text = (line or "").rstrip("\n")
        if text:
            log_sink(text)

    log(f"[法人] 目标地区：{pref}" + (f"，城市过滤：{city_filter}" if city_filter else ""))
    downloader = NtaZenkenDownloader(cache_dir=cache_dir, log_sink=log_sink)
    dl = downloader.download_prefecture_zip(pref)

    log(f"[法人] 下载文件数：{len(dl.zip_paths)}")
    log(f"[法人] 开始解析并导出（输出：{out_jsonl.name} / {out_csv.name}）")

    with out_jsonl.open("w", encoding="utf-8") as jf, out_csv.open("w", encoding="utf-8", newline="") as cf:
        writer = csv.DictWriter(
            cf,
            fieldnames=[
                "corporate_number",
                "name",
                "address",
                "post_code",
                "prefecture",
                "city",
                "street",
                "kind",
                "latest",
                "close_date",
                "source_zip",
                "source_csv",
                "status",
            ],
        )
        writer.writeheader()

        for zip_path in dl.zip_paths:
            with zipfile.ZipFile(zip_path) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    raise RuntimeError(f"zip 内未发现 CSV：{zip_path}")
                csv_name = csv_names[0]
                log(f"[法人] 读取：{zip_path.name}::{csv_name}")
                with zf.open(csv_name) as raw:
                    # NTA 提供的 CSV 是 Shift_JIS(CP932)
                    text = io.TextIOWrapper(raw, encoding="cp932", errors="replace", newline="")
                    reader = csv.reader(text)
                    for row in reader:
                        stats.read_rows += 1
                        if max_records and stats.exported_rows >= max_records:
                            break
                        if len(row) < 30:
                            continue
                        kind = (row[8] or "").strip()
                        latest = (row[23] or "").strip()
                        close_date = (row[18] or "").strip()
                        name = (row[6] or "").strip()

                        if latest_only and latest != "1":
                            stats.skipped_non_latest += 1
                            continue
                        if active_only and close_date:
                            stats.skipped_closed += 1
                            continue
                        if kind != "301":
                            stats.skipped_non_kind_301 += 1
                            continue
                        if company_only and not _COMPANY_FORM_RE.search(name):
                            stats.skipped_non_company_form += 1
                            continue

                        prefecture_name = (row[9] or "").strip()
                        city_name = (row[10] or "").strip()
                        street_number = (row[11] or "").strip()
                        address_outside = (row[16] or "").strip()
                        post_code = (row[15] or "").strip()
                        corporate_number = (row[1] or "").strip()
                        address = f"{prefecture_name}{city_name}{street_number}{address_outside}"

                        if city_filter:
                            cf = city_filter.strip()
                            if cf:
                                variants = _city_variants(cf)
                                if variants and not any(
                                    (v in city_name or v in address) for v in variants
                                ):
                                    continue

                        rec = {
                            "corporate_number": corporate_number,
                            "name": name,
                            "address": address,
                            "post_code": post_code,
                            "prefecture": prefecture_name,
                            "city": city_name,
                            "street": street_number + address_outside,
                            "kind": kind,
                            "latest": latest,
                            "close_date": close_date,
                            "source_zip": zip_path.name,
                            "source_csv": csv_name,
                            "status": "ok",
                        }
                        jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        writer.writerow(rec)
                        stats.exported_rows += 1

                        if stats.read_rows % 200000 == 0:
                            log(f"[法人] 进度：读取 {stats.read_rows} 行，导出 {stats.exported_rows} 条公司")

            if max_records and stats.exported_rows >= max_records:
                break

    meta = {
        "prefecture": pref,
        "city_filter": city_filter.strip() if city_filter else None,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "output": {"jsonl": str(out_jsonl), "csv": str(out_csv)},
        "filters": {
            "company_only": bool(company_only),
            "active_only": bool(active_only),
            "latest_only": bool(latest_only),
        },
        "stats": {
            "read_rows": stats.read_rows,
            "exported_rows": stats.exported_rows,
            "skipped_non_latest": stats.skipped_non_latest,
            "skipped_closed": stats.skipped_closed,
            "skipped_non_kind_301": stats.skipped_non_kind_301,
            "skipped_non_company_form": stats.skipped_non_company_form,
        },
    }
    out_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[法人] 完成：导出{stats.exported_rows} 条公司（读取 {stats.read_rows} 行）")
    return meta


