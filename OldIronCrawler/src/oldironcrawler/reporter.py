from __future__ import annotations

import csv
from pathlib import Path

from oldironcrawler.runtime.store import SiteStageMetrics


def print_site_result(
    *,
    completed_index: int,
    total: int,
    website: str,
    company_name: str,
    representative: str,
    emails: str,
    searched_representative: str = "",
    phones: str = "",
    reason: str = "",
    stage_metrics: SiteStageMetrics | None = None,
    show_emails: bool = True,
    show_phones: bool = True,
    show_representative: bool = True,
    show_searched_representative: bool = True,
) -> None:
    print(f"[{completed_index}/{total}] {website}", flush=True)
    print(f"  公司名: {_display(company_name)}", flush=True)
    if show_representative:
        print(f"  代表人: {_display(representative)}", flush=True)
    if show_emails:
        print(f"  邮箱: {_display(emails)}", flush=True)
    if show_searched_representative:
        print(f"  搜索现役最大代表人: {_display(searched_representative)}", flush=True)
    if show_phones:
        print(f"  电话: {_display(phones)}", flush=True)
    if stage_metrics is not None:
        print(f"  阶段耗时: {_format_stage_timing(stage_metrics)}", flush=True)
        print(f"  页面统计: {_format_stage_counts(stage_metrics)}", flush=True)
    if str(reason or "").strip():
        print(f"  原因: {reason.strip()}", flush=True)
    print("  ------------------------------------------------------------", flush=True)


def print_progress_heartbeat(*, total: int, done: int, running: int, dropped: int, pending: int) -> None:
    print(
        f"[进度] total={total} done={done} running={running} dropped={dropped} pending={pending}",
        flush=True,
    )


def write_delivery_csv(
    path: Path,
    rows: list[dict[str, str]],
    *,
    include_email: bool = True,
    include_phone: bool = True,
    include_representative: bool = True,
    include_searched_representative: bool = True,
) -> None:
    # 交付列随开关动态裁剪：关掉的字段那一列直接不出现在 CSV 里。
    fieldnames = ["company_name"]
    if include_representative:
        fieldnames.append("representative")
    if include_email:
        fieldnames.append("emails")
    if include_searched_representative:
        fieldnames.append("searched_representative")
    if include_phone:
        fieldnames.append("phones")
    fieldnames.append("website")
    _write_csv_atomic(path, fieldnames, rows)


def write_delivery_reports(
    *,
    store,
    success_path: Path,
    failed_path: Path,
    include_email: bool = True,
    include_phone: bool = True,
    include_representative: bool = True,
    include_searched_representative: bool = True,
) -> None:
    success_rows: list[dict[str, str]] = []
    failed_rows: list[dict[str, str]] = []
    success_fieldnames = _build_success_fieldnames(
        include_email=include_email,
        include_phone=include_phone,
        include_representative=include_representative,
        include_searched_representative=include_searched_representative,
    )
    failed_fieldnames = _build_failed_fieldnames(success_fieldnames)
    for row in store.delivery_report_rows():
        normalized = _normalize_report_row(row)
        missing_fields = _missing_selected_fields(
            normalized,
            include_email=include_email,
            include_phone=include_phone,
            include_representative=include_representative,
            include_searched_representative=include_searched_representative,
        )
        if normalized["status"] == "done" and not missing_fields:
            success_rows.append(_project_fields(normalized, success_fieldnames))
            continue
        failed_rows.append(_build_failed_row(normalized, failed_fieldnames, missing_fields))
    _write_csv_atomic(success_path, success_fieldnames, success_rows)
    _write_csv_atomic(failed_path, failed_fieldnames, failed_rows)


def _write_csv_atomic(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    with temp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    temp_path.replace(path)


def _build_success_fieldnames(
    *,
    include_email: bool,
    include_phone: bool,
    include_representative: bool,
    include_searched_representative: bool,
) -> list[str]:
    fieldnames = ["company_name"]
    if include_representative:
        fieldnames.append("representative")
    if include_email:
        fieldnames.append("emails")
    if include_searched_representative:
        fieldnames.append("searched_representative")
    if include_phone:
        fieldnames.append("phones")
    fieldnames.append("website")
    return fieldnames


def _build_failed_fieldnames(success_fieldnames: list[str]) -> list[str]:
    selected_fields = [field for field in success_fieldnames if field not in {"company_name", "website"}]
    return ["company_name", "website", "missing_fields", "status", "failure_reason", *selected_fields]


def _normalize_report_row(row: dict[str, str]) -> dict[str, str]:
    normalized = {key: str(value or "").strip() for key, value in row.items()}
    if not normalized.get("company_name"):
        normalized["company_name"] = normalized.get("input_company_name", "")
    return normalized


def _missing_selected_fields(
    row: dict[str, str],
    *,
    include_email: bool,
    include_phone: bool,
    include_representative: bool,
    include_searched_representative: bool,
) -> list[str]:
    required = ["company_name"]
    if include_representative:
        required.append("representative")
    if include_email:
        required.append("emails")
    if include_searched_representative:
        required.append("searched_representative")
    if include_phone:
        required.append("phones")
    return [field for field in required if not row.get(field, "").strip()]


def _project_fields(row: dict[str, str], fieldnames: list[str]) -> dict[str, str]:
    return {field: row.get(field, "") for field in fieldnames}


def _build_failed_row(row: dict[str, str], fieldnames: list[str], missing_fields: list[str]) -> dict[str, str]:
    result = _project_fields(row, fieldnames)
    result["missing_fields"] = ";".join(missing_fields)
    result["status"] = row.get("status", "")
    result["failure_reason"] = _build_failure_reason(row, missing_fields)
    return result


def _build_failure_reason(row: dict[str, str], missing_fields: list[str]) -> str:
    last_error = str(row.get("last_error", "") or "").strip()
    if last_error:
        return last_error
    if missing_fields:
        return f"缺少：{';'.join(missing_fields)}"
    return "未满足本次选择字段"


def _display(value: str) -> str:
    text = str(value or "").strip()
    return text if text else "未找到"


def _format_stage_timing(metrics: SiteStageMetrics) -> str:
    return " | ".join(
        [
            f"发现 {metrics.discover_ms / 1000:.1f}s",
            f"LLM选页 {metrics.llm_pick_ms / 1000:.1f}s",
            f"抓页 {metrics.fetch_pages_ms / 1000:.1f}s",
            f"LLM抽取 {metrics.llm_extract_ms / 1000:.1f}s",
            f"AI搜索 {metrics.search_rep_ms / 1000:.1f}s",
            f"联系方式规则 {metrics.email_rule_ms / 1000:.1f}s",
            f"公司规则 {metrics.company_rule_ms / 1000:.1f}s",
        ]
    )


def _format_stage_counts(metrics: SiteStageMetrics) -> str:
    return " | ".join(
        [
            f"候选 {metrics.discovered_url_count}",
            f"负责人页 {metrics.rep_url_count}",
            f"邮箱页 {metrics.email_url_count}",
            f"目标页 {metrics.target_url_count}",
            f"实抓 {metrics.fetched_page_count}",
        ]
    )
