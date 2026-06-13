from __future__ import annotations

import os
import subprocess
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from rich.console import Group
from rich.text import Text

from oldironcrawler import app as app_module
from oldironcrawler.config import resolve_websites_dir
from oldironcrawler.console import prompt_runtime_llm_key, wait_for_enter
from oldironcrawler.extractor.llm_client import LlmConfigurationError, LlmTemporaryError
from oldironcrawler.importer import list_input_files
from oldironcrawler.ui.console import clear_screen, get_console, hairline, inline_stats, kv_block, screen, wordmark
from oldironcrawler.ui.menu import MenuItem, MenuSpec, run_menu

_BACK = "__back__"
_WORDMARK = "OLDIRONCRAWLER"
_SUBTITLE = "公司官网采集"


@dataclass
class DashboardSession:
    project_root: Path
    current_key: str
    selected_input: Path | None = None
    concurrency: int = app_module.DEFAULT_SITE_CONCURRENCY
    site_timeout_seconds: int = app_module.DEFAULT_SITE_TIMEOUT_SECONDS
    last_delivery_path: Path | None = None
    last_failed_path: Path | None = None
    llm_base_url: str = ""
    collect_company_name_enabled: bool = False
    collect_email_enabled: bool = True
    collect_phone_enabled: bool = False
    extract_representative_enabled: bool = False
    search_representative_enabled: bool = False


def run_dashboard(project_root: Path, initial_key: str) -> int:
    session = DashboardSession(
        project_root=project_root.resolve(),
        current_key=str(initial_key or "").strip(),
    )
    _ensure_key_before_panel(session)
    while True:
        choice = run_menu(_main_menu_spec(session))
        if choice in (None, "quit"):
            return 0
        if choice == "start":
            _handle_start_crawl(session)
        elif choice == "websites":
            _handle_open_websites(session)
        elif choice == "output":
            _handle_open_output(session)
        elif choice == "config":
            if _handle_system_config(session) == "exit":
                return 0


def _main_menu_spec(session: DashboardSession) -> MenuSpec:
    return MenuSpec(
        title=_WORDMARK,
        subtitle=_SUBTITLE,
        status_block=_main_status_block(session),
        items=[
            MenuItem("start", "开始抓取"),
            MenuItem("websites", "打开 websites 文件夹"),
            MenuItem("output", "打开 output 文件夹"),
            MenuItem("config", "系统配置"),
            MenuItem("quit", "退出程序"),
        ],
        hint="↑↓ 移动   Enter 确认   1–5 直达   Ctrl+C 退出",
        back_value="quit",
    )


def _main_status_block(session: DashboardSession) -> Group:
    websites = len(list_input_files(_get_websites_dir(session.project_root)))
    results = len(_list_output_results(session.project_root / "output"))
    rows = [
        ("KEY", _key_value(session)),
        ("入口", session.llm_base_url or "未就绪"),
        ("当前文件", session.selected_input.name if session.selected_input else "未选择"),
    ]
    stats = inline_stats(
        [
            ("待爬", str(websites)),
            ("结果", str(results)),
            ("并发", str(session.concurrency)),
            ("超时", f"{session.site_timeout_seconds}s"),
        ]
    )
    return Group(kv_block(rows), Text(), stats, _toggles_line(session))


def _toggles_line(session: DashboardSession) -> Text:
    toggles = [
        ("公司名", session.collect_company_name_enabled),
        ("邮箱", session.collect_email_enabled),
        ("电话", session.collect_phone_enabled),
        ("代表人", session.extract_representative_enabled),
        ("搜索代表人", session.search_representative_enabled),
    ]
    text = Text()
    for index, (label, enabled) in enumerate(toggles):
        if index:
            text.append("    ", style="hair")
        text.append("● " if enabled else "○ ", style="ready" if enabled else "dim")
        text.append(label, style="value" if enabled else "dim")
    return text


def _key_value(session: DashboardSession) -> Text:
    text = Text()
    if session.current_key:
        text.append("● ", style="ready")
        text.append("已就绪", style="value")
    else:
        text.append("○ ", style="dim")
        text.append("未设置", style="dim")
    return text


def _ensure_key_before_panel(session: DashboardSession) -> None:
    while True:
        if not session.current_key:
            session.current_key = prompt_runtime_llm_key()
        try:
            config = app_module._load_runtime_config(session.project_root, session.current_key)
            app_module._apply_runtime_preferences(
                config,
                concurrency=session.concurrency,
                site_timeout_seconds=session.site_timeout_seconds,
            )
            app_module._validate_llm_runtime(config)
            session.llm_base_url = config.llm_base_url
            app_module._persist_runtime_llm_key(session.project_root, session.current_key)
            return
        except (LlmConfigurationError, LlmTemporaryError) as exc:
            session.current_key = app_module._recover_runtime_llm_key(session.current_key, exc)


def _handle_start_crawl(session: DashboardSession) -> None:
    selected = _select_input_file(session)
    if selected is None:
        return
    session.selected_input = selected
    try:
        result = app_module.run_selected_input(
            session.project_root,
            session.current_key,
            selected,
            concurrency=session.concurrency,
            site_timeout_seconds=session.site_timeout_seconds,
            llm_base_url=session.llm_base_url,
            collect_email_enabled=session.collect_email_enabled,
            collect_phone_enabled=session.collect_phone_enabled,
            collect_company_name_enabled=session.collect_company_name_enabled,
            extract_representative_enabled=session.extract_representative_enabled,
            search_representative_enabled=session.search_representative_enabled,
        )
    except Exception as exc:  # noqa: BLE001
        log_path = _write_app_error_log(session, selected, exc)
        suffix = f"\n错误日志：{log_path}" if log_path is not None else ""
        _notice(f"抓取过程中出现未处理错误：{exc}{suffix}")
        return
    session.current_key = result.effective_key
    session.llm_base_url = result.llm_base_url or session.llm_base_url
    session.last_delivery_path = result.delivery_path
    session.last_failed_path = result.failed_path
    wait_for_enter(
        f"任务完成：\n成功文件：{result.delivery_path}\n失败文件：{result.failed_path}\n按回车返回主菜单。"
    )


def _select_input_file(session: DashboardSession) -> Path | None:
    files = list_input_files(_get_websites_dir(session.project_root))
    items = [MenuItem(str(index), path.name) for index, path in enumerate(files)]
    subtitle = f"共 {len(files)} 个可抓取表" if files else "把 txt/csv/xlsx 放进 websites 文件夹"
    spec = MenuSpec(
        title="选择爬取表",
        subtitle=subtitle,
        status_block=Text(
            f"当前文件 {session.selected_input.name if session.selected_input else '未选择'}",
            style="label",
        ),
        items=items,
        hint="↑↓ 移动   Enter 确认   Esc 返回",
        back_value=_BACK,
    )
    choice = run_menu(spec)
    if choice in (None, _BACK) or not files:
        return None
    return files[int(choice)]


def _handle_open_websites(session: DashboardSession) -> None:
    websites_dir = _get_websites_dir(session.project_root)
    files = list_input_files(websites_dir)
    lines = [Text(f"当前共 {len(files)} 个可抓取表", style="value"), Text()]
    lines.extend(Text(f"  {index}. {path.name}", style="label") for index, path in enumerate(files, start=1))
    lines.extend([Text(), Text(f"路径 {websites_dir}", style="hint")])
    _render_info("websites 文件夹", lines)
    _open_folder(websites_dir)
    wait_for_enter("已尝试打开 websites 文件夹，按回车返回主菜单。")


def _handle_open_output(session: DashboardSession) -> None:
    results = _list_output_results(session.project_root / "output")
    lines = [Text(f"当前共 {len(results)} 个结果文件", style="value"), Text()]
    lines.extend(Text(f"  {index}. {path.name}", style="label") for index, path in enumerate(results, start=1))
    if session.last_delivery_path is not None:
        lines.extend([Text(), Text(f"最近成功 {session.last_delivery_path.name}", style="ready")])
    if session.last_failed_path is not None:
        lines.append(Text(f"最近失败 {session.last_failed_path.name}", style="hint"))
    lines.extend([Text(), Text(f"路径 {session.project_root / 'output'}", style="hint")])
    _render_info("output 文件夹", lines)
    _open_folder(session.project_root / "output")
    wait_for_enter("已尝试打开 output 文件夹，按回车返回主菜单。")


def _handle_system_config(session: DashboardSession) -> str | None:
    while True:
        choice = run_menu(_config_menu_spec(session))
        if choice in (None, "back"):
            return None
        if choice == "key":
            if _handle_key_settings(session) == "exit":
                return "exit"
        elif choice == "concurrency":
            _handle_numeric_setting(
                title="并发设置",
                current_value=session.concurrency,
                min_value=1,
                max_value=64,
                apply_value=lambda value: setattr(session, "concurrency", value),
                description="请输入新的并发值，范围 1-64。",
            )
        elif choice == "timeout":
            _handle_numeric_setting(
                title="单站等待上限",
                current_value=session.site_timeout_seconds,
                min_value=60,
                max_value=600,
                apply_value=lambda value: setattr(session, "site_timeout_seconds", value),
                description="请输入新的秒数，范围 60-600。",
            )
        elif choice == "company":
            session.collect_company_name_enabled = not session.collect_company_name_enabled
            _notice(f"公司名已{'开启' if session.collect_company_name_enabled else '关闭'}。")
        elif choice == "email":
            session.collect_email_enabled = not session.collect_email_enabled
            _notice(f"邮箱已{'开启' if session.collect_email_enabled else '关闭'}。")
        elif choice == "phone":
            session.collect_phone_enabled = not session.collect_phone_enabled
            _notice(f"电话已{'开启' if session.collect_phone_enabled else '关闭'}。")
        elif choice == "rep":
            session.extract_representative_enabled = not session.extract_representative_enabled
            _notice(f"提取代表人已{'开启' if session.extract_representative_enabled else '关闭'}。")
        elif choice == "search":
            session.search_representative_enabled = not session.search_representative_enabled
            _notice(f"搜索现役最大代表人已{'开启' if session.search_representative_enabled else '关闭'}。")


def _config_menu_spec(session: DashboardSession) -> MenuSpec:
    return MenuSpec(
        title="系统配置",
        subtitle=_SUBTITLE,
        status_block=Group(
            kv_block(
                [
                    ("KEY", _key_value(session)),
                    ("并发", str(session.concurrency)),
                    ("单站超时", f"{session.site_timeout_seconds} 秒"),
                ]
            ),
            Text(),
            _toggles_line(session),
        ),
        items=[
            MenuItem("key", "Key 设置"),
            MenuItem("concurrency", "并发设置"),
            MenuItem("timeout", "单站等待上限"),
            MenuItem("company", "公司名（开/关切换）"),
            MenuItem("email", "邮箱（开/关切换）"),
            MenuItem("phone", "电话（开/关切换）"),
            MenuItem("rep", "提取代表人（开/关切换）"),
            MenuItem("search", "搜索现役最大代表人（开/关切换）"),
            MenuItem("back", "返回主菜单"),
        ],
        back_value="back",
    )


def _handle_key_settings(session: DashboardSession) -> str | None:
    while True:
        spec = MenuSpec(
            title="Key 设置",
            subtitle=_SUBTITLE,
            status_block=kv_block([("当前状态", _key_value(session))]),
            items=[
                MenuItem("change", "更换 Key"),
                MenuItem("delete", "删除 Key 并退出系统"),
                MenuItem("back", "返回系统配置"),
            ],
            back_value="back",
        )
        choice = run_menu(spec)
        if choice in (None, "back"):
            return None
        if choice == "change":
            session.current_key = prompt_runtime_llm_key()
            _ensure_key_before_panel(session)
            _notice("Key 已更新并鉴权成功。")
        elif choice == "delete":
            session.current_key = ""
            app_module._persist_runtime_llm_key(session.project_root, "")
            wait_for_enter("Key 已删除，系统将退出，请重新启动程序。")
            return "exit"


def _handle_numeric_setting(
    *,
    title: str,
    current_value: int,
    min_value: int,
    max_value: int,
    apply_value,
    description: str,
) -> None:
    while True:
        _render_info(
            title,
            [
                Text(f"当前值 {current_value}", style="value"),
                Text(description, style="label"),
                Text("输入 0 返回。", style="hint"),
            ],
        )
        raw = input("请输入新的数值: ").strip()
        if raw == "0":
            return
        try:
            value = int(raw)
        except ValueError:
            _notice("输入不是有效数字，请重新输入。")
            continue
        if value < min_value or value > max_value:
            _notice(f"输入超出范围，请输入 {min_value}-{max_value} 之间的数字。")
            continue
        apply_value(value)
        _notice(f"{title}已更新为 {value}。")
        return


def _list_output_results(output_dir: Path) -> list[Path]:
    if not output_dir.exists():
        return []
    results = []
    for item in output_dir.iterdir():
        if not item.is_file():
            continue
        if item.name.startswith("结果会输出到这里"):
            continue
        results.append(item)
    return sorted(results, key=lambda item: item.name.lower())


def _get_websites_dir(project_root: Path) -> Path:
    return resolve_websites_dir(project_root)


def _open_folder(path: Path) -> None:
    try:
        if os.name == "nt":
            os.startfile(str(path))
            return
        if os.name == "posix":
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        return


def _write_app_error_log(session: DashboardSession, selected: Path, exc: Exception) -> Path | None:
    try:
        log_dir = session.project_root / "output" / "runtime"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "app_error.log"
        timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        text = "\n".join(
            [
                "=" * 80,
                f"time: {timestamp}",
                f"input: {selected.name}",
                f"path: {selected}",
                f"error: {type(exc).__name__}: {exc}",
                "traceback:",
                stack.rstrip(),
                "",
            ]
        )
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(text)
        return log_path
    except Exception:
        return None


def _render_info(title: str, lines: list) -> None:
    console = get_console()
    clear_screen()
    console.print(screen(wordmark(title, _SUBTITLE), Text(), hairline(), Text(), Group(*lines)))


def _notice(message: str) -> None:
    _render_info("提示", [Text(message, style="value")])
    wait_for_enter("按回车继续。")
