"""England 集群 CLI。"""

from __future__ import annotations

import argparse
import locale
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import json

import requests

from england_crawler.cluster.config import ClusterConfig
from england_crawler.cluster.coordinator import CoordinatorRuntime
from england_crawler.cluster.db import ClusterDb
from england_crawler.cluster.export import export_cluster_snapshots
from england_crawler.cluster.migrate import migrate_england_history
from england_crawler.cluster.repository import ClusterRepository
from england_crawler.cluster.schema import initialize_schema
from england_crawler.cluster.sources import submit_england_sources
from england_crawler.cluster.worker import WORKER_ROLE_CAPABILITIES
from england_crawler.delivery import build_delivery_bundle


ROOT = Path(__file__).resolve().parents[3]
logger = logging.getLogger(__name__)


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def _runtime_dir() -> Path:
    path = ROOT / "output" / "cluster_runtime" / "pools"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _pid_path(role: str, index: int) -> Path:
    return _runtime_dir() / f"{role}.{index}.pid"


def _stdout_path(role: str, index: int) -> Path:
    return _runtime_dir() / f"{role}.{index}.out.log"


def _stderr_path(role: str, index: int) -> Path:
    return _runtime_dir() / f"{role}.{index}.err.log"


def _worker_command_markers(role: str, index: int) -> list[str]:
    return [
        "run.py cluster worker",
        f"worker {role}",
        f"--worker-index {index}",
    ]


def _decode_process_output(data: bytes) -> str:
    if not data:
        return ""
    preferred = locale.getpreferredencoding(False) or "utf-8"
    for encoding in (preferred, "utf-8", "gbk", "mbcs"):
        try:
            return data.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return data.decode("utf-8", errors="ignore")


def _run_captured_command(args: list[str]) -> tuple[str, str]:
    result = subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=False,
    )
    return _decode_process_output(result.stdout), _decode_process_output(result.stderr)


def _process_command_line(pid: int) -> str:
    if pid <= 0:
        return ""
    if os.name == "nt":
        command = (
            "Get-CimInstance Win32_Process -Filter \\\"ProcessId = {pid}\\\" | "
            "Select-Object -ExpandProperty CommandLine"
        ).format(pid=pid)
        stdout, _ = _run_captured_command(["powershell", "-NoProfile", "-Command", command])
        return stdout.strip()
    stdout, _ = _run_captured_command(["ps", "-o", "command=", "-p", str(pid)])
    return stdout.strip()


def _is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        stdout, _ = _run_captured_command(["tasklist", "/FI", f"PID eq {pid}"])
        return str(pid) in stdout
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _is_expected_worker_process(pid: int, *, role: str, index: int) -> bool:
    if not _is_running(pid):
        return False
    command_line = _process_command_line(pid).lower()
    if not command_line:
        return False
    markers = [item.lower() for item in _worker_command_markers(role, index)]
    return all(marker in command_line for marker in markers)


def _terminate_pid(pid: int) -> None:
    if pid <= 0:
        return
    if os.name == "nt":
        _run_captured_command(["taskkill", "/PID", str(pid), "/T", "/F"])
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return


def _spawn_worker_process(
    config: ClusterConfig,
    *,
    role: str,
    index: int,
    detach: bool,
):
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["ENGLAND_CLUSTER_POSTGRES_DSN"] = config.postgres_dsn
    env["ENGLAND_CLUSTER_BASE_URL"] = config.coordinator_base_url
    env["ENGLAND_CLUSTER_WORKER_ID"] = f"{config.worker_id}-{role}-{index}"
    cmd = [
        sys.executable,
        "-u",
        str(ROOT / "run.py"),
        "cluster",
        "worker",
        role,
        "--worker-index",
        str(index),
    ]
    if detach:
        with _stdout_path(role, index).open("w", encoding="utf-8") as out_fp, _stderr_path(role, index).open("w", encoding="utf-8") as err_fp:
            return subprocess.Popen(  # noqa: S603
                cmd,
                cwd=str(ROOT),
                env=env,
                stdout=out_fp,
                stderr=err_fp,
                start_new_session=True,
            )
    return subprocess.Popen(  # noqa: S603
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )


def _stream_process_output(process: subprocess.Popen, *, role: str, index: int) -> threading.Thread:
    prefix = f"[{role}.{index}] "
    log_path = _stdout_path(role, index)

    def _runner() -> None:
        if process.stdout is None:
            return
        with log_path.open("a", encoding="utf-8") as fp:
            for line in process.stdout:
                text = line.rstrip("\n")
                if not text:
                    continue
                rendered = prefix + text
                print(rendered, flush=True)
                fp.write(rendered + "\n")

    thread = threading.Thread(target=_runner, name=f"Stream-{role}-{index}", daemon=True)
    thread.start()
    return thread


def _role_counts(config: ClusterConfig) -> list[tuple[str, int]]:
    return [(role, count) for role, count in config.build_worker_role_counts() if count > 0]


def _coordinator_healthz_url(config: ClusterConfig) -> str:
    return config.coordinator_base_url.rstrip("/") + "/healthz"


def _coordinator_is_healthy(config: ClusterConfig) -> bool:
    if sys.platform == "darwin":
        result = subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--max-time",
                "3",
                "--noproxy",
                "*",
                "-w",
                "\n%{http_code}",
                _coordinator_healthz_url(config),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            return False
        _, _, status_text = result.stdout.rpartition("\n")
        return str(status_text).strip() == "200"
    session = requests.Session()
    session.trust_env = False
    try:
        response = session.get(_coordinator_healthz_url(config), timeout=3.0)
        return int(response.status_code or 0) == 200
    except requests.RequestException:
        return False
    finally:
        session.close()


def _start_local_worker_pools(config: ClusterConfig, *, detach: bool) -> tuple[int, int, list[subprocess.Popen], list[threading.Thread]]:
    launched = 0
    already_running = 0
    processes: list[subprocess.Popen] = []
    threads: list[threading.Thread] = []
    for role, count in _role_counts(config):
        for index in range(1, count + 1):
            pid_file = _pid_path(role, index)
            if pid_file.exists():
                try:
                    pid = int(pid_file.read_text(encoding="utf-8").strip())
                except ValueError:
                    pid = 0
                if _is_expected_worker_process(pid, role=role, index=index):
                    already_running += 1
                    continue
                pid_file.unlink(missing_ok=True)
            proc = _spawn_worker_process(config, role=role, index=index, detach=detach)
            pid_file.write_text(str(proc.pid), encoding="utf-8")
            processes.append(proc)
            if not detach:
                threads.append(_stream_process_output(proc, role=role, index=index))
            launched += 1
            if config.worker_startup_delay_seconds > 0:
                time.sleep(config.worker_startup_delay_seconds)
    return launched, already_running, processes, threads


def _stop_local_worker_pools() -> int:
    stopped = 0
    for pid_file in _runtime_dir().glob("*.pid"):
        stem_parts = pid_file.stem.rsplit(".", 1)
        role = stem_parts[0] if stem_parts else ""
        try:
            index = int(stem_parts[1]) if len(stem_parts) == 2 else 0
        except ValueError:
            index = 0
        try:
            pid = int(pid_file.read_text(encoding="utf-8").strip())
        except (FileNotFoundError, ValueError):
            pid = 0
        if role and index > 0 and _is_expected_worker_process(pid, role=role, index=index):
            _terminate_pid(pid)
            stopped += 1
        pid_file.unlink(missing_ok=True)
    return stopped


def _run_pool_supervisor(processes: list[subprocess.Popen], threads: list[threading.Thread]) -> int:
    try:
        while True:
            alive = 0
            for process in processes:
                if process.poll() is None:
                    alive += 1
            if alive == 0:
                break
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("收到 Ctrl+C，正在停止 worker 池...", flush=True)
        for process in processes:
            if process.poll() is None:
                _terminate_pid(int(process.pid))
    finally:
        for thread in threads:
            thread.join(timeout=1.0)
        for pid_file in _runtime_dir().glob("*.pid"):
            stem_parts = pid_file.stem.rsplit(".", 1)
            role = stem_parts[0] if stem_parts else ""
            try:
                index = int(stem_parts[1]) if len(stem_parts) == 2 else 0
            except ValueError:
                index = 0
            try:
                pid = int(pid_file.read_text(encoding="utf-8").strip())
            except ValueError:
                pid = 0
            if not role or index <= 0 or not _is_expected_worker_process(pid, role=role, index=index):
                pid_file.unlink(missing_ok=True)
    return 0


def _print_cluster_status(db: ClusterDb, config: ClusterConfig) -> None:
    ClusterRepository(db, config).requeue_expired_tasks()
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT worker_id, status, last_heartbeat_at
                FROM cluster_workers
                ORDER BY
                    CASE WHEN status = 'online' THEN 0 ELSE 1 END,
                    last_heartbeat_at DESC,
                    worker_id ASC
                """
            )
            workers = cur.fetchall()
            cur.execute("SELECT task_type, status, COUNT(*) AS count FROM england_cluster_tasks GROUP BY task_type, status ORDER BY task_type, status")
            task_rows = cur.fetchall()
    print("Workers:")
    for row in workers:
        print(f"  {row['worker_id']} | {row['status']} | {row['last_heartbeat_at']}")
    print("Tasks:")
    for row in task_rows:
        print(f"  {row['task_type']} | {row['status']} | {row['count']}")


def _normalize_submit_target(raw: str) -> str:
    value = str(raw or "").strip().lower().replace("_", "-")
    if value in {"companieshouse", "companies-house"}:
        return "companies-house"
    return value


def _print_submit_outcomes(lines: list[str]) -> None:
    print("England 任务补种完成：")
    for line in lines:
        print(f"  {line}")


def _build_cluster_db(config: ClusterConfig) -> ClusterDb:
    return ClusterDb(
        config.postgres_dsn,
        min_size=config.db_pool_min_size,
        max_size=config.db_pool_max_size,
        timeout_seconds=config.db_pool_timeout_seconds,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="England 集群模式命令")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="初始化 England 集群数据库")
    sub.add_parser("migrate-england", help="把 England 现有 SQLite 与交付历史迁入 Postgres")
    sub.add_parser("status", help="查看 England 集群状态")
    export = sub.add_parser("export", help="把 Postgres 导出回 England/output")
    export.add_argument("--skip-delivery", action="store_true", help="跳过历史交付导出")
    produce = sub.add_parser("produce", help="先导出快照，再执行 day 交付")
    produce.add_argument("day_label", help="交付日，例如 day2")
    sub.add_parser("coordinator", help="启动 England 集群协调器")
    submit = sub.add_parser("submit", help="向集群提交新任务")
    submit.add_argument("target", help="提交目标（dnb / companies-house / England）")
    submit.add_argument("--input-xlsx", default=str(ROOT / "docs" / "英国.xlsx"), help="Companies House 输入文件")
    submit.add_argument("--max-companies", type=int, default=0, help="Companies House 最大导入数")
    worker = sub.add_parser("worker", help="启动单个 England 集群 worker")
    worker.add_argument("role", choices=sorted(WORKER_ROLE_CAPABILITIES.keys()), help="worker 角色")
    worker.add_argument("--worker-index", type=int, default=1, help="worker 编号")
    start = sub.add_parser("start-pools", help="按配置启动本机全部 worker 池")
    start.add_argument("--detach", action="store_true", help="后台启动 worker 池并写日志到文件")
    sub.add_parser("stop-pools", help="停止本机全部 worker 池")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    return parser


def run_cluster(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    config = ClusterConfig.from_env(ROOT)
    if args.command == "init-db":
        db = _build_cluster_db(config)
        initialize_schema(db)
        print("England 集群数据库初始化完成。")
        return 0
    if args.command == "migrate-england":
        db = _build_cluster_db(config)
        migrate_england_history(db, ROOT / "output")
        print("England 历史数据迁移完成。")
        return 0
    if args.command == "export":
        db = _build_cluster_db(config)
        export_cluster_snapshots(db, ROOT / "output", include_delivery=not bool(args.skip_delivery))
        print("England Postgres 快照已导出到 output。")
        return 0
    if args.command == "produce":
        db = _build_cluster_db(config)
        export_cluster_snapshots(db, ROOT / "output", include_delivery=True)
        summary = build_delivery_bundle(
            data_root=ROOT / "output",
            delivery_root=ROOT / "output" / "delivery",
            day_label=str(args.day_label),
        )
        print(
            "交付完成：day{day}，基线 day{baseline}，当日增量 {delta}，当前总量 {total}".format(
                day=int(summary["day"]),
                baseline=int(summary["baseline_day"]),
                delta=int(summary["delta_companies"]),
                total=int(summary["total_current_companies"]),
            )
        )
        day_value = int(summary["day"])
        print(f"目录：{ROOT / 'output' / 'delivery' / f'England_day{day_value:03d}'}")
        return 0
    if args.command == "submit":
        db = _build_cluster_db(config)
        initialize_schema(db)
        repo = ClusterRepository(db, config)
        repo.reconcile_company_backed_task_states()
        target = _normalize_submit_target(str(args.target))
        if target == "dnb":
            count = repo.submit_dnb_seed_tasks()
            print(f"England DNB 新增种子任务：{count}")
            return 0
        if target == "companies-house":
            count = repo.submit_companies_house_input(
                Path(args.input_xlsx),
                max_companies=max(int(args.max_companies or 0), 0),
            )
            print(f"England Companies House 新增任务：{count}")
            return 0
        if target == "england":
            outcomes = submit_england_sources(
                repo,
                input_xlsx=Path(args.input_xlsx),
                max_companies=max(int(args.max_companies or 0), 0),
            )
            _print_submit_outcomes([item.render() for item in outcomes])
            return 0
        parser.error(f"不支持的提交目标：{args.target}")
        return 1
    if args.command == "status":
        db = _build_cluster_db(config)
        _print_cluster_status(db, config)
        return 0
    if args.command == "coordinator":
        logger.info(
            "England 集群配置已加载：env=%s dsn=%s base_url=%s",
            ROOT / ".env",
            config.postgres_dsn,
            config.coordinator_base_url,
        )
        CoordinatorRuntime(config).serve_forever()
        return 0
    if args.command == "worker":
        from england_crawler.cluster.worker import ClusterWorkerRuntime

        ClusterWorkerRuntime(
            config,
            role=str(args.role),
            worker_index=max(int(args.worker_index or 1), 1),
            worker_id=os.getenv("ENGLAND_CLUSTER_WORKER_ID", "").strip(),
        ).run_forever()
        return 0
    if args.command == "start-pools":
        if not _coordinator_is_healthy(config):
            print(f"England 协调器未就绪：{_coordinator_healthz_url(config)}")
            return 1
        launched, already_running, processes, threads = _start_local_worker_pools(config, detach=bool(args.detach))
        print(f"England 本机 worker 池已启动：新增 {launched}，已在运行 {already_running}")
        if args.detach:
            return 0
        if launched == 0:
            print("当前没有新增 worker。若想重新接管日志，先执行 cluster stop-pools。")
            return 0
        return _run_pool_supervisor(processes, threads)
    if args.command == "stop-pools":
        stopped = _stop_local_worker_pools()
        print(f"England 本机 worker 池已停止：{stopped}")
        return 0
    print(f"不支持的集群命令：{args.command}")
    return 1
