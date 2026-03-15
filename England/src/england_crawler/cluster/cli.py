"""England 集群 CLI。"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from england_crawler.cluster.config import ClusterConfig
from england_crawler.cluster.coordinator import CoordinatorRuntime
from england_crawler.cluster.db import ClusterDb
from england_crawler.cluster.export import export_cluster_snapshots
from england_crawler.cluster.migrate import migrate_england_history
from england_crawler.cluster.repository import ClusterRepository
from england_crawler.cluster.schema import initialize_schema
from england_crawler.cluster.worker import WORKER_ROLE_CAPABILITIES
from england_crawler.delivery import build_delivery_bundle


ROOT = Path(__file__).resolve().parents[3]


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
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


def _is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _role_counts(config: ClusterConfig) -> list[tuple[str, int]]:
    return [(role, count) for role, count in config.build_worker_role_counts() if count > 0]


def _start_local_worker_pools(config: ClusterConfig) -> int:
    launched = 0
    for role, count in _role_counts(config):
        for index in range(1, count + 1):
            pid_file = _pid_path(role, index)
            if pid_file.exists():
                try:
                    pid = int(pid_file.read_text(encoding="utf-8").strip())
                except ValueError:
                    pid = 0
                if _is_running(pid):
                    continue
                pid_file.unlink(missing_ok=True)
            env = os.environ.copy()
            env["ENGLAND_CLUSTER_POSTGRES_DSN"] = config.postgres_dsn
            env["ENGLAND_CLUSTER_BASE_URL"] = config.coordinator_base_url
            env["ENGLAND_CLUSTER_WORKER_ID"] = f"{config.worker_id}-{role}-{index}"
            cmd = [
                sys.executable,
                str(ROOT / "run.py"),
                "cluster",
                "worker",
                role,
                "--worker-index",
                str(index),
            ]
            with _stdout_path(role, index).open("w", encoding="utf-8") as out_fp, _stderr_path(role, index).open("w", encoding="utf-8") as err_fp:
                proc = subprocess.Popen(  # noqa: S603
                    cmd,
                    cwd=str(ROOT),
                    env=env,
                    stdout=out_fp,
                    stderr=err_fp,
                )
            pid_file.write_text(str(proc.pid), encoding="utf-8")
            launched += 1
    return launched


def _stop_local_worker_pools() -> int:
    stopped = 0
    for pid_file in _runtime_dir().glob("*.pid"):
        try:
            pid = int(pid_file.read_text(encoding="utf-8").strip())
        except ValueError:
            pid = 0
        if _is_running(pid):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
            time.sleep(0.3)
            if _is_running(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
            stopped += 1
        pid_file.unlink(missing_ok=True)
    return stopped


def _print_cluster_status(db: ClusterDb) -> None:
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT worker_id, status, last_heartbeat_at FROM cluster_workers ORDER BY worker_id")
            workers = cur.fetchall()
            cur.execute("SELECT task_type, status, COUNT(*) AS count FROM england_cluster_tasks GROUP BY task_type, status ORDER BY task_type, status")
            task_rows = cur.fetchall()
    print("Workers:")
    for row in workers:
        print(f"  {row['worker_id']} | {row['status']} | {row['last_heartbeat_at']}")
    print("Tasks:")
    for row in task_rows:
        print(f"  {row['task_type']} | {row['status']} | {row['count']}")


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
    submit.add_argument("target", choices=["dnb", "companies-house"], help="提交目标")
    submit.add_argument("--input-xlsx", default=str(ROOT / "docs" / "英国.xlsx"), help="Companies House 输入文件")
    submit.add_argument("--max-companies", type=int, default=0, help="Companies House 最大导入数")
    worker = sub.add_parser("worker", help="启动单个 England 集群 worker")
    worker.add_argument("role", choices=sorted(WORKER_ROLE_CAPABILITIES.keys()), help="worker 角色")
    worker.add_argument("--worker-index", type=int, default=1, help="worker 编号")
    sub.add_parser("start-pools", help="按配置启动本机全部 worker 池")
    sub.add_parser("stop-pools", help="停止本机全部 worker 池")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    return parser


def run_cluster(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    config = ClusterConfig.from_env(ROOT)
    db = ClusterDb(config.postgres_dsn)
    if args.command == "init-db":
        initialize_schema(db)
        print("England 集群数据库初始化完成。")
        return 0
    if args.command == "migrate-england":
        migrate_england_history(db, ROOT / "output")
        print("England 历史数据迁移完成。")
        return 0
    if args.command == "export":
        export_cluster_snapshots(db, ROOT / "output", include_delivery=not bool(args.skip_delivery))
        print("England Postgres 快照已导出到 output。")
        return 0
    if args.command == "produce":
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
        initialize_schema(db)
        repo = ClusterRepository(db, config)
        if args.target == "dnb":
            count = repo.submit_dnb_seed_tasks()
            print(f"England DNB 新增种子任务：{count}")
            return 0
        count = repo.submit_companies_house_input(
            Path(args.input_xlsx),
            max_companies=max(int(args.max_companies or 0), 0),
        )
        print(f"England Companies House 新增任务：{count}")
        return 0
    if args.command == "status":
        _print_cluster_status(db)
        return 0
    if args.command == "coordinator":
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
        launched = _start_local_worker_pools(config)
        print(f"England 本机 worker 池已启动：{launched}")
        return 0
    if args.command == "stop-pools":
        stopped = _stop_local_worker_pools()
        print(f"England 本机 worker 池已停止：{stopped}")
        return 0
    print(f"不支持的集群命令：{args.command}")
    return 1
