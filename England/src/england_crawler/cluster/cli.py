"""England 集群 CLI。"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from england_crawler.cluster.config import ClusterConfig
from england_crawler.cluster.coordinator import CoordinatorRuntime
from england_crawler.cluster.db import ClusterDb
from england_crawler.cluster.export import export_cluster_snapshots
from england_crawler.cluster.migrate import migrate_england_history
from england_crawler.cluster.repository import ClusterRepository
from england_crawler.cluster.schema import initialize_schema


ROOT = Path(__file__).resolve().parents[3]


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="England 集群模式命令")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="初始化 England 集群数据库")
    sub.add_parser("migrate-england", help="把 England 现有 SQLite 与交付历史迁入 Postgres")
    export = sub.add_parser("export", help="把 Postgres 导出回 England/output")
    export.add_argument("--skip-delivery", action="store_true", help="跳过历史交付导出")
    sub.add_parser("coordinator", help="启动 England 集群协调器")
    submit = sub.add_parser("submit", help="向集群提交新任务")
    submit.add_argument("target", choices=["dnb", "companies-house"], help="提交目标")
    submit.add_argument("--input-xlsx", default=str(ROOT / "docs" / "英国.xlsx"), help="Companies House 输入文件")
    submit.add_argument("--max-companies", type=int, default=0, help="Companies House 最大导入数")
    sub.add_parser("worker", help="启动 England 集群 worker")
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
    if args.command == "submit":
        initialize_schema(db)
        repo = ClusterRepository(db, config)
        if args.target == "dnb":
            count = repo.submit_dnb_seed_tasks()
            print(f"England DNB 已提交种子任务：{count}")
            return 0
        count = repo.submit_companies_house_input(
            Path(args.input_xlsx),
            max_companies=max(int(args.max_companies or 0), 0),
        )
        print(f"England Companies House 已提交任务：{count}")
        return 0
    if args.command == "coordinator":
        CoordinatorRuntime(config).serve_forever()
        return 0
    if args.command == "worker":
        from england_crawler.cluster.worker import ClusterWorkerRuntime

        ClusterWorkerRuntime(config).run_forever()
        return 0
    print(f"不支持的集群命令：{args.command}")
    return 1
