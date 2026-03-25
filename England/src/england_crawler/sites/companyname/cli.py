"""CompanyName CLI 入口。"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from england_crawler.sites.companyname.config import CompanyNameConfig
from england_crawler.sites.companyname.pipeline import run_companyname_pipeline


LOGGER = logging.getLogger(__name__)


def run_companyname(argv: list[str]) -> int:
    """CLI 入口。"""
    parser = argparse.ArgumentParser(description="England CompanyName 管线")
    parser.add_argument("--gmap-workers", type=int, default=128, help="Google Maps 并发数")
    parser.add_argument("--email-workers", dest="firecrawl_workers", type=int, default=64, help="官网爬虫/邮箱补充并发数")
    parser.add_argument("--skip-gmap", action="store_true", help="跳过 Google Maps 补官网阶段")
    parser.add_argument("--skip-email", dest="skip_firecrawl", action="store_true", help="跳过官网爬虫邮箱补充阶段")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    parser.add_argument("--reseed", action="store_true", help="强制重新从 Excel 读取公司名")
    parser.add_argument("--excel", action="append", dest="excel_files", default=None, help="额外 Excel 文件路径（可多次指定）")
    args = parser.parse_args(argv)

    # 项目根目录
    project_root = Path(__file__).resolve().parents[4]  # England/
    output_dir = project_root / "output" / "companyname"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "run.log"

    # 日志
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(log_path), encoding="utf-8"),
        ],
    )

    # 默认 Excel 文件
    docs_dir = project_root / "docs"
    default_excels = [
        docs_dir / "AllCompanyNames.xlsx",
        docs_dir / "英国.xlsx",
    ]
    excel_files = [Path(f) for f in (args.excel_files or [])]
    if not excel_files:
        excel_files = default_excels

    # 构建配置
    config = CompanyNameConfig.from_env(
        project_root=project_root,
        output_dir=output_dir,
        excel_files=excel_files,
        gmap_workers=args.gmap_workers,
        firecrawl_workers=args.firecrawl_workers,
    )
    config.reseed = args.reseed

    LOGGER.info("=== England CompanyName 管线启动 ===")
    LOGGER.info("GMap 并发: %d | 邮箱并发: %d", config.gmap_workers, config.firecrawl_workers)
    LOGGER.info("Excel 文件: %s", [str(f) for f in config.excel_files])
    LOGGER.info("数据库: %s", config.store_db_path)
    LOGGER.info("LLM 模型: %s", config.llm_model)

    try:
        run_companyname_pipeline(
            config=config,
            skip_gmap=args.skip_gmap,
            skip_firecrawl=args.skip_firecrawl,
        )
    except KeyboardInterrupt:
        LOGGER.info("用户中断，安全退出")
    except Exception:
        LOGGER.exception("管线异常")
        return 1
    return 0
