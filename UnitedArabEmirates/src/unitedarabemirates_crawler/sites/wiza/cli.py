"""Wiza CLI。"""

from __future__ import annotations

from pathlib import Path

from oldiron_core.snov import SnovServiceSettings

from ..common import run_site_cli
from .client import WizaUsageLimitError
from .pipeline import run_pipeline_list
from .snov_pipeline import run_pipeline_snov


SITE_ROOT = Path(__file__).resolve().parents[4]


def run_site(argv: list[str]) -> int:
    try:
        _validate_snov_before_run(argv)
        return run_site_cli(
            site_name="wiza",
            description="Wiza 阿联酋企业采集",
            output_dir=SITE_ROOT / "output" / "wiza",
            argv=argv,
            run_list=run_pipeline_list,
            run_email=run_pipeline_snov,
            enable_gmap=False,
        )
    except WizaUsageLimitError as exc:
        print(f"Wiza 暂停：{exc}")
        print("当前会保留已有登录态和数据库；等 Wiza 恢复后，重跑同一条命令即可续上。")
        return 1
    except RuntimeError as exc:
        print(f"Wiza Snov 暂停：{exc}")
        return 1


def _validate_snov_before_run(argv: list[str]) -> None:
    mode = str(argv[0] or "all").strip().lower() if argv else "all"
    if mode not in {"all", "email"}:
        return
    settings = SnovServiceSettings.from_env()
    settings.validate(require_llm=True)
