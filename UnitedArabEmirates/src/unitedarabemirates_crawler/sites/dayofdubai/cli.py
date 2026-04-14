"""Day Of Dubai CLI。"""

from __future__ import annotations

from pathlib import Path

from ..common import run_site_cli
from .pipeline import run_pipeline_list


SITE_ROOT = Path(__file__).resolve().parents[4]


def run_site(argv: list[str]) -> int:
    return run_site_cli(
        site_name="dayofdubai",
        description="Day Of Dubai 阿联酋企业采集",
        output_dir=SITE_ROOT / "output" / "dayofdubai",
        argv=argv,
        run_list=run_pipeline_list,
    )
