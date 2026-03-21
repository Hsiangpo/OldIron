"""按日交付打包入口。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from malaysia_crawler.delivery import build_delivery_bundle  # noqa: E402


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("用法：python product.py dayN")
        return 1
    day_label = argv[0]
    db_path = ROOT / "output" / "runtime" / "malaysia_pipeline.db"
    delivery_root = ROOT / "output" / "delivery"
    try:
        summary = build_delivery_bundle(
            db_path=db_path,
            delivery_root=delivery_root,
            day_label=day_label,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"执行失败：{exc}")
        return 1
    day = int(summary["day"])
    print(
        "交付完成：day{day}，基线 day{baseline}，当日增量 {delta}，当前总量 {total}".format(
            day=day,
            baseline=int(summary["baseline_day"]),
            delta=int(summary["delta_companies"]),
            total=int(summary["total_current_companies"]),
        )
    )
    print(f"目录：{delivery_root / f'Malaysia_day{day:03d}'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
