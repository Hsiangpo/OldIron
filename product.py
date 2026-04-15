"""OldIron 根交付入口。"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
COUNTRY_BUILDERS: dict[str, str] = {
    "England": "england_crawler.delivery",
    "Brazil": "brazil_crawler.delivery",
    "Denmark": "denmark_crawler.delivery",
    "Germany": "germany_crawler.delivery",
    "Finland": "finland_crawler.delivery",
    "Japan": "japan_crawler.delivery",
    "Taiwan": "taiwan_crawler.delivery",
    "UnitedStates": "unitedstates_crawler.delivery",
    "UnitedArabEmirates": "unitedarabemirates_crawler.delivery",
}


def _usage() -> int:
    print("用法：python product.py <国家目录名> dayN")
    print("示例：python product.py England day1")
    return 1


def _country_root(country: str) -> Path:
    return ROOT / str(country or "").strip()


def _import_country_builder(country: str):
    module_name = COUNTRY_BUILDERS.get(country)
    if not module_name:
        return None
    country_src = _country_root(country) / "src"
    if str(country_src) not in sys.path:
        sys.path.insert(0, str(country_src))
    module = importlib.import_module(module_name)
    return getattr(module, "build_delivery_bundle", None)


def _run_shared_country_delivery(country: str, day_label: str) -> int:
    builder = _import_country_builder(country)
    if builder is None:
        return _run_legacy_country_delivery(country, day_label)
    country_dir = _country_root(country)
    delivery_root = country_dir / "output" / "delivery"
    try:
        summary = builder(
            data_root=country_dir / "output",
            delivery_root=delivery_root,
            day_label=day_label,
        )
    except Exception as exc:
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
    print(f"目录：{delivery_root / f'{country}_day{day:03d}'}")
    return 0


def _run_legacy_country_delivery(country: str, day_label: str) -> int:
    country_dir = _country_root(country)
    if not country_dir.is_dir():
        print(f"错误：找不到指定的国家目录 '{country}'")
        return 1
    country_product = country_dir / "product.py"
    if not country_product.exists():
        print(f"错误：该国家目录下缺少交付脚本 '{country_product}'")
        return 1
    try:
        result = subprocess.run(
            [sys.executable, "product.py", day_label],
            cwd=country_dir,
            check=False,
        )
    except KeyboardInterrupt:
        print("中断：用户取消了交付过程。")
        return 1
    except Exception as exc:
        print(f"执行失败：{exc}")
        return 1
    if result.returncode != 0:
        print(f"交付失败：{country} 返回值 {result.returncode}")
    return int(result.returncode)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        return _usage()
    country = str(argv[0] or "").strip()
    day_label = str(argv[1] or "").strip()
    if not country or not day_label:
        return _usage()
    return _run_shared_country_delivery(country, day_label)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
