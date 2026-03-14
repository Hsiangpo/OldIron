"""Cliproxy 端口连通性探针。"""

from __future__ import annotations

import argparse
import logging
import time

from curl_cffi import requests as cffi_requests

from .pool import ProxyPool, build_proxy_pool_from_env

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""
    parser = argparse.ArgumentParser(
        prog="python run.py probe-proxy",
        description="使用 .env 中 AHU 代理配置做连通性探针",
    )
    parser.add_argument("--prefix", default="AHU", help="代理配置前缀，默认 AHU")
    parser.add_argument(
        "--targets",
        default="http://httpbin.org/ip,https://www.gstatic.com/generate_204",
        help="探测目标，逗号分隔",
    )
    parser.add_argument("--timeout", type=float, default=8.0, help="单次探测超时秒数")
    parser.add_argument("--interval", type=float, default=30.0, help="轮询间隔秒数")
    parser.add_argument("--rounds", type=int, default=1, help="探测轮数，0 表示无限轮询")
    parser.add_argument(
        "--stop-on-success",
        action="store_true",
        help="任一端口成功后立即退出",
    )
    return parser


def _parse_targets(raw: str) -> list[str]:
    """解析目标列表。"""
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if values:
        return values
    return ["http://httpbin.org/ip"]


def _collect_leases(pool: ProxyPool) -> list[tuple[int, str, str]]:
    """从代理池收集每个节点的一次租约。"""
    leases: dict[int, tuple[str, str]] = {}
    attempts = max(3, pool.size * 3)
    for _ in range(attempts):
        lease = pool.acquire()
        if lease is None:
            continue
        if lease.endpoint_id not in leases:
            leases[lease.endpoint_id] = (lease.label, lease.proxy_url)
        if len(leases) >= pool.size:
            break
    return [(endpoint_id, label, proxy_url) for endpoint_id, (label, proxy_url) in sorted(leases.items())]


def _probe_once(
    session: cffi_requests.Session,
    proxy_url: str,
    target: str,
    timeout: float,
) -> tuple[bool, str]:
    """执行一次请求探测。"""
    try:
        response = session.get(
            target,
            proxy=proxy_url,
            timeout=timeout,
            headers={"accept": "*/*"},
        )
        return True, f"HTTP {response.status_code}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def run_proxy_probe(argv: list[str]) -> int:
    """执行代理探针。"""
    parser = _build_parser()
    args = parser.parse_args(argv)

    targets = _parse_targets(args.targets)
    pool = build_proxy_pool_from_env(prefix=args.prefix)
    if pool is None or not pool.enabled:
        print(f"未检测到 {args.prefix}_PROXY 配置，无法探测。")
        return 1

    leases = _collect_leases(pool)
    if not leases:
        print("代理池为空，无法探测。")
        return 1

    print(f"探测节点数: {len(leases)}，目标数: {len(targets)}，轮数: {args.rounds if args.rounds > 0 else '无限'}")
    print(f"单次超时: {args.timeout:.1f}s，轮询间隔: {args.interval:.1f}s")

    session = cffi_requests.Session(impersonate="chrome110")
    round_idx = 0
    any_success = False
    try:
        while True:
            round_idx += 1
            now = time.strftime("%H:%M:%S")
            print(f"\n===== 第 {round_idx} 轮 @ {now} =====")

            success_count = 0
            total = 0
            for endpoint_id, label, proxy_url in leases:
                for target in targets:
                    total += 1
                    ok, message = _probe_once(session, proxy_url, target, max(1.0, float(args.timeout)))
                    status = "OK" if ok else "FAIL"
                    print(f"[{status}] 节点#{endpoint_id} {label} -> {target} | {message}")
                    if ok:
                        success_count += 1

            any_success = any_success or (success_count > 0)
            print(f"本轮结果: 成功 {success_count}/{total}")

            if args.stop_on_success and success_count > 0:
                print("检测到可用端口，按 --stop-on-success 提前退出。")
                return 0

            if args.rounds > 0 and round_idx >= args.rounds:
                break

            time.sleep(max(1.0, float(args.interval)))
    finally:
        session.close()

    if any_success:
        print("探测完成：至少有一个端口可用。")
        return 0

    print("探测完成：未发现可用端口。")
    return 2

