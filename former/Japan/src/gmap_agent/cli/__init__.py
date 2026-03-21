from __future__ import annotations

import argparse
import asyncio
import json
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import datetime
from pathlib import Path
from typing import Iterable

from ..config import DEFAULT_BASE_URL, DEFAULT_BBOX, DEFAULT_QUERY, DEFAULT_SEARCH_PB
from ..crawler import GoogleMapsSearcher, SearchConfig
from ..http_client import HttpClient, HttpConfig
from ..models import PlaceRecord
from ..output_writer import append_jsonl, load_existing_records, write_csv, write_json, write_jsonl
from ..pb_capture import capture_tbm_map_pb
from ..query import load_queries

_QUERY_CHECKPOINT_FILE = "query_checkpoint.json"


def _print_ts(message: str, *, flush: bool = True) -> None:
    text = str(message or "")
    if not text:
        return
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} {text}", flush=flush)


def main() -> None:
    args = _parse_args()
    if args.query_file and not Path(args.query_file).exists():
        raise SystemExit(f"query-file 不存在: {args.query_file}")
    query_arg = args.query
    if args.query_file and args.query == DEFAULT_QUERY:
        query_arg = None
    queries = load_queries(query_arg, args.query_file)
    if not queries:
        raise SystemExit("关键词不能为空，请提供 --query 或 --query-file")
    pb_template = args.search_pb
    if args.search_sourceurl:
        parsed_pb = parse_sourceurl(args.search_sourceurl)
        pb_template = pb_template or parsed_pb
    pb_template = pb_template or DEFAULT_SEARCH_PB
    if not pb_template:
        raise SystemExit("缺少搜索参数 pb；请提供 --search-pb 或 --search-sourceurl")
    if args.phone_enrich:
        _print_ts("[策略] 已开启电话补抓：缺电话记录将按 CID 请求详情页", flush=True)

    http = HttpClient(
        HttpConfig(
            hl=args.hl,
            gl=args.gl,
            cookie=args.cookie,
            proxy=args.proxy,
            timeout=args.timeout,
            max_retries=args.retries,
            backoff_seconds=args.backoff,
        )
    )

    run_dir = Path(args.run_dir) if args.run_dir else _build_run_dir(Path(args.output_dir))
    run_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = run_dir / "places_with_websites.jsonl"
    json_path = run_dir / "places_with_websites.json"
    csv_path = run_dir / "places_with_websites.csv"

    existing: list[PlaceRecord] = []
    if args.resume:
        existing = load_existing_records(jsonl_path)
        if existing:
            _print_ts(f"[续跑] 已加载 {len(existing)} 条历史记录", flush=True)

    start_index = 0
    if args.resume:
        start_index = _load_query_checkpoint(run_dir)
        if start_index:
            _print_ts(f"[续跑] 查询进度：{start_index}/{len(queries)}", flush=True)

    _prepare_incremental_output(jsonl_path, existing=existing, resume=args.resume)

    centers = list(_iter_centers(args))
    if not centers:
        raise SystemExit("没有可搜索的中心点")
    total_centers = len(centers)

    existing_count = len(existing)
    results = _run_search(
        args,
        http,
        pb_template,
        existing,
        centers,
        total_centers,
        queries,
        run_dir=run_dir,
        start_index=start_index,
        incremental_jsonl_path=jsonl_path,
    )
    new_count = len(results) - existing_count
    if new_count <= 0:
        _print_ts("[提示] 本次未获取到新的官网，尝试重新抓取 pb 参数并重试…", flush=True)
        refreshed = _capture_pb_sync(args, queries[0])
        if refreshed:
            pb_template = refreshed
            results = _run_search(
                args,
                http,
                pb_template,
                existing,
                centers,
                total_centers,
                queries,
                run_dir=run_dir,
                start_index=start_index,
                incremental_jsonl_path=jsonl_path,
            )
        else:
            _print_ts("[提示] 自动抓取 pb 失败，继续使用当前参数输出结果。", flush=True)

    write_jsonl(jsonl_path, results)
    write_json(json_path, results)
    write_csv(csv_path, results)

    _print_ts(f"[完成] 记录数={len(results)}", flush=True)
    _print_ts(f"[完成] 输出目录: {run_dir}", flush=True)


def _prepare_incremental_output(path: Path, *, existing: list[PlaceRecord], resume: bool) -> None:
    if not resume:
        if path.exists():
            path.unlink(missing_ok=True)
        return
    if existing and not path.exists():
        write_jsonl(path, existing)


def _load_query_checkpoint(run_dir: Path) -> int:
    path = run_dir / _QUERY_CHECKPOINT_FILE
    if not path.exists():
        return 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return 0
    if not isinstance(payload, dict):
        return 0
    raw = payload.get("next_query_index")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    return value if value > 0 else 0


def _write_query_checkpoint(run_dir: Path, *, next_query_index: int, total_queries: int) -> None:
    path = run_dir / _QUERY_CHECKPOINT_FILE
    tmp_path = path.with_suffix(".tmp")
    payload = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "next_query_index": int(max(0, next_query_index)),
        "total_queries": int(max(0, total_queries)),
    }
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _run_search(
    args: argparse.Namespace,
    http: HttpClient,
    pb_template: str,
    existing: list[PlaceRecord],
    centers: list[tuple[float, float]],
    total_centers: int,
    queries: list[str],
    *,
    run_dir: Path,
    start_index: int = 0,
    incremental_jsonl_path: Path | None = None,
) -> list[PlaceRecord]:
    searcher = GoogleMapsSearcher(
        http,
        SearchConfig(
            pb_template=pb_template,
            query=queries[0],
            base_url=args.base_url,
            page_size=args.page_size,
        ),
    )
    seen_cid = {record.cid for record in existing if record.cid}
    seen_site = {record.website.lower() for record in existing if record.website}
    results: list[PlaceRecord] = list(existing)

    total_queries = len(queries)
    start = min(max(0, int(start_index or 0)), total_queries)
    last_ckpt_write = time.time()
    since_ckpt_write = 0
    per_query_timeout = max(30.0, float(args.timeout) * (max(1, int(args.retries)) + 2))

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        for q_idx, query in enumerate(queries[start:], start=start + 1):
            _print_ts(f"[搜索] 关键词 {q_idx}/{total_queries}: {query}", flush=True)
            future_map = {
                executor.submit(searcher.search_places, lat, lng, query): (lat, lng)
                for lat, lng in centers
            }
            completed = 0
            timed_out = False
            try:
                future_iter = as_completed(future_map, timeout=per_query_timeout)
                for future in future_iter:
                    completed += 1
                    try:
                        places = future.result()
                    except Exception:
                        _print_ts(
                            f"[搜索] {query} 进度 {completed}/{total_centers}：本区域搜索失败（可能网络波动/限流），继续下一块区域",
                            flush=True,
                        )
                        continue
                    kept = 0
                    incremental_batch: list[PlaceRecord] = []
                    for place in places:
                        if not place.website:
                            continue
                        if place.cid and place.cid in seen_cid:
                            continue
                        if place.website and place.website.lower() in seen_site:
                            continue
                        _maybe_enrich_phone(searcher, args, place)
                        if place.cid:
                            seen_cid.add(place.cid)
                        if place.website:
                            seen_site.add(place.website.lower())
                        results.append(place)
                        incremental_batch.append(place)
                        kept += 1
                    if incremental_jsonl_path is not None and incremental_batch:
                        append_jsonl(incremental_jsonl_path, incremental_batch)
                    if kept:
                        _print_ts(
                            f"[搜索] {query} 进度 {completed}/{total_centers}：新增官网 {kept} 个（累计 {len(results)}）",
                            flush=True,
                        )
            except FuturesTimeoutError:
                timed_out = True
            if timed_out:
                for pending in future_map:
                    if not pending.done():
                        pending.cancel()
                _print_ts(
                    (
                        f"[搜索] {query} 进度 {completed}/{total_centers}："
                        f"单关键词超时（>{int(per_query_timeout)}s），已跳过未完成区域并继续"
                    ),
                    flush=True,
                )

            since_ckpt_write += 1
            now = time.time()
            if since_ckpt_write >= 5 or (now - last_ckpt_write) >= 5.0:
                # Persist "next query index" so resume doesn't redo previous keywords.
                _write_query_checkpoint(run_dir, next_query_index=q_idx, total_queries=total_queries)
                last_ckpt_write = now
                since_ckpt_write = 0

    _write_query_checkpoint(run_dir, next_query_index=total_queries, total_queries=total_queries)
    return results


def _maybe_enrich_phone(searcher: GoogleMapsSearcher, args: argparse.Namespace, place: PlaceRecord) -> None:
    if not getattr(args, "phone_enrich", False):
        return
    if place.phone or not place.cid:
        return
    try:
        phone = searcher.fetch_place_phone(place.cid)
    except Exception:
        return
    if isinstance(phone, str) and phone.strip():
        place.phone = phone.strip()


def _capture_pb_sync(args: argparse.Namespace, query: str) -> str | None:
    center = _center_from_args(args)
    try:
        return asyncio.run(
            capture_tbm_map_pb(
                query,
                base_url=args.base_url,
                hl=args.hl,
                gl=args.gl,
                center_lat=center[0],
                center_lng=center[1],
            )
        )
    except RuntimeError:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        if loop.is_running():
            return None
        return loop.run_until_complete(
            capture_tbm_map_pb(
                query,
                base_url=args.base_url,
                hl=args.hl,
                gl=args.gl,
                center_lat=center[0],
                center_lng=center[1],
            )
        )
    except Exception:
        return None


def _center_from_args(args: argparse.Namespace) -> tuple[float, float]:
    if args.center:
        return _parse_center(args.center[0])
    min_lat, min_lng, max_lat, max_lng = args.bbox
    return (min_lat + max_lat) / 2.0, (min_lng + max_lng) / 2.0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Google Maps tbm=map 协议爬虫")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--query-file", default=None, help="一行一个关键词（UTF-8）")
    parser.add_argument("--hl", default="ja")
    parser.add_argument("--gl", default="jp")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--search-pb", default=None)
    parser.add_argument("--search-sourceurl", default=None)
    parser.add_argument("--bbox", type=_parse_bbox, default=DEFAULT_BBOX)
    parser.add_argument("--center", action="append", default=None, help="lat,lng (repeatable)")
    parser.add_argument("--grid-rows", type=int, default=1)
    parser.add_argument("--grid-cols", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--page-size", type=int, default=0, help="调整 pb 中的 !7i 结果数（实验性）")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--phone-enrich", action="store_true", help="缺电话时按 CID 请求详情补全电话")

    parser.add_argument("--cookie", default=None)
    parser.add_argument("--proxy", default=None)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--backoff", type=float, default=1.5)

    return parser.parse_args()


def _build_run_dir(base_dir: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return base_dir / f"gmap_{stamp}"


def _parse_bbox(value: str) -> tuple[float, float, float, float]:
    parts = [float(item) for item in value.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("bbox 格式: min_lat,min_lng,max_lat,max_lng")
    return parts[0], parts[1], parts[2], parts[3]


def _iter_centers(args: argparse.Namespace) -> Iterable[tuple[float, float]]:
    if args.center:
        for item in args.center:
            yield _parse_center(item)
        return

    min_lat, min_lng, max_lat, max_lng = args.bbox
    rows = max(1, args.grid_rows)
    cols = max(1, args.grid_cols)
    lat_step = (max_lat - min_lat) / rows
    lng_step = (max_lng - min_lng) / cols
    for row in range(rows):
        for col in range(cols):
            center_lat = min_lat + (row + 0.5) * lat_step
            center_lng = min_lng + (col + 0.5) * lng_step
            yield center_lat, center_lng


def _parse_center(value: str) -> tuple[float, float]:
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("center 格式: lat,lng")
    return float(parts[0]), float(parts[1])


def parse_sourceurl(url: str) -> str | None:
    if "sourceurl=" in url:
        url = urllib.parse.unquote(url.split("sourceurl=", 1)[1])
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    return params.get("pb", [None])[0]


if __name__ == "__main__":
    main()
