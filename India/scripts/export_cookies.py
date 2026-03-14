import argparse
import json
import sys
from pathlib import Path

try:
    import browser_cookie3
except Exception as exc:
    print(f"browser_cookie3 未安装或不可用: {exc}")
    print("请先安装依赖: pip install browser-cookie3")
    sys.exit(1)


def _find_local_state(cookie_file: str | None) -> str | None:
    if not cookie_file:
        return None
    path = Path(cookie_file)
    for parent in path.parents:
        candidate = parent / "Local State"
        if candidate.exists():
            return str(candidate)
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="导出 ZaubaCorp cookies")
    parser.add_argument("--domain", default="zaubacorp.com", help="目标域名")
    parser.add_argument(
        "--browser",
        default="chrome",
        choices=["chrome", "edge"],
        help="浏览器类型",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Chrome/Edge Profile 名称，如 'Default' 或 'Profile 1'",
    )
    parser.add_argument("--cookie-file", default=None, help="Cookies 文件路径(可选)")
    parser.add_argument("--key-file", default=None, help="Local State 文件路径(可选)")
    parser.add_argument("--output", default="cookies.json", help="输出路径")
    args = parser.parse_args()

    out_path = Path(args.output)
    loader = browser_cookie3.chrome if args.browser == "chrome" else browser_cookie3.edge
    kwargs = {"domain_name": args.domain}
    if args.profile:
        kwargs["profile"] = args.profile
    if args.cookie_file:
        kwargs["cookie_file"] = args.cookie_file

    key_file = args.key_file or _find_local_state(args.cookie_file)
    if key_file:
        kwargs["key_file"] = key_file

    jar = loader(**kwargs)
    cookies = []
    for cookie in jar:
        cookies.append(
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "secure": cookie.secure,
            }
        )

    out_path.write_text(
        json.dumps({"cookies": cookies}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"已导出 {len(cookies)} 条 cookie 到 {out_path}")
    if not cookies:
        print("提示: 请确认使用的浏览器 Profile 正确，并已打开目标页面通过 Cloudflare 验证后再导出。")


if __name__ == "__main__":
    main()