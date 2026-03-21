import argparse
import json
import sys
from urllib.error import URLError
from urllib.request import urlopen

try:
    import websocket
except Exception as exc:
    print(f"websocket-client is required: {exc}")
    print("Install: pip install websocket-client")
    sys.exit(1)


def fetch_json(url: str):
    with urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def pick_target(targets, domain: str | None, target_url: str | None):
    pages = [t for t in targets if t.get("type") == "page"]
    if target_url:
        for t in pages:
            if t.get("url") == target_url:
                return t
        return None
    if domain:
        for t in pages:
            if domain in (t.get("url") or ""):
                return t
    return pages[0] if pages else None


def send(ws, method: str, params: dict | None = None):
    send.msg_id += 1
    payload = {"id": send.msg_id, "method": method}
    if params:
        payload["params"] = params
    ws.send(json.dumps(payload))
    while True:
        msg = json.loads(ws.recv())
        if msg.get("id") == send.msg_id:
            return msg


send.msg_id = 0


def is_domain_match(cookie_domain: str, domain: str | None) -> bool:
    if not domain:
        return True
    cd = cookie_domain.lstrip(".").lower()
    d = domain.lower()
    return cd == d or cd.endswith(f".{d}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export cookies from Chrome DevTools Protocol")
    parser.add_argument("--host", default="127.0.0.1", help="CDP host")
    parser.add_argument("--port", type=int, default=9222, help="CDP port")
    parser.add_argument("--domain", default="zaubacorp.com", help="Filter cookies by domain")
    parser.add_argument("--target-url", default=None, help="Exact page URL to match")
    parser.add_argument("--output", default="cookies.json", help="Output file path")
    parser.add_argument("--list-targets", action="store_true", help="List available targets")
    args = parser.parse_args()

    base = f"http://{args.host}:{args.port}"
    list_url = f"{base}/json/list"

    try:
        targets = fetch_json(list_url)
    except URLError as exc:
        print(f"Failed to connect to CDP at {base}: {exc}")
        sys.exit(1)

    if args.list_targets:
        for t in targets:
            print(f"{t.get('type')} {t.get('url')}")
        return

    target = pick_target(targets, args.domain, args.target_url)
    if not target:
        print("No matching target found. Use --list-targets to inspect.")
        sys.exit(1)

    ws_url = target.get("webSocketDebuggerUrl")
    if not ws_url:
        print("Target missing webSocketDebuggerUrl.")
        sys.exit(1)

    ws = websocket.create_connection(ws_url, timeout=10)
    try:
        send(ws, "Network.enable")

        response = send(ws, "Network.getAllCookies")
        cookies = (response.get("result") or {}).get("cookies") or []
        if not cookies:
            urls = []
            if target.get("url"):
                urls.append(target["url"])
            if args.domain:
                urls.append(f"https://{args.domain}/")
                urls.append(f"http://{args.domain}/")
            response = send(ws, "Network.getCookies", {"urls": urls})
            cookies = (response.get("result") or {}).get("cookies") or []

        filtered = []
        for cookie in cookies:
            if is_domain_match(cookie.get("domain", ""), args.domain):
                filtered.append(
                    {
                        "name": cookie.get("name", ""),
                        "value": cookie.get("value", ""),
                        "domain": cookie.get("domain", ""),
                        "path": cookie.get("path", "/"),
                        "secure": bool(cookie.get("secure", False)),
                    }
                )

        with open(args.output, "w", encoding="utf-8") as f:
            json.dump({"cookies": filtered}, f, ensure_ascii=False, indent=2)

        print(f"Exported {len(filtered)} cookies to {args.output}")
        if not filtered:
            print("Tip: open the target page in the same CDP browser and pass Cloudflare first.")
    finally:
        ws.close()


if __name__ == "__main__":
    main()
