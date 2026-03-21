"""本地桥接代理：把请求经由前置代理转发到上游代理池。"""

from __future__ import annotations

import argparse
import base64
import logging
import os
import select
import socket
import socketserver
from dataclasses import dataclass
from urllib.parse import quote, unquote, urlsplit

from .pool import ProxyLease, ProxyPool, build_proxy_pool_from_env

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PreProxyConfig:
    """前置代理配置。"""

    scheme: str
    host: str
    port: int
    username: str = ""
    password: str = ""
    remote_dns: bool = False


@dataclass(slots=True)
class BridgeConfig:
    """桥接代理配置。"""

    listen_host: str
    listen_port: int
    connect_timeout: float
    idle_timeout: float
    pre_proxy: PreProxyConfig | None
    pool: ProxyPool


class _BridgeServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """多线程桥接代理服务。"""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], bridge: BridgeConfig) -> None:
        super().__init__(server_address, _BridgeHandler)
        self.bridge = bridge


class _BridgeHandler(socketserver.BaseRequestHandler):
    """单连接处理器。"""

    server: _BridgeServer

    def handle(self) -> None:
        self.request.settimeout(self.server.bridge.idle_timeout)
        lease: ProxyLease | None = None
        upstream: socket.socket | None = None
        try:
            request_line, headers, body = _read_client_request(self.request)
            lease = self.server.bridge.pool.acquire()
            if lease is None:
                raise RuntimeError("上游代理池为空")
            upstream = _open_upstream_tunnel(self.server.bridge, lease)
            _forward_request(self.request, upstream, request_line, headers, body, lease)
            self.server.bridge.pool.mark_success(lease.endpoint_id)
        except Exception as exc:  # noqa: BLE001
            if lease is not None:
                cooldown = self.server.bridge.pool.mark_failure(lease.endpoint_id)
                logger.warning("桥接失败 %s，冷却 %ds: %s", lease.label, cooldown, exc)
            else:
                logger.warning("桥接失败：%s", exc)
            _safe_send(self.request, _build_error_response(502, str(exc)))
        finally:
            _safe_close(upstream)


def _env_int(name: str, default: int) -> int:
    """读取整型环境变量。"""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default



def _env_float(name: str, default: float) -> float:
    """读取浮点环境变量。"""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default



def _build_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""
    parser = argparse.ArgumentParser(
        prog="python run.py proxy-bridge",
        description="启动本地桥接代理：本地 -> 前置代理 -> 上游代理池",
    )
    parser.add_argument("--prefix", default="CHAIN", help="上游代理配置前缀，默认 CHAIN")
    parser.add_argument("--listen-host", default="", help="监听地址，默认读 <prefix>_LISTEN_HOST")
    parser.add_argument("--listen-port", type=int, default=0, help="监听端口，默认读 <prefix>_LISTEN_PORT")
    parser.add_argument("--preproxy-url", default="", help="前置代理地址，默认读 <prefix>_PRE_PROXY_URL")
    parser.add_argument("--connect-timeout", type=float, default=0.0, help="上游连接超时秒数")
    parser.add_argument("--idle-timeout", type=float, default=0.0, help="客户端空闲超时秒数")
    return parser



def _parse_pre_proxy_url(raw: str) -> PreProxyConfig | None:
    """解析前置代理 URL。"""
    value = raw.strip()
    if not value:
        return None
    parsed = urlsplit(value)
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"前置代理地址无效: {value}")
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https", "socks5", "socks5h"}:
        raise ValueError(f"暂不支持的前置代理协议: {scheme}")
    return PreProxyConfig(
        scheme=scheme,
        host=parsed.hostname,
        port=int(parsed.port),
        username=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        remote_dns=scheme == "socks5h",
    )



def _read_until(sock: socket.socket, marker: bytes, max_bytes: int = 131072) -> bytes:
    """读取直到命中结束标记。"""
    data = bytearray()
    while marker not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > max_bytes:
            raise RuntimeError("HTTP 头过大")
    return bytes(data)



def _read_client_request(sock: socket.socket) -> tuple[str, dict[str, str], bytes]:
    """读取客户端请求头与请求体。"""
    raw = _read_until(sock, b"\r\n\r\n")
    if b"\r\n\r\n" not in raw:
        raise RuntimeError("客户端请求头不完整")
    header_bytes, body = raw.split(b"\r\n\r\n", 1)
    lines = header_bytes.decode("iso-8859-1").split("\r\n")
    if not lines or len(lines[0].split()) < 3:
        raise RuntimeError("客户端请求行无效")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip()] = value.strip()
    if headers.get("Transfer-Encoding", "").lower() == "chunked":
        raise RuntimeError("暂不支持 chunked 请求体")
    content_length = int(headers.get("Content-Length", "0") or "0")
    while len(body) < content_length:
        chunk = sock.recv(min(65536, content_length - len(body)))
        if not chunk:
            break
        body += chunk
    return lines[0], headers, body



def _build_basic_auth(username: str, password: str) -> str:
    """构造 Basic 鉴权头。"""
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"



def _normalize_target(request_line: str, headers: dict[str, str]) -> tuple[str, str, str]:
    """规范化请求行。"""
    method, target, version = request_line.split(" ", 2)
    if method.upper() == "CONNECT":
        return method, target, version
    if target.startswith("http://") or target.startswith("https://"):
        return method, target, version
    host = headers.get("Host", "").strip()
    if not host:
        raise RuntimeError("缺少 Host 头，无法转发")
    return method, f"http://{host}{target}", version



def _build_upstream_request(
    request_line: str,
    headers: dict[str, str],
    proxy_auth: str,
    upstream_authority: str,
    body: bytes,
) -> bytes:
    """构造发往上游代理的普通 HTTP 请求。"""
    method, target, version = _normalize_target(request_line, headers)
    forwarded: list[str] = [f"{method} {target} {version}"]
    filtered = {
        key: value
        for key, value in headers.items()
        if key.lower() not in {"proxy-authorization", "proxy-connection", "connection", "host"}
    }
    filtered["Host"] = upstream_authority
    filtered["Proxy-Authorization"] = proxy_auth
    filtered["Connection"] = "close"
    for key, value in filtered.items():
        forwarded.append(f"{key}: {value}")
    forwarded.append("")
    raw = "\r\n".join(forwarded).encode("iso-8859-1") + b"\r\n"
    return raw + body



def _build_connect_request(target: str, version: str, upstream_authority: str, proxy_auth: str) -> bytes:
    """构造 CONNECT 请求。"""
    lines = [
        f"CONNECT {target} {version}",
        f"Host: {upstream_authority}",
        f"Proxy-Authorization: {proxy_auth}",
        "Proxy-Connection: Keep-Alive",
        "",
        "",
    ]
    return "\r\n".join(lines).encode("iso-8859-1")



def _open_upstream_tunnel(bridge: BridgeConfig, lease: ProxyLease) -> socket.socket:
    """打开到上游代理节点的 TCP 通道。"""
    parsed = urlsplit(lease.proxy_url)
    if not parsed.hostname or not parsed.port:
        raise RuntimeError(f"上游代理地址无效: {lease.proxy_url}")
    if bridge.pre_proxy is None:
        sock = socket.create_connection((parsed.hostname, int(parsed.port)), timeout=bridge.connect_timeout)
    elif bridge.pre_proxy.scheme in {"socks5", "socks5h"}:
        sock = _connect_via_socks5(bridge.pre_proxy, parsed.hostname, int(parsed.port), bridge.connect_timeout)
    else:
        sock = _connect_via_http_proxy(bridge.pre_proxy, parsed.hostname, int(parsed.port), bridge.connect_timeout)
    sock.settimeout(bridge.idle_timeout)
    return sock



def _connect_via_http_proxy(config: PreProxyConfig, host: str, port: int, timeout: float) -> socket.socket:
    """通过 HTTP 前置代理建立隧道。"""
    sock = socket.create_connection((config.host, config.port), timeout=timeout)
    lines = [f"CONNECT {host}:{port} HTTP/1.1", f"Host: {host}:{port}"]
    if config.username:
        lines.append(f"Proxy-Authorization: {_build_basic_auth(config.username, config.password)}")
    lines.extend(["Proxy-Connection: Keep-Alive", "", ""])
    sock.sendall("\r\n".join(lines).encode("iso-8859-1"))
    response = _read_until(sock, b"\r\n\r\n")
    status_line = response.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="replace")
    if " 200 " not in f" {status_line} ":
        sock.close()
        raise RuntimeError(f"前置 HTTP 代理 CONNECT 失败: {status_line}")
    return sock



def _connect_via_socks5(config: PreProxyConfig, host: str, port: int, timeout: float) -> socket.socket:
    """通过 SOCKS5 前置代理建立隧道。"""
    sock = socket.create_connection((config.host, config.port), timeout=timeout)
    methods = [0x00, 0x02] if config.username else [0x00]
    sock.sendall(bytes([0x05, len(methods), *methods]))
    version, method = _read_exact(sock, 2)
    if version != 0x05 or method == 0xFF:
        sock.close()
        raise RuntimeError("前置 SOCKS5 代理协商失败")
    if method == 0x02:
        username = config.username.encode("utf-8")
        password = config.password.encode("utf-8")
        sock.sendall(bytes([0x01, len(username)]) + username + bytes([len(password)]) + password)
        auth_status = _read_exact(sock, 2)
        if auth_status[1] != 0x00:
            sock.close()
            raise RuntimeError("前置 SOCKS5 代理鉴权失败")
    address = _build_socks_address(host, port, config.remote_dns)
    sock.sendall(bytes([0x05, 0x01, 0x00]) + address)
    reply = _read_exact(sock, 4)
    if reply[1] != 0x00:
        sock.close()
        raise RuntimeError(f"前置 SOCKS5 代理连接失败，错误码={reply[1]}")
    _discard_socks_bound_address(sock, reply[3])
    return sock



def _build_socks_address(host: str, port: int, remote_dns: bool) -> bytes:
    """构造 SOCKS5 地址段。"""
    if not remote_dns:
        try:
            packed = socket.inet_aton(host)
            return bytes([0x01]) + packed + port.to_bytes(2, "big")
        except OSError:
            pass
    encoded = host.encode("idna")
    if len(encoded) > 255:
        raise RuntimeError("SOCKS5 域名过长")
    return bytes([0x03, len(encoded)]) + encoded + port.to_bytes(2, "big")



def _discard_socks_bound_address(sock: socket.socket, atyp: int) -> None:
    """丢弃 SOCKS5 返回地址。"""
    if atyp == 0x01:
        _read_exact(sock, 6)
        return
    if atyp == 0x03:
        size = _read_exact(sock, 1)[0]
        _read_exact(sock, size + 2)
        return
    if atyp == 0x04:
        _read_exact(sock, 18)
        return
    raise RuntimeError(f"未知 SOCKS5 地址类型: {atyp}")



def _read_exact(sock: socket.socket, size: int) -> bytes:
    """精确读取指定字节数。"""
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise RuntimeError("连接被提前关闭")
        chunks.extend(chunk)
    return bytes(chunks)



def _forward_request(
    client_sock: socket.socket,
    upstream: socket.socket,
    request_line: str,
    headers: dict[str, str],
    body: bytes,
    lease: ProxyLease,
) -> None:
    """按请求类型转发。"""
    method, target, version = _normalize_target(request_line, headers)
    parsed = urlsplit(lease.proxy_url)
    upstream_authority = f"{parsed.hostname}:{parsed.port}"
    proxy_auth = _build_basic_auth(unquote(parsed.username or ""), unquote(parsed.password or ""))
    if method.upper() == "CONNECT":
        upstream.sendall(_build_connect_request(target, version, upstream_authority, proxy_auth))
        response = _read_until(upstream, b"\r\n\r\n")
        status_line = response.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="replace")
        if " 200 " not in f" {status_line} ":
            client_sock.sendall(response)
            return
        client_sock.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        _tunnel_bidirectional(client_sock, upstream)
        return
    upstream.sendall(_build_upstream_request(request_line, headers, proxy_auth, upstream_authority, body))
    _relay_response(upstream, client_sock)



def _relay_response(upstream: socket.socket, client_sock: socket.socket) -> None:
    """把上游响应原样写回客户端。"""
    while True:
        chunk = upstream.recv(65536)
        if not chunk:
            break
        client_sock.sendall(chunk)



def _tunnel_bidirectional(left: socket.socket, right: socket.socket) -> None:
    """双向转发隧道数据。"""
    sockets = [left, right]
    while True:
        readable, _, _ = select.select(sockets, [], [], 1.0)
        if not readable:
            continue
        for source in readable:
            target = right if source is left else left
            try:
                chunk = source.recv(65536)
            except OSError:
                return
            if not chunk:
                return
            target.sendall(chunk)



def _build_error_response(status: int, message: str) -> bytes:
    """构造错误响应。"""
    body = f"bridge error: {message}".encode("utf-8", errors="replace")
    lines = [
        f"HTTP/1.1 {status} Bad Gateway",
        "Content-Type: text/plain; charset=utf-8",
        f"Content-Length: {len(body)}",
        "Connection: close",
        "",
        "",
    ]
    return "\r\n".join(lines).encode("iso-8859-1") + body



def _safe_send(sock: socket.socket, data: bytes) -> None:
    """安全写回客户端。"""
    try:
        sock.sendall(data)
    except OSError:
        pass



def _safe_close(sock: socket.socket | None) -> None:
    """安全关闭连接。"""
    if sock is None:
        return
    try:
        sock.close()
    except OSError:
        pass



def run_proxy_bridge(argv: list[str]) -> int:
    """启动本地桥接代理。"""
    args = _build_parser().parse_args(argv)
    prefix = args.prefix.strip().upper() or "CHAIN"
    pool = build_proxy_pool_from_env(prefix=prefix)
    if pool is None or not pool.enabled:
        print(f"未检测到 {prefix}_PROXY 配置，无法启动桥接代理。")
        return 1
    listen_host = args.listen_host.strip() or os.getenv(f"{prefix}_LISTEN_HOST", "127.0.0.1").strip() or "127.0.0.1"
    listen_port = args.listen_port or _env_int(f"{prefix}_LISTEN_PORT", 8899)
    connect_timeout = args.connect_timeout or _env_float(f"{prefix}_CONNECT_TIMEOUT", 15.0)
    idle_timeout = args.idle_timeout or _env_float(f"{prefix}_IDLE_TIMEOUT", 90.0)
    pre_proxy_url = args.preproxy_url.strip() or os.getenv(f"{prefix}_PRE_PROXY_URL", "").strip()
    bridge = BridgeConfig(
        listen_host=listen_host,
        listen_port=listen_port,
        connect_timeout=max(3.0, float(connect_timeout)),
        idle_timeout=max(10.0, float(idle_timeout)),
        pre_proxy=_parse_pre_proxy_url(pre_proxy_url),
        pool=pool,
    )
    with _BridgeServer((bridge.listen_host, bridge.listen_port), bridge) as server:
        print(f"桥接代理已启动：http://{bridge.listen_host}:{bridge.listen_port}")
        if bridge.pre_proxy is not None:
            print(
                f"前置代理：{bridge.pre_proxy.scheme}://{bridge.pre_proxy.host}:{bridge.pre_proxy.port}，上游节点数：{bridge.pool.size}"
            )
        else:
            print(f"前置代理：未配置（直连上游），上游节点数：{bridge.pool.size}")
        print("按 Ctrl+C 停止")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("桥接代理已停止")
    return 0

