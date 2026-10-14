from __future__ import annotations

import json
import select
import socket
import socketserver
import threading
from datetime import datetime, timezone
from pathlib import Path


MAX_HEADER = 16 * 1024


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class AuditLog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def write(self, **value: object) -> None:
        value["at_utc"] = utc_now()
        payload = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
        with self.lock:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(payload)
                stream.flush()


class AllowlistProxy:
    def __init__(
        self,
        bind_host: str,
        allowed: set[tuple[str, int]],
        audit_path: Path,
    ) -> None:
        audit = AuditLog(audit_path)
        allowed_normalized = {
            (host.lower().rstrip("."), port) for host, port in allowed
        }

        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                self.request.settimeout(10)
                header = bytearray()
                while b"\r\n\r\n" not in header and len(header) < MAX_HEADER:
                    chunk = self.request.recv(4096)
                    if not chunk:
                        return
                    header.extend(chunk)
                first = (
                    bytes(header).split(b"\r\n", 1)[0].decode("ascii", errors="replace")
                )
                parts = first.split()
                if (
                    len(parts) != 3
                    or parts[0].upper() != "CONNECT"
                    or ":" not in parts[1]
                ):
                    audit.write(
                        decision="DENY", reason="CONNECT required", request=first
                    )
                    self.request.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                    return
                host_text, port_text = parts[1].rsplit(":", 1)
                host = host_text.strip("[]").lower().rstrip(".")
                try:
                    port = int(port_text)
                except ValueError:
                    port = -1
                if (host, port) not in allowed_normalized:
                    audit.write(
                        decision="DENY",
                        reason="destination not allowlisted",
                        host=host,
                        port=port,
                    )
                    self.request.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\n")
                    return
                try:
                    upstream = socket.create_connection((host, port), timeout=15)
                except OSError as exc:
                    audit.write(
                        decision="ERROR",
                        reason=type(exc).__name__,
                        host=host,
                        port=port,
                    )
                    self.request.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    return
                audit.write(decision="ALLOW", host=host, port=port)
                self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                with upstream:
                    sockets = [self.request, upstream]
                    while True:
                        readable, _, exceptional = select.select(
                            sockets, [], sockets, 30
                        )
                        if exceptional or not readable:
                            return
                        for source in readable:
                            try:
                                data = source.recv(65536)
                            except OSError:
                                return
                            if not data:
                                return
                            target = (
                                upstream if source is self.request else self.request
                            )
                            try:
                                target.sendall(data)
                            except OSError:
                                return

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        self.server = Server((bind_host, 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
