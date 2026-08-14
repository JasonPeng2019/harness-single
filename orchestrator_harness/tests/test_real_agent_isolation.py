from __future__ import annotations

import base64
import importlib.util
import json
import socket
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

SUPPORT = Path(__file__).resolve().parent / "support"


def load_support(name: str):
    spec = importlib.util.spec_from_file_location(name, SUPPORT / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


proxy_module = load_support("allowlist_connect_proxy")
driver_module = load_support("wsl_real_agent_driver")
AllowlistProxy = proxy_module.AllowlistProxy
bwrap_base = driver_module.bwrap_base
minimal_auth = driver_module.minimal_auth


def jwt(expiry: datetime) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = (
        base64.urlsafe_b64encode(json.dumps({"exp": int(expiry.timestamp())}).encode())
        .rstrip(b"=")
        .decode()
    )
    return f"{header}.{payload}.signature"


class RealAgentIsolationTests(unittest.TestCase):
    def test_minimal_auth_omits_refresh_and_requires_expired_id_format(self) -> None:
        now = datetime.now(timezone.utc)
        access = jwt(now + timedelta(hours=1))
        expired_id = jwt(now - timedelta(hours=1))
        refresh = "refresh-secret-value"
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.json"
            destination = root / "home" / "auth.json"
            source.write_text(
                json.dumps(
                    {
                        "auth_mode": "chatgpt",
                        "tokens": {
                            "access_token": access,
                            "id_token": expired_id,
                            "refresh_token": refresh,
                            "account_id": "account",
                        },
                    }
                )
            )
            secrets, access_expiry, id_expiry = minimal_auth(source, destination)
            value = json.loads(destination.read_text())
            self.assertEqual("", value["tokens"]["refresh_token"])
            self.assertEqual(expired_id, value["tokens"]["id_token"])
            self.assertNotIn(refresh, destination.read_text())
            self.assertEqual({access, expired_id, refresh}, set(secrets))
            self.assertGreater(access_expiry, id_expiry)

    def test_bwrap_mount_manifest_has_only_synthetic_writable_binds(self) -> None:
        argv = bwrap_base(
            Path("/pinned/bwrap"),
            Path("/pinned/release"),
            Path("/synthetic/workspace"),
            Path("/synthetic/home"),
            "http://10.0.0.1:1234",
        )
        rendered = "\0".join(argv).replace("\\", "/")
        self.assertNotIn("/mnt/c", rendered)
        self.assertNotIn("BYO-Firmware-MCP", rendered)
        self.assertIn("--cap-drop\0ALL", rendered)
        self.assertIn("--ro-bind\0/usr\0/usr", rendered)
        self.assertIn("--bind\0/synthetic/workspace\0/workspace", rendered)
        self.assertIn("--bind\0/synthetic/home\0/home/agent/.codex", rendered)

    def test_connect_proxy_denies_nonallowlisted_destination(self) -> None:
        with TemporaryDirectory() as temporary:
            audit = Path(temporary) / "audit.jsonl"
            proxy = AllowlistProxy("127.0.0.1", {("allowed.invalid", 443)}, audit)
            proxy.start()
            try:
                with socket.create_connection(
                    ("127.0.0.1", proxy.port), timeout=2
                ) as client:
                    client.sendall(
                        b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
                    )
                    self.assertTrue(client.recv(128).startswith(b"HTTP/1.1 403"))
            finally:
                proxy.close()
            record = json.loads(audit.read_text().splitlines()[0])
            self.assertEqual("DENY", record["decision"])

    def test_connect_proxy_relays_exact_allowlisted_destination(self) -> None:
        upstream = socket.socket()
        upstream.bind(("127.0.0.1", 0))
        upstream.listen(1)
        upstream_port = upstream.getsockname()[1]

        def echo_once() -> None:
            connection, _ = upstream.accept()
            with connection:
                connection.sendall(b"hello")
                connection.recv(16)

        thread = threading.Thread(target=echo_once)
        thread.start()
        with TemporaryDirectory() as temporary:
            audit = Path(temporary) / "audit.jsonl"
            proxy = AllowlistProxy("127.0.0.1", {("127.0.0.1", upstream_port)}, audit)
            proxy.start()
            try:
                with socket.create_connection(
                    ("127.0.0.1", proxy.port), timeout=2
                ) as client:
                    target = f"127.0.0.1:{upstream_port}"
                    client.sendall(
                        f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode()
                    )
                    response = bytearray()
                    while b"\r\n\r\n" not in response:
                        chunk = client.recv(128)
                        self.assertTrue(chunk, "proxy closed before CONNECT response")
                        response.extend(chunk)
                    header, payload = bytes(response).split(b"\r\n\r\n", 1)
                    self.assertTrue(header.startswith(b"HTTP/1.1 200"))
                    while len(payload) < 5:
                        chunk = client.recv(5 - len(payload))
                        self.assertTrue(chunk, "proxy closed before tunnel payload")
                        payload += chunk
                    self.assertEqual(b"hello", payload)
                    client.sendall(b"done")
            finally:
                proxy.close()
                upstream.close()
                thread.join(timeout=2)
            records = [json.loads(line) for line in audit.read_text().splitlines()]
            self.assertEqual("ALLOW", records[-1]["decision"])


if __name__ == "__main__":
    unittest.main()
