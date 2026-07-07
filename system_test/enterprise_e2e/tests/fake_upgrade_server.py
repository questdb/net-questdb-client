"""Minimal fake WebSocket-upgrade server for exercising the .NET client's
connect-walk / role-negotiation without a real QuestDB node.

Reproduces the *reject* side of the Java client's ``TestWebSocketServer`` /
``FakeStatusServer`` (java-questdb-client
``core/.../cutlass/qwp/client/WriteFailoverTest.java``): accept the TCP
connection, read the HTTP upgrade request, and reply with a fixed HTTP status
plus an optional ``X-QuestDB-Role`` header, then close. That is enough to drive
the terminal / rotation cases the .NET client classifies on the ``/write/v4``
upgrade response:

* ``421 Misdirected Request`` + ``X-QuestDB-Role: REPLICA|PRIMARY_CATCHUP``
  -> ``QwpIngressRoleRejectedException`` (rotate / role-terminal),
* ``401 Unauthorized`` / ``403 Forbidden`` -> ``AuthError`` (immediately terminal),
* any other non-101 status -> a generic upgrade failure.

It does NOT complete a WebSocket handshake, so it cannot ack frames; positive
streaming paths (walk-to-primary, durable-ack) need a real primary from
``server_factory``.
"""

from __future__ import annotations

import socket
import threading
from typing import Optional


class FakeUpgradeServer:
    def __init__(self, status: int, reason: str, *, role: Optional[str] = None,
                 zone: Optional[str] = None) -> None:
        self._status = status
        self._reason = reason
        self._role = role
        self._zone = zone
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._port = self._sock.getsockname()[1]
        self._stop = threading.Event()
        # Count of accepted connections — proves an endpoint was actually probed
        # during a multi-host walk.
        self.connections = 0
        self._thread = threading.Thread(
            target=self._loop, name=f"fake-upgrade-{self._port}", daemon=True)

    @property
    def port(self) -> int:
        return self._port

    def start(self) -> "FakeUpgradeServer":
        self._thread.start()
        return self

    def _loop(self) -> None:
        self._sock.settimeout(0.5)
        while not self._stop.is_set():
            try:
                client, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self.connections += 1
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client: socket.socket) -> None:
        try:
            client.settimeout(2.0)
            # Drain the request headers so the client's upgrade write completes
            # before we reply with the rejection.
            buf = b""
            try:
                while b"\r\n\r\n" not in buf and len(buf) < 65536:
                    chunk = client.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
            except OSError:
                pass

            lines = [f"HTTP/1.1 {self._status} {self._reason}"]
            if self._role is not None:
                lines.append(f"X-QuestDB-Role: {self._role}")
            if self._zone is not None:
                lines.append(f"X-QuestDB-Zone: {self._zone}")
            lines.append("Content-Length: 0")
            lines.append("Connection: close")
            lines.append("")
            lines.append("")
            try:
                client.sendall("\r\n".join(lines).encode("ascii"))
            except OSError:
                pass
        finally:
            try:
                client.close()
            except OSError:
                pass

    def close(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass

    def __enter__(self) -> "FakeUpgradeServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.close()
