"""Read the headers a backend profile really sends, over loopback HTTP/1.1.

Used by the profile-table tests and to regenerate the ``headers`` of each
profile in ``profiles.json`` after a backend upgrade::

    python -m scrapely.http._capture > /tmp/native.json

No network access: a one-shot server on 127.0.0.1 records the request.
"""

from __future__ import annotations

import json
import socket
import sys
import threading
from typing import Dict, List, Tuple


class LoopbackServer:
    """Accepts connections on 127.0.0.1 and records each request's header lines in order."""

    def __init__(self, response: bytes = b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok') -> None:
        self.response = response
        self.requests: List[str] = []
        self._sock = socket.socket()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('127.0.0.1', 0))
        self._sock.listen(64)
        self.port = self._sock.getsockname()[1]
        threading.Thread(target=self._loop, daemon=True).start()

    @property
    def url(self) -> str:
        return f'http://127.0.0.1:{self.port}'

    def _loop(self) -> None:
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            threading.Thread(target=self._one, args=(conn,), daemon=True).start()

    def _one(self, conn: socket.socket) -> None:
        data = b''
        try:
            while b'\r\n\r\n' not in data:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                data += chunk
            self.requests.append(data.decode('latin-1'))
            conn.sendall(self.response)
        finally:
            conn.close()

    def close(self) -> None:
        self._sock.close()


def parse_headers(raw: str) -> List[Tuple[str, str]]:
    head = raw.split('\r\n\r\n', 1)[0]
    out = []
    for line in head.split('\r\n')[1:]:
        if ':' in line:
            k, v = line.split(':', 1)
            out.append((k.strip().lower(), v.strip()))
    return out


def native_headers(wreq_profile: str, wreq_platform: str) -> List[Tuple[str, str]]:
    """Header (name, value) pairs wreq sends for a profile, in its native order (host excluded)."""
    import wreq
    from wreq.blocking import Client

    srv = LoopbackServer()
    try:
        emu = wreq.Emulation(profile=getattr(wreq.Profile, wreq_profile), platform=getattr(wreq.Platform, wreq_platform))
        Client(emulation=emu).get(srv.url + '/').text()
        return [(k, v) for k, v in parse_headers(srv.requests[-1]) if k != 'host']
    finally:
        srv.close()


def main() -> None:
    from ._profiles import load_table

    out: Dict[str, Dict[str, str]] = {}
    for p in load_table().profiles:
        out[p.id] = dict(native_headers(p.wreq_profile, p.wreq_platform))
    json.dump(out, sys.stdout, indent=2)
    sys.stdout.write('\n')


if __name__ == '__main__':
    main()
