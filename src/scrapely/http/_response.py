"""Response wrapper with the requests / curl_cffi attribute names."""

from __future__ import annotations

import json as _json
import re
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple

_CHARSET_RE = re.compile(r'charset=["\']?([\w.:-]+)', re.I)
_META_CHARSET_RE = re.compile(rb'<meta[^>]+charset=["\']?([\w.:-]+)', re.I)


class Headers(Mapping[str, str]):
    """Case-insensitive, multi-value response headers. ``h["set-cookie"]`` joins repeats with ", "."""

    def __init__(self, items: List[Tuple[str, str]]) -> None:
        self._items = [(k.lower(), v) for k, v in items]

    def __getitem__(self, key: str) -> str:
        values = self.get_list(key)
        if not values:
            raise KeyError(key)
        return ', '.join(values)

    def get_list(self, key: str) -> List[str]:
        key = key.lower()
        return [v for k, v in self._items if k == key]

    def multi_items(self) -> List[Tuple[str, str]]:
        return list(self._items)

    def __iter__(self) -> Iterator[str]:
        seen: List[str] = []
        for k, _ in self._items:
            if k not in seen:
                seen.append(k)
        return iter(seen)

    def __len__(self) -> int:
        return len({k for k, _ in self._items})

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and any(k == key.lower() for k, _ in self._items)

    def __repr__(self) -> str:
        return f'Headers({dict(self)!r})'


class HTTPError(Exception):
    """Raised by ``Response.raise_for_status()`` for 4xx/5xx responses."""

    def __init__(self, message: str, response: 'Response') -> None:
        super().__init__(message)
        self.response = response


class Response:
    """An HTTP response.

    Attributes:
        status_code: the HTTP status.
        headers: case-insensitive response headers.
        url: the final URL (after redirects).
        content: the body as bytes; ``text`` decodes it, ``json()`` parses it.
        cookies: cookies of the identity that made the request (name → value).
        blocked: the block reason when every attempt was blocked
            (``"cloudflare"``, ``"perimeterx"``, ``"datadome"``, ``"akamai"``,
            ``"429"``, ``"511"``, ``"403"`` ...), else None.
        profile: the browser profile that sent the final attempt, e.g. ``chrome154-win``.
        attempts: how many attempts the request took.
    """

    def __init__(
        self,
        status_code: int,
        headers: List[Tuple[str, str]],
        content: bytes,
        url: str,
        cookies: Optional[Dict[str, str]] = None,
        *,
        blocked: Optional[str] = None,
        profile: str = '',
        attempts: int = 1,
        elapsed: float = 0.0,
        http_version: str = '',
        request_method: str = 'GET',
    ) -> None:
        self.status_code = int(status_code)
        self.headers = Headers(headers)
        self.content = content
        self.url = url
        self.cookies: Dict[str, str] = dict(cookies or {})
        self.blocked = blocked
        self.profile = profile
        self.attempts = attempts
        self.elapsed = elapsed
        self.http_version = http_version
        self.request_method = request_method
        self._text: Optional[str] = None
        self._encoding: Optional[str] = None

    @property
    def status(self) -> int:
        return self.status_code

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 400

    @property
    def reason(self) -> str:
        from http import HTTPStatus

        try:
            return HTTPStatus(self.status_code).phrase
        except ValueError:
            return ''

    @property
    def encoding(self) -> str:
        if self._encoding is None:
            m = _CHARSET_RE.search(self.headers.get('content-type', ''))
            enc = m.group(1) if m else None
            if enc is None:
                m2 = _META_CHARSET_RE.search(self.content[:4096])
                enc = m2.group(1).decode('ascii', 'ignore') if m2 else None
            try:
                import codecs

                codecs.lookup(enc or 'utf-8')
            except LookupError:
                enc = None
            self._encoding = enc or 'utf-8'
        return self._encoding

    @encoding.setter
    def encoding(self, value: str) -> None:
        self._encoding = value
        self._text = None

    @property
    def text(self) -> str:
        if self._text is None:
            self._text = self.content.decode(self.encoding, 'replace')
        return self._text

    def json(self, **kwargs: Any) -> Any:
        return _json.loads(self.content.decode(self.encoding, 'replace') if self.content else '', **kwargs)

    def raise_for_status(self) -> 'Response':
        if self.status_code >= 400:
            raise HTTPError(f'HTTP {self.status_code} {self.reason} for {self.url}', self)
        return self

    def __repr__(self) -> str:
        extra = f' blocked={self.blocked}' if self.blocked else ''
        return f'<Response [{self.status_code}]{extra}>'
