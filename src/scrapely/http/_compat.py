"""curl_cffi-style ``AsyncSession`` / ``Session`` on top of the anti-blocking client.

Migration is one line::

    from curl_cffi.requests import AsyncSession      # before
    from scrapely.http import AsyncSession           # after

Like a curl_cffi session, all requests of one session share one cookie jar
and one proxy session. Unlike it, a blocked session is retired and replaced
by a fresh identity (new proxy session, new current browser profile, empty
jar) and the request is retried. ``impersonate=`` picks the browser family;
its version is clamped to the current profiles, because an old version is
scored as a bot.
"""

from __future__ import annotations

from typing import Any, Optional

from ._client import AsyncClient, Client, Response, _Pin, _SessionCookies


def _timeout(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (tuple, list)):
        return float(sum(v for v in value if v))
    return float(value)


def _compat_kwargs(kwargs: dict) -> dict:
    out = dict(kwargs)
    if 'timeout' in out:
        out['timeout'] = _timeout(out['timeout'])
    # curl_cffi options with no meaning here.
    for key in ('max_clients', 'default_headers', 'curl_options', 'http_version', 'debug', 'interface', 'trust_env', 'base_url', 'allow_redirects', 'max_redirects', 'auth', 'cookies', 'default_encoding', 'discard_cookies', 'thread', 'use_thread_local_curl', 'loop', 'async_curl', 'ja3', 'akamai', 'extra_fp', 'raise_for_status', 'retry', 'cert'):
        out.pop(key, None)
    return out


class AsyncSession(AsyncClient):
    """Drop-in for ``curl_cffi.requests.AsyncSession``: one pinned identity, rotated on blocks."""

    _quiet_summary = True

    def __init__(self, impersonate: Optional[str] = None, proxy: Optional[str] = None, proxies: Any = None, timeout: Any = 30, headers: Any = None, verify: bool = True, **kwargs: Any) -> None:
        cookies = kwargs.get('cookies')
        super().__init__(impersonate=impersonate, proxy=proxy, proxies=proxies, timeout=_timeout(timeout), headers=headers, verify=verify, **_compat_kwargs(kwargs))
        self.headers = self.default_headers
        self._session_pin = _Pin()
        self.cookies = _SessionCookies(_PinView(self, self._session_pin))
        if cookies:
            for k, v in dict(cookies).items():
                self.cookies.set(k, v)

    async def request(self, method: str, url: str, **kwargs: Any) -> Response:  # type: ignore[override]
        kwargs.pop('impersonate', None)
        if 'timeout' in kwargs:
            kwargs['timeout'] = _timeout(kwargs['timeout'])
        kwargs.setdefault('_pin', self._session_pin)
        return await super().request(method, url, **kwargs)


class Session(Client):
    """Drop-in for ``curl_cffi.requests.Session``: one pinned identity, rotated on blocks."""

    _quiet_summary = True

    def __init__(self, impersonate: Optional[str] = None, proxy: Optional[str] = None, proxies: Any = None, timeout: Any = 30, headers: Any = None, verify: bool = True, **kwargs: Any) -> None:
        cookies = kwargs.get('cookies')
        super().__init__(impersonate=impersonate, proxy=proxy, proxies=proxies, timeout=_timeout(timeout), headers=headers, verify=verify, **_compat_kwargs(kwargs))
        self.headers = self.default_headers
        self._session_pin = _Pin()
        self.cookies = _SessionCookies(_PinView(self, self._session_pin))
        if cookies:
            for k, v in dict(cookies).items():
                self.cookies.set(k, v)

    def request(self, method: str, url: str, **kwargs: Any) -> Response:  # type: ignore[override]
        kwargs.pop('impersonate', None)
        if 'timeout' in kwargs:
            kwargs['timeout'] = _timeout(kwargs['timeout'])
        kwargs.setdefault('_pin', self._session_pin)
        return super().request(method, url, **kwargs)


class _PinView:
    """Adapter so _SessionCookies can read a compat session's pinned identity."""

    def __init__(self, client: Any, pin: _Pin) -> None:
        self._client = client
        self._pin = pin
