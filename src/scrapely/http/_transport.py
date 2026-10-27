"""HTTP backends: wreq (default) and curl_cffi (fallback when wreq can't be imported).

A transport owns one backend client per identity (profile + proxy + cookie
jar) and turns a prepared request into a ``RawResponse``. Everything above
this layer (identities, retries, block detection) is backend-agnostic.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger('scrapely.http')

_warned_fallback = False


@dataclass
class RawResponse:
    status: int
    headers: List[Tuple[str, str]]
    body: bytes
    url: str
    set_cookies: List[str] = field(default_factory=list)
    http_version: str = ''


class TransportError(Exception):
    """A network, TLS, proxy or timeout error from the backend."""

    def __init__(self, kind: str, message: str, original: Optional[BaseException] = None) -> None:
        super().__init__(message)
        self.kind = kind
        self.original = original


def _wreq_error_kind(exc: BaseException) -> str:
    name = type(exc).__name__
    return {
        'TimeoutError': 'timeout',
        'ProxyConnectionError': 'proxy',
        'TlsError': 'tls',
        'ConnectionError': 'connect',
        'ConnectionResetError': 'reset',
        'BodyError': 'body',
        'DecodingError': 'decode',
        'RedirectError': 'redirect',
        'RequestError': 'request',
    }.get(name, name.lower())


def wreq_available() -> bool:
    try:
        import wreq  # noqa: F401
    except Exception:  # noqa: BLE001 - missing wheel / old Python
        return False
    return True


def curl_cffi_available() -> bool:
    try:
        import curl_cffi  # noqa: F401
    except Exception:  # noqa: BLE001
        return False
    return True


def resolve_backend(backend: str) -> str:
    """'wreq' or 'curl_cffi' for backend='auto'|'wreq'|'curl_cffi'."""
    global _warned_fallback
    if backend not in ('auto', 'wreq', 'curl_cffi'):
        raise ValueError(f"backend must be 'auto', 'wreq' or 'curl_cffi'; got {backend!r}")
    if backend == 'wreq' or (backend == 'auto' and wreq_available()):
        if not wreq_available():
            raise ImportError('scrapely.http backend="wreq" needs the wreq package (Python 3.11+): pip install "wreq>=0.13,<0.14"')
        return 'wreq'
    if curl_cffi_available():
        if backend == 'auto' and not _warned_fallback:
            _warned_fallback = True
            logger.warning(
                'scrapely.http: wreq is not installed (it needs Python 3.11+); using curl_cffi instead. '
                'Fingerprints are less complete; install "wreq>=0.13,<0.14" for the full profile set.'
            )
        return 'curl_cffi'
    raise ImportError('scrapely.http needs wreq (pip install "wreq>=0.13,<0.14") or curl_cffi')


# ─── wreq ───────────────────────────────────────────────────────────────────


def _td(seconds: Optional[float]) -> Optional[datetime.timedelta]:
    return datetime.timedelta(seconds=float(seconds)) if seconds else None


def _wreq_client_kwargs(profile: Any, proxy_url: Optional[str], timeout: Optional[float], verify: bool, http1_only: bool) -> Dict[str, Any]:
    import wreq

    emulation = wreq.Emulation(
        profile=getattr(wreq.Profile, profile.wreq_profile),
        platform=getattr(wreq.Platform, profile.wreq_platform),
    )
    kwargs: Dict[str, Any] = {
        'emulation': emulation,
        'cookie_provider': wreq.Jar(),
        'redirect': wreq.redirect.Policy.limited(10),
        'tls_verify': bool(verify),
    }
    if timeout:
        kwargs['timeout'] = _td(timeout)
    if proxy_url:
        kwargs['proxies'] = [wreq.Proxy.all(proxy_url)]
    if http1_only:
        kwargs['http1_only'] = True
    return kwargs


def _wreq_request_kwargs(prepared: Any, body: Optional[bytes], timeout: Optional[float], allow_redirects: bool) -> Dict[str, Any]:
    import wreq

    kw: Dict[str, Any] = {
        'headers': prepared.headers,
        'orig_headers': prepared.order,
        'default_headers': prepared.default_headers,
    }
    if body is not None:
        kw['body'] = body
    if timeout:
        kw['timeout'] = _td(timeout)
    if not allow_redirects:
        kw['redirect'] = wreq.redirect.Policy.none()
    return kw


def _wreq_headers(resp: Any) -> Tuple[List[Tuple[str, str]], List[str]]:
    headers: List[Tuple[str, str]] = []
    cookies: List[str] = []
    for k, v in resp.headers:
        name = bytes(k).decode('latin-1').lower()
        value = bytes(v).decode('latin-1')
        headers.append((name, value))
        if name == 'set-cookie':
            cookies.append(value)
    return headers, cookies


def _wreq_version(resp: Any) -> str:
    try:
        return str(resp.version).split('.')[-1]
    except Exception:  # noqa: BLE001
        return ''


def _wreq_method(method: str) -> Any:
    import wreq

    return getattr(wreq.Method, method.upper())


def _add_cookie(jar: Any, cookie: str, url: str) -> None:
    for part in cookie.split(';'):
        part = part.strip()
        if '=' in part:
            jar.add(part, url)


class WreqAsyncTransport:
    name = 'wreq'

    def new_client(self, profile: Any, proxy_url: Optional[str], timeout: Optional[float], verify: bool, http1_only: bool) -> Any:
        import wreq

        return wreq.Client(**_wreq_client_kwargs(profile, proxy_url, timeout, verify, http1_only))

    def add_cookies(self, client: Any, cookie: str, url: str) -> None:
        if client.cookie_jar is not None:
            _add_cookie(client.cookie_jar, cookie, url)

    def cookies(self, client: Any) -> Dict[str, str]:
        jar = client.cookie_jar
        return {c.name: c.value for c in jar.get_all()} if jar is not None else {}

    async def send(self, client: Any, method: str, url: str, prepared: Any, body: Optional[bytes], timeout: Optional[float], allow_redirects: bool) -> RawResponse:
        try:
            resp = await client.request(_wreq_method(method), url, **_wreq_request_kwargs(prepared, body, timeout, allow_redirects))
            async with resp:
                data = bytes(await resp.bytes())
                headers, cookies = _wreq_headers(resp)
                return RawResponse(resp.status.as_int(), headers, data, str(resp.url), cookies, _wreq_version(resp))
        except Exception as exc:  # noqa: BLE001 - normalized below
            if type(exc).__module__ in ('exceptions', 'wreq.exceptions', 'wreq'):
                raise TransportError(_wreq_error_kind(exc), str(exc), exc) from exc
            raise

    async def close(self, client: Any) -> None:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass


class WreqSyncTransport:
    name = 'wreq'

    def new_client(self, profile: Any, proxy_url: Optional[str], timeout: Optional[float], verify: bool, http1_only: bool) -> Any:
        from wreq.blocking import Client

        return Client(**_wreq_client_kwargs(profile, proxy_url, timeout, verify, http1_only))

    add_cookies = WreqAsyncTransport.add_cookies
    cookies = WreqAsyncTransport.cookies

    def send(self, client: Any, method: str, url: str, prepared: Any, body: Optional[bytes], timeout: Optional[float], allow_redirects: bool) -> RawResponse:
        try:
            resp = client.request(_wreq_method(method), url, **_wreq_request_kwargs(prepared, body, timeout, allow_redirects))
            with resp:
                data = bytes(resp.bytes())
                headers, cookies = _wreq_headers(resp)
                return RawResponse(resp.status.as_int(), headers, data, str(resp.url), cookies, _wreq_version(resp))
        except Exception as exc:  # noqa: BLE001
            if type(exc).__module__ in ('exceptions', 'wreq.exceptions', 'wreq'):
                raise TransportError(_wreq_error_kind(exc), str(exc), exc) from exc
            raise

    def close(self, client: Any) -> None:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass


# ─── curl_cffi (fallback) ───────────────────────────────────────────────────


def _curl_headers(resp: Any) -> Tuple[List[Tuple[str, str]], List[str]]:
    headers: List[Tuple[str, str]] = []
    cookies: List[str] = []
    try:
        items = resp.headers.multi_items()
    except AttributeError:
        items = list(resp.headers.items())
    for k, v in items:
        name = str(k).lower()
        headers.append((name, str(v)))
        if name == 'set-cookie':
            cookies.append(str(v))
    return headers, cookies


def _curl_error_kind(exc: BaseException) -> str:
    text = f'{type(exc).__name__} {exc}'.lower()
    for key, kind in (('timeout', 'timeout'), ('timed out', 'timeout'), ('proxy', 'proxy'), ('ssl', 'tls'), ('tls', 'tls'), ('resolve', 'connect'), ('connect', 'connect'), ('reset', 'reset')):
        if key in text:
            return kind
    return 'request'


def _curl_session_kwargs(target: str, proxy_url: Optional[str], timeout: Optional[float], verify: bool, http1_only: bool) -> Dict[str, Any]:
    kw: Dict[str, Any] = {'impersonate': target, 'verify': verify}
    if timeout:
        kw['timeout'] = timeout
    if proxy_url:
        kw['proxy'] = proxy_url
    if http1_only:
        try:
            from curl_cffi import CurlHttpVersion

            kw['http_version'] = CurlHttpVersion.V1_1
        except Exception:  # noqa: BLE001
            pass
    return kw


def _curl_request_headers(prepared: Any) -> Dict[str, str]:
    # curl_cffi brings its own UA/client hints for its target; only send the
    # request-dependent values, never the wreq profile's UA.
    skip = {'user-agent', 'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform', 'accept-encoding', 'priority', 'te'}
    return {k: v for k, v in prepared.headers.items() if k not in skip}


class CurlAsyncTransport:
    name = 'curl_cffi'

    def __init__(self, targets: Dict[str, str]) -> None:
        self.targets = targets

    def new_client(self, profile: Any, proxy_url: Optional[str], timeout: Optional[float], verify: bool, http1_only: bool) -> Any:
        from curl_cffi.requests import AsyncSession

        target = self.targets.get(f'{profile.family}-{profile.os}') or self.targets.get(profile.family, 'chrome')
        return AsyncSession(**_curl_session_kwargs(target, proxy_url, timeout, verify, http1_only))

    def add_cookies(self, client: Any, cookie: str, url: str) -> None:
        from urllib.parse import urlsplit

        host = urlsplit(url).hostname or ''
        for part in cookie.split(';'):
            if '=' in part:
                k, v = part.strip().split('=', 1)
                client.cookies.set(k, v, domain=host)

    def cookies(self, client: Any) -> Dict[str, str]:
        try:
            return {c.name: c.value for c in client.cookies.jar}
        except Exception:  # noqa: BLE001
            return dict(client.cookies)

    async def send(self, client: Any, method: str, url: str, prepared: Any, body: Optional[bytes], timeout: Optional[float], allow_redirects: bool) -> RawResponse:
        try:
            resp = await client.request(method.upper(), url, headers=_curl_request_headers(prepared), data=body, timeout=timeout, allow_redirects=allow_redirects)
        except Exception as exc:  # noqa: BLE001
            raise TransportError(_curl_error_kind(exc), str(exc), exc) from exc
        headers, cookies = _curl_headers(resp)
        return RawResponse(int(resp.status_code), headers, bytes(resp.content or b''), str(resp.url), cookies, '')

    async def close(self, client: Any) -> None:
        try:
            await client.close()
        except Exception:  # noqa: BLE001
            pass


class CurlSyncTransport(CurlAsyncTransport):
    def new_client(self, profile: Any, proxy_url: Optional[str], timeout: Optional[float], verify: bool, http1_only: bool) -> Any:
        from curl_cffi.requests import Session

        target = self.targets.get(f'{profile.family}-{profile.os}') or self.targets.get(profile.family, 'chrome')
        return Session(**_curl_session_kwargs(target, proxy_url, timeout, verify, http1_only))

    def send(self, client: Any, method: str, url: str, prepared: Any, body: Optional[bytes], timeout: Optional[float], allow_redirects: bool) -> RawResponse:  # type: ignore[override]
        try:
            resp = client.request(method.upper(), url, headers=_curl_request_headers(prepared), data=body, timeout=timeout, allow_redirects=allow_redirects)
        except Exception as exc:  # noqa: BLE001
            raise TransportError(_curl_error_kind(exc), str(exc), exc) from exc
        headers, cookies = _curl_headers(resp)
        return RawResponse(int(resp.status_code), headers, bytes(resp.content or b''), str(resp.url), cookies, '')

    def close(self, client: Any) -> None:  # type: ignore[override]
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass
