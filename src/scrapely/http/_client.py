"""AsyncClient / Client: the anti-blocking HTTP client."""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import urlencode, urlsplit, urlunsplit

from ._detect import detect_block, parse_retry_after
from ._headers import accept_language_for, country_for_locale, encode_body, pick_accept_language, pick_country, prepare
from ._options import FingerprintOptions
from ._policy import BackoffPolicy, HostState, Identity, RetryPolicy, country_from_proxy, decide, new_session_id
from ._profiles import BrowserProfile, impersonate_to_options, load_table, pick, select_candidates
from ._response import Response
from ._stats import RUN_STATS, HttpStats
from ._transport import (
    CurlAsyncTransport,
    CurlSyncTransport,
    RawResponse,
    TransportError,
    WreqAsyncTransport,
    WreqSyncTransport,
    resolve_backend,
)

logger = logging.getLogger('scrapely.http')

IsBlocked = Callable[[Response], Any]
_warned_ignored: set = set()
_warn_lock = threading.Lock()


def _warn_ignored(names: Sequence[str]) -> None:
    with _warn_lock:
        new = [n for n in names if n not in _warned_ignored]
        _warned_ignored.update(new)
    if new:
        logger.warning(
            'scrapely.http: ignored caller header(s) %s: the browser profile sets them so they match its TLS '
            'fingerprint. Pass allow_header_override=True to send them anyway.',
            ', '.join(sorted(new)),
        )


def _with_params(url: str, params: Any) -> str:
    if not params:
        return url
    items = list(params.items()) if isinstance(params, Mapping) else list(params)
    query = urlencode([(k, v) for k, v in items if v is not None], doseq=True)
    if not query:
        return url
    parts = urlsplit(url)
    joined = f'{parts.query}&{query}' if parts.query else query
    return urlunsplit((parts.scheme, parts.netloc, parts.path, joined, parts.fragment))


def _normalize_url(url: str) -> str:
    """``https://host?q`` → ``https://host/?q``: an empty path is sent as ``/``, as browsers do.

    wreq 0.13 sends ``:path: ?q`` over HTTP/2 for such URLs, and servers
    (Cloudflare, nginx) answer 400 Bad Request.
    """
    parts = urlsplit(url)
    if parts.scheme.lower() in ('http', 'https') and parts.netloc and not parts.path:
        return urlunsplit((parts.scheme, parts.netloc, '/', parts.query, parts.fragment))
    return url


def _can_pick_country(proxy_configuration: Any) -> bool:
    """True for a platform proxy that lets each identity choose its exit country."""
    check = getattr(proxy_configuration, '_can_pick_country', None)
    if not callable(check):
        return False
    try:
        return bool(check())
    except Exception:  # noqa: BLE001 - a foreign proxy configuration: keep its own behaviour
        return False


def _rotations(max_rotations: Optional[int], max_retries: Optional[int], default: Optional[int]) -> Optional[int]:
    """``max_rotations`` wins; ``max_retries`` is its older name."""
    if max_rotations is not None:
        return max(0, int(max_rotations))
    if max_retries is not None:
        return max(0, int(max_retries))
    return default


def _proxy_from_proxies(proxies: Any) -> Optional[str]:
    if not proxies:
        return None
    if isinstance(proxies, str):
        return proxies
    for key in ('https', 'all', 'http', 'https://', 'http://'):
        if proxies.get(key):
            return str(proxies[key])
    return None


class _Pin:
    """Holder of a pinned identity (a session)."""

    __slots__ = ('identity', 'alock', 'tlock')

    def __init__(self) -> None:
        self.identity: Optional[Identity] = None
        self.alock: Optional[asyncio.Lock] = None
        self.tlock = threading.Lock()


class CookieStore:
    """Cookies the caller sets (``client.cookies.set(...)``); applied to every identity.

    Reading (``client.cookies["name"]``, ``dict(client.cookies)``) shows the
    caller's cookies plus, for sessions, the cookies the site set on the
    session's current identity.
    """

    def __init__(self, owner: '_Base') -> None:
        self._owner = owner
        self._cookies: List[Tuple[str, str, str, str]] = []  # name, value, domain, path

    def set(self, name: str, value: str, domain: str = '', path: str = '/') -> None:
        self._cookies = [c for c in self._cookies if not (c[0] == name and c[2] == domain)]
        self._cookies.append((name, str(value), domain, path))
        self._owner._apply_cookie_all(name, str(value), domain, path)

    def update(self, cookies: Mapping[str, str], domain: str = '') -> None:
        for k, v in cookies.items():
            self.set(k, v, domain)

    def _view(self) -> Dict[str, str]:
        out = {c[0]: c[1] for c in self._cookies}
        out.update(self._owner._identity_cookies())
        return out

    def get(self, name: str, default: Any = None) -> Any:
        return self._view().get(name, default)

    def __getitem__(self, name: str) -> str:
        return self._view()[name]

    def __contains__(self, name: object) -> bool:
        return name in self._view()

    def __iter__(self) -> Any:
        return iter(self._view())

    def __len__(self) -> int:
        return len(self._view())

    def items(self) -> Any:
        return self._view().items()

    def keys(self) -> Any:
        return self._view().keys()

    def get_dict(self) -> Dict[str, str]:
        return self._view()

    def __repr__(self) -> str:
        return f'CookieStore({self._view()!r})'


class _Base:
    """Options, profile choice and bookkeeping shared by AsyncClient and Client."""

    def __init__(
        self,
        *,
        proxy_configuration: Any = None,
        proxy: Optional[str] = None,
        proxies: Any = None,
        fingerprint_options: Any = None,
        fingerprintOptions: Any = None,  # noqa: N803 - Crawlee-style alias
        impersonate: Optional[str] = None,
        max_rotations: Optional[int] = None,
        max_retries: Optional[int] = None,
        polite: bool = False,
        backoff: Optional[BackoffPolicy] = None,
        timeout: Optional[float] = 30.0,
        max_concurrency: int = 8,
        max_usage: int = 50,
        max_age: float = 30 * 60,
        sticky: bool = True,
        min_delay: float = 0.0,
        max_delay: float = 0.0,
        headers: Optional[Mapping[str, str]] = None,
        verify: bool = True,
        is_blocked: Optional[IsBlocked] = None,
        allow_header_override: bool = False,
        backend: str = 'auto',
        seed: Optional[int] = None,
        _transport: Any = None,
        _sleep: Any = None,
        _clock: Any = None,
    ) -> None:
        opts_value = fingerprint_options if fingerprint_options is not None else fingerprintOptions
        self.options = FingerprintOptions.parse(opts_value)
        if impersonate and not self.options.browsers:
            browsers, oses, devices, warning = impersonate_to_options(impersonate)
            if warning:
                logger.warning('scrapely.http: %s', warning)
            self.options.browsers = browsers
            self.options.operating_systems = self.options.operating_systems or oses
            self.options.devices = self.options.devices or devices
        self.table = load_table()
        self.candidates, notes = select_candidates(
            self.options.browsers, self.options.operating_systems, self.options.devices, self.options.strict, self.table
        )
        for note in notes:
            logger.warning('scrapely.http: %s', note)
        self.proxy_configuration = proxy_configuration
        self.fixed_proxy = proxy or _proxy_from_proxies(proxies)
        if proxy_configuration is not None and self.fixed_proxy:
            raise ValueError('pass either proxy_configuration or proxy/proxies, not both')
        # Only a proxy configuration gives each identity its own exit IP.
        self.rotates_ip = proxy_configuration is not None
        country = country_from_proxy(proxy_configuration if proxy_configuration is not None else self.fixed_proxy)
        self.proxy_country = country
        # Default Accept-Language of this client (identities draw their own, see _identity_geo).
        self.accept_language = accept_language_for(country, self.options.locales)
        # A platform proxy without a country: every identity picks its exit
        # country and sends the matching Accept-Language.
        self.picks_country = country is None and proxy_configuration is not None and _can_pick_country(proxy_configuration)
        if backoff is None and polite:
            backoff = BackoffPolicy()
        self.policy = RetryPolicy(max_rotations=_rotations(max_rotations, max_retries, 6), backoff=backoff)
        self.timeout = timeout
        self.max_concurrency = max(1, int(max_concurrency))
        self.max_usage = max_usage
        self.max_age = max_age
        self.sticky = sticky
        self.min_delay = max(0.0, float(min_delay))
        self.max_delay = max(self.min_delay, float(max_delay))
        self.default_headers = dict(headers or {})
        self.verify = verify
        self.is_blocked = is_blocked
        self.allow_header_override = allow_header_override
        self.http1_only = self.options.http_version == '1'
        self.rng = random.Random(seed)
        self._stats = HttpStats()
        self.hosts: Dict[str, HostState] = {}
        self._caller_cookies = CookieStore(self)
        self.cookies: Any = self._caller_cookies
        self._closed = False
        self._live: List[Identity] = []
        self._clock = _clock or time.monotonic
        if _transport is not None:
            self.transport = _transport
        else:
            self.backend = resolve_backend(backend)
            self.transport = self._make_transport(self.backend)

    # subclasses
    def _make_transport(self, backend: str) -> Any:  # pragma: no cover - overridden
        raise NotImplementedError

    # ── helpers ──
    def _host(self, url: str) -> Tuple[str, HostState]:
        host = (urlsplit(url).hostname or '').lower()
        state = self.hosts.get(host)
        if state is None:
            bp = self.policy.backoff
            state = self.hosts[host] = HostState(breaker=bool(bp is not None and bp.circuit_breaker))
        return host, state

    def _pick_profile(self, avoid: Optional[BrowserProfile]) -> BrowserProfile:
        return pick(self.candidates, self.rng, avoid)

    def _identity_geo(self) -> Tuple[Optional[str], str]:
        """(exit country to request from the proxy or None, Accept-Language) for a new identity.

        - ``locales``: one of them per identity, as a browser has one language
          setup; with a country-pickable proxy the exit is that locale's country.
        - proxy with a country: a language setup common in that country.
        - platform proxy without a country: a country weighted like real
          traffic, used for both the proxy session and the language.
        - otherwise (no proxy, own proxy URLs, datacenter): the client default.
        """
        locales = self.options.locales
        if locales:
            loc = locales[0] if len(locales) == 1 else self.rng.choice(locales)
            return (country_for_locale(loc) if self.picks_country else None), accept_language_for(None, [loc])
        if self.proxy_country:
            return None, pick_accept_language(self.proxy_country, self.rng)
        if self.picks_country:
            cc = pick_country(self.rng)
            return cc, pick_accept_language(cc, self.rng)
        return None, self.accept_language

    def _new_identity(
        self,
        client: Any,
        profile: BrowserProfile,
        session_id: str,
        proxy_url: Optional[str],
        accept_language: Optional[str] = None,
        country: Optional[str] = None,
    ) -> Identity:
        ident = Identity(
            session_id=session_id,
            profile=profile,
            proxy_url=proxy_url,
            client=client,
            accept_language=accept_language or self.accept_language,
            created_at=self._clock(),
            country=country or self.proxy_country,
        )
        for name, value, domain, path in self._caller_cookies._cookies:
            self._apply_cookie(ident, name, value, domain, path)
        self._stats.record_identity(True)
        self._live.append(ident)
        return ident

    def _apply_cookie(self, ident: Identity, name: str, value: str, domain: str, path: str) -> None:
        host = domain.lstrip('.') if domain else ''
        if not host:
            return
        cookie = f'{name}={value}; Path={path or "/"}'
        if domain:
            cookie += f'; Domain={domain}'
        try:
            jar = getattr(ident.client, 'cookie_jar', None)
            if jar is not None:
                jar.add(cookie, f'https://{host}/')
            else:
                self.transport.add_cookies(ident.client, f'{name}={value}', f'https://{host}/')
        except Exception as exc:  # noqa: BLE001
            logger.debug('scrapely.http: could not set cookie %s: %s', name, exc)

    def _apply_cookie_all(self, name: str, value: str, domain: str, path: str) -> None:
        for ident in list(self._live):
            if not ident.retired:
                self._apply_cookie(ident, name, value, domain, path)

    def _identity_cookies(self) -> Dict[str, str]:
        return {}

    def _request_cookie_header(self, cookies: Optional[Mapping[str, str]]) -> str:
        """Cookies to store for this request's site: caller cookies without a domain + ``cookies=``."""
        pairs = [(n, v) for n, v, d, _ in self._caller_cookies._cookies if not d]
        if cookies:
            pairs.extend((str(k), str(v)) for k, v in dict(cookies).items())
        return '; '.join(f'{k}={v}' for k, v in pairs)

    def _prepare(self, ident: Identity, method: str, url: str, kind: str, headers: Any, ctype: Optional[str], has_body: bool) -> Any:
        merged: Dict[str, str] = dict(self.default_headers)
        if headers:
            items = headers.items() if isinstance(headers, Mapping) else headers
            for k, v in items:
                for existing in [e for e in merged if e.lower() == str(k).lower()]:
                    del merged[existing]
                merged[k] = v
        prep = prepare(
            table=self.table,
            profile=ident.profile,
            kind=kind,
            method=method,
            url=url,
            headers=merged,
            accept_language=ident.accept_language,
            body_content_type=ctype,
            has_body=has_body,
            allow_header_override=self.allow_header_override,
        )
        if prep.ignored:
            _warn_ignored(prep.ignored)
        return prep

    def _evaluate(self, ident: Identity, raw: RawResponse, started: float, attempts: int, method: str, check: Optional[IsBlocked]) -> Tuple[Response, Optional[str]]:
        hdrs = {}
        for k, v in raw.headers:
            hdrs.setdefault(k, v)
        reason = detect_block(raw.status, hdrs, raw.body, raw.set_cookies)
        resp = Response(
            raw.status,
            raw.headers,
            raw.body,
            raw.url,
            self.transport.cookies(ident.client),
            profile=ident.profile.label,
            attempts=attempts,
            elapsed=self._clock() - started,
            http_version=raw.http_version,
            request_method=method,
        )
        if reason is None and check is not None:
            verdict = None
            try:
                verdict = check(resp)
            except Exception as exc:  # noqa: BLE001 - a broken callback must not break the request
                logger.debug('scrapely.http: is_blocked raised %s', exc)
            if verdict:
                reason = verdict if isinstance(verdict, str) else 'custom'
        return resp, reason

    def _record(self, ident: Identity, raw: Optional[RawResponse], reason: Optional[str], error: Optional[TransportError], host: str, hs: HostState) -> None:
        if error is not None:
            self._stats.record_error(ident.profile.label, error.kind)
        elif raw is not None:
            self._stats.record_attempt(ident.profile.label, raw.status, len(raw.body), reason)
        if hs.record(reason is not None) and not hs.warned:
            hs.warned = True
            self._stats.record_circuit(host)
            logger.warning(
                'scrapely.http: %s blocks most requests (%.0f%% of the last %d); slowing it down to one request at a time, 2-5 s apart',
                host,
                hs.blocked_ratio() * 100,
                len(hs.outcomes),
            )

    def _pace_delay(self, ident: Identity, hs: HostState) -> float:
        now = self._clock()
        wait = 0.0
        if ident.last_at and ident.next_delay:
            wait = max(wait, ident.last_at + ident.next_delay - now)
        if hs.slow:
            wait = max(wait, hs.next_at - now)
        return max(0.0, wait)

    def _after_send(self, ident: Identity, hs: HostState) -> None:
        now = self._clock()
        ident.usage += 1
        ident.last_at = now
        ident.next_delay = self.rng.uniform(self.min_delay, self.max_delay) if self.max_delay > 0 else 0.0
        if hs.slow:
            hs.next_at = now + self.rng.uniform(2.0, 5.0)

    # curl_cffi-style sessions are often opened by the dozen; they log their
    # summary at debug level and the run total once at exit instead.
    _quiet_summary = False

    def _summary_and_stats(self) -> Dict[str, Any]:
        RUN_STATS.merge(self._stats)
        if self._stats.requests:
            if self._quiet_summary:
                logger.debug(self._stats.summary())
                _register_run_summary()
            else:
                logger.info(self._stats.summary())
        return RUN_STATS.as_dict()

    def stats(self) -> Dict[str, Any]:
        """Counters of this client: requests, blocks per vendor, rotations (per reason and host), identities, bytes, per profile."""
        return self._stats.as_dict()


# ─── async ──────────────────────────────────────────────────────────────────


class AsyncClient(_Base):
    """Async anti-blocking HTTP client.

    Each request goes out on a browser identity (profile + proxy session +
    cookie jar) from a small pool, so concurrent requests never share cookies.
    A block (429, block page, captcha, challenge), a 5xx or a network error
    rotates the request to a brand-new identity at once, without waiting.
    ``polite=True`` (or ``backoff=BackoffPolicy(...)``) adds Retry-After,
    per-host backoff and a slow mode for hosts that block most requests.

    Usage::

        async with AsyncClient(proxy_configuration=proxy) as http:
            r = await http.get(url)
            r = await http.post(api, json=payload, kind="json")
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sleep = kwargs.get('_sleep') or asyncio.sleep
        self._idle: List[Identity] = []
        self._count = 0
        self._cond: Optional[asyncio.Condition] = None
        self._slow_locks: Dict[str, asyncio.Lock] = {}

    def _make_transport(self, backend: str) -> Any:
        return WreqAsyncTransport() if backend == 'wreq' else CurlAsyncTransport(self.table.curl_cffi_targets)

    def _condition(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    async def _proxy_url(self, session_id: str, country: Optional[str] = None) -> Optional[str]:
        if self.proxy_configuration is not None:
            if country:
                url = await self.proxy_configuration.new_url(session_id=session_id, country_code=country)
            else:
                url = await self.proxy_configuration.new_url(session_id=session_id)
            return str(url) if url else None
        return self.fixed_proxy

    async def _open(self, avoid: Optional[BrowserProfile]) -> Identity:
        profile = self._pick_profile(avoid)
        sid = new_session_id(self.rng)
        country, al = self._identity_geo()
        proxy_url = await self._proxy_url(sid, country)
        client = self.transport.new_client(profile, proxy_url, self.timeout, self.verify, self.http1_only)
        return self._new_identity(client, profile, sid, proxy_url, al, country)

    async def _retire(self, ident: Identity, force: bool = False) -> None:
        """Retire an identity; its client closes once no request uses it any more."""
        if not ident.retired:
            ident.retired = True
            self._stats.record_identity(False)
        if ident in self._live:
            self._live.remove(ident)
        if (force or ident.in_flight <= 0) and not ident.closed:
            ident.closed = True
            await self.transport.close(ident.client)

    async def _acquire(self, pin: Optional[_Pin], avoid: Optional[BrowserProfile]) -> Identity:
        if self._closed:
            raise RuntimeError('the HTTP client is closed')
        if pin is not None:
            if pin.alock is None:
                pin.alock = asyncio.Lock()
            async with pin.alock:
                ident = pin.identity
                if ident is not None and ident.expired(self._clock(), self.max_usage, self.max_age):
                    pin.identity = None
                    await self._retire(ident)
                    ident = None
                if ident is None:
                    ident = pin.identity = await self._open(avoid)
                ident.in_flight += 1
                return ident
        cond = self._condition()
        async with cond:
            while True:
                while self._idle:
                    ident = self._idle.pop(0)
                    if ident.expired(self._clock(), self.max_usage, self.max_age):
                        self._count -= 1
                        await self._retire(ident)
                        continue
                    ident.in_flight += 1
                    return ident
                if self._count < self.max_concurrency:
                    self._count += 1
                    break
                await cond.wait()
        try:
            ident = await self._open(avoid)
            ident.in_flight += 1
            return ident
        except BaseException:
            async with cond:
                self._count -= 1
                cond.notify()
            raise

    async def _release(self, pin: Optional[_Pin], ident: Identity, retire: bool) -> None:
        ident.in_flight -= 1
        if pin is not None:
            if retire or not self.sticky:
                if pin.identity is ident:
                    pin.identity = None
                await self._retire(ident)
            elif ident.retired and ident.in_flight <= 0:
                await self._retire(ident)
            return
        cond = self._condition()
        drop = retire or not self.sticky or ident.expired(self._clock(), self.max_usage, self.max_age)
        async with cond:
            if drop:
                self._count -= 1
            else:
                self._idle.append(ident)
            cond.notify()
        if drop:
            await self._retire(ident)

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        data: Any = None,
        json: Any = None,
        content: Any = None,
        cookies: Optional[Mapping[str, str]] = None,
        kind: str = 'navigate',
        timeout: Optional[float] = None,
        allow_redirects: bool = True,
        max_rotations: Optional[int] = None,
        max_retries: Optional[int] = None,
        is_blocked: Optional[IsBlocked] = None,
        _pin: Optional[_Pin] = None,
        **_ignored: Any,
    ) -> Response:
        """Send a request; a block or error rotates to a fresh identity at once. Returns the last response."""
        method = method.upper()
        url = _normalize_url(_with_params(url, params))
        body, ctype = encode_body(data, json, content)
        host, hs = self._host(url)
        started = self._clock()
        attempt = 0
        rotations = 0
        same_used = 0
        rotation_cap = _rotations(max_rotations, max_retries, None)
        avoid: Optional[BrowserProfile] = None
        ident: Optional[Identity] = None
        check = is_blocked or self.is_blocked
        while True:
            if ident is None:
                ident = await self._acquire(_pin, avoid)
            slow_lock = self._slow_locks.setdefault(host, asyncio.Lock()) if hs.slow else None
            raw: Optional[RawResponse] = None
            error: Optional[TransportError] = None
            resp: Optional[Response] = None
            reason: Optional[str] = None
            try:
                if slow_lock is not None:
                    await slow_lock.acquire()
                try:
                    wait = self._pace_delay(ident, hs)
                    if wait > 0:
                        await self._sleep(wait)
                    prep = self._prepare(ident, method, url, kind, headers, ctype, body is not None)
                    if prep.cookie:
                        self.transport.add_cookies(ident.client, prep.cookie, url)
                    extra_cookies = self._request_cookie_header(cookies)
                    if extra_cookies:
                        self.transport.add_cookies(ident.client, extra_cookies, url)
                    try:
                        raw = await self.transport.send(ident.client, method, url, prep, body, timeout or self.timeout, allow_redirects)
                    except TransportError as exc:
                        error = exc
                    self._after_send(ident, hs)
                finally:
                    if slow_lock is not None:
                        slow_lock.release()
                if raw is not None:
                    resp, reason = self._evaluate(ident, raw, started, attempt + 1, method, check)
                self._record(ident, raw, reason, error, host, hs)
                decision = decide(
                    self.policy,
                    rotations_done=rotations,
                    status=raw.status if raw is not None else None,
                    blocked=reason,
                    error_kind=error.kind if error is not None else None,
                    retry_after=parse_retry_after(resp.headers.get('retry-after')) if resp is not None else None,
                    same_identity_retries=same_used,
                    host=hs,
                    rng=self.rng,
                    max_rotations=rotation_cap,
                    rotates_ip=self.rotates_ip,
                )
            except BaseException:
                # Cancelled or failed in our own code: nothing says the identity is burnt.
                await self._release(_pin, ident, retire=False)
                raise
            if not decision.retry:
                await self._release(_pin, ident, retire=decision.retire)
                ok = resp is not None and reason is None and resp.status_code < 500
                self._stats.record_request(ok)
                if resp is None:
                    assert error is not None
                    raise error
                resp.blocked = reason
                return resp
            self._stats.record_retry()
            logger.debug('scrapely.http: retrying %s %s on %s: %s', method, url, 'the same identity' if decision.same_identity else 'a fresh identity', decision.reason)
            if decision.same_identity:
                same_used += 1
            else:
                # New proxy session, new profile (a different one when there is a choice), empty jar.
                rotations += 1
                self._stats.record_rotation(decision.reason, host)
                avoid = ident.profile
                await self._release(_pin, ident, retire=True)
                ident = None
            if decision.delay > 0:
                await self._sleep(decision.delay)
            attempt += 1

    async def get(self, url: str, **kwargs: Any) -> Response:
        return await self.request('GET', url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Response:
        return await self.request('POST', url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> Response:
        return await self.request('PUT', url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> Response:
        return await self.request('PATCH', url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> Response:
        return await self.request('DELETE', url, **kwargs)

    async def head(self, url: str, **kwargs: Any) -> Response:
        return await self.request('HEAD', url, **kwargs)

    async def options(self, url: str, **kwargs: Any) -> Response:
        return await self.request('OPTIONS', url, **kwargs)

    def session(self) -> 'AsyncHttpSession':
        """A pinned identity for multi-step flows (homepage → API with its cookies).

        All requests of the session use one profile, proxy session and cookie
        jar. On a block the whole identity is retired and the session continues
        on a fresh one (new IP, new profile, empty jar).
        """
        return AsyncHttpSession(self)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        for ident in list(self._live):
            await self._retire(ident, force=True)
        self._idle.clear()
        await _write_http_stats_async(self._summary_and_stats())

    close = aclose

    async def __aenter__(self) -> 'AsyncClient':
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()


class AsyncHttpSession:
    """A pinned identity on an ``AsyncClient`` (see ``AsyncClient.session()``)."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client
        self._pin = _Pin()
        self.cookies = _SessionCookies(self)

    @property
    def profile(self) -> Optional[str]:
        return self._pin.identity.profile.label if self._pin.identity else None

    @property
    def session_id(self) -> Optional[str]:
        return self._pin.identity.session_id if self._pin.identity else None

    @property
    def accept_language(self) -> Optional[str]:
        """Accept-Language of the current identity."""
        return self._pin.identity.accept_language if self._pin.identity else None

    @property
    def country(self) -> Optional[str]:
        """Exit country the current identity asked the proxy for (None when the proxy chooses)."""
        return self._pin.identity.country if self._pin.identity else None

    async def request(self, method: str, url: str, **kwargs: Any) -> Response:
        return await self._client.request(method, url, _pin=self._pin, **kwargs)

    async def get(self, url: str, **kwargs: Any) -> Response:
        return await self.request('GET', url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Response:
        return await self.request('POST', url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> Response:
        return await self.request('PUT', url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> Response:
        return await self.request('PATCH', url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> Response:
        return await self.request('DELETE', url, **kwargs)

    async def head(self, url: str, **kwargs: Any) -> Response:
        return await self.request('HEAD', url, **kwargs)

    async def aclose(self) -> None:
        if self._pin.identity is not None:
            ident, self._pin.identity = self._pin.identity, None
            await self._client._retire(ident, force=True)

    close = aclose

    async def __aenter__(self) -> 'AsyncHttpSession':
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()


class _SessionCookies(CookieStore):
    """Cookies of a session: the client's caller cookies plus the session identity's jar."""

    def __init__(self, session: Any) -> None:
        self._session = session
        super().__init__(session._client)

    def set(self, name: str, value: str, domain: str = '', path: str = '/') -> None:
        self._session._client._caller_cookies.set(name, value, domain, path)

    def _view(self) -> Dict[str, str]:
        out = self._session._client._caller_cookies._view()
        ident = self._session._pin.identity
        if ident is not None:
            out.update(self._session._client.transport.cookies(ident.client))
        return out


# ─── sync ───────────────────────────────────────────────────────────────────


class Client(_Base):
    """Synchronous anti-blocking HTTP client for thread-based actors (same options as ``AsyncClient``)."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sleep = kwargs.get('_sleep') or time.sleep
        self._idle: List[Identity] = []
        self._count = 0
        self._cond = threading.Condition()
        self._slow_locks: Dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def _make_transport(self, backend: str) -> Any:
        return WreqSyncTransport() if backend == 'wreq' else CurlSyncTransport(self.table.curl_cffi_targets)

    def _proxy_url(self, session_id: str, country: Optional[str] = None) -> Optional[str]:
        pc = self.proxy_configuration
        if pc is None:
            return self.fixed_proxy
        sync = getattr(pc, '_new_url_sync', None)
        if sync is not None and not getattr(pc, '_new_url_function', None):
            url = sync(session_id, country_code=country) if country else sync(session_id)
        else:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                url = asyncio.run(pc.new_url(session_id=session_id, country_code=country) if country else pc.new_url(session_id=session_id))
            else:
                raise RuntimeError('scrapely.http.Client: use AsyncClient inside async code (this proxy configuration needs await)')
        return str(url) if url else None

    def _open(self, avoid: Optional[BrowserProfile]) -> Identity:
        profile = self._pick_profile(avoid)
        with self._lock:
            sid = new_session_id(self.rng)
            country, al = self._identity_geo()
        proxy_url = self._proxy_url(sid, country)
        client = self.transport.new_client(profile, proxy_url, self.timeout, self.verify, self.http1_only)
        with self._lock:
            return self._new_identity(client, profile, sid, proxy_url, al, country)

    def _retire(self, ident: Identity, force: bool = False) -> None:
        """Retire an identity; its client closes once no request uses it any more."""
        with self._lock:
            if not ident.retired:
                ident.retired = True
                self._stats.record_identity(False)
            if ident in self._live:
                self._live.remove(ident)
            close = (force or ident.in_flight <= 0) and not ident.closed
            if close:
                ident.closed = True
        if close:
            self.transport.close(ident.client)

    def _acquire(self, pin: Optional[_Pin], avoid: Optional[BrowserProfile]) -> Identity:
        if self._closed:
            raise RuntimeError('the HTTP client is closed')
        if pin is not None:
            with pin.tlock:
                ident = pin.identity
                if ident is not None and ident.expired(self._clock(), self.max_usage, self.max_age):
                    pin.identity = None
                    self._retire(ident)
                    ident = None
                if ident is None:
                    ident = pin.identity = self._open(avoid)
                with self._lock:
                    ident.in_flight += 1
                return ident
        with self._cond:
            while True:
                expired: List[Identity] = []
                found: Optional[Identity] = None
                while self._idle:
                    cand = self._idle.pop(0)
                    if cand.expired(self._clock(), self.max_usage, self.max_age):
                        self._count -= 1
                        expired.append(cand)
                        continue
                    found = cand
                    break
                for e in expired:
                    self._retire(e)
                if found is not None:
                    found.in_flight += 1
                    return found
                if self._count < self.max_concurrency:
                    self._count += 1
                    break
                self._cond.wait()
        try:
            ident = self._open(avoid)
            with self._lock:
                ident.in_flight += 1
            return ident
        except BaseException:
            with self._cond:
                self._count -= 1
                self._cond.notify()
            raise

    def _release(self, pin: Optional[_Pin], ident: Identity, retire: bool) -> None:
        with self._lock:
            ident.in_flight -= 1
        if pin is not None:
            if retire or not self.sticky:
                with pin.tlock:
                    if pin.identity is ident:
                        pin.identity = None
                self._retire(ident)
            elif ident.retired and ident.in_flight <= 0:
                self._retire(ident)
            return
        drop = retire or not self.sticky or ident.expired(self._clock(), self.max_usage, self.max_age)
        with self._cond:
            if drop:
                self._count -= 1
            else:
                self._idle.append(ident)
            self._cond.notify()
        if drop:
            self._retire(ident)

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        data: Any = None,
        json: Any = None,
        content: Any = None,
        cookies: Optional[Mapping[str, str]] = None,
        kind: str = 'navigate',
        timeout: Optional[float] = None,
        allow_redirects: bool = True,
        max_rotations: Optional[int] = None,
        max_retries: Optional[int] = None,
        is_blocked: Optional[IsBlocked] = None,
        _pin: Optional[_Pin] = None,
        **_ignored: Any,
    ) -> Response:
        """Send a request; a block or error rotates to a fresh identity at once. Returns the last response."""
        method = method.upper()
        url = _normalize_url(_with_params(url, params))
        body, ctype = encode_body(data, json, content)
        with self._lock:
            host, hs = self._host(url)
        started = self._clock()
        attempt = 0
        rotations = 0
        same_used = 0
        rotation_cap = _rotations(max_rotations, max_retries, None)
        avoid: Optional[BrowserProfile] = None
        ident: Optional[Identity] = None
        check = is_blocked or self.is_blocked
        while True:
            if ident is None:
                ident = self._acquire(_pin, avoid)
            with self._lock:
                slow_lock = self._slow_locks.setdefault(host, threading.Lock()) if hs.slow else None
            raw: Optional[RawResponse] = None
            error: Optional[TransportError] = None
            resp: Optional[Response] = None
            reason: Optional[str] = None
            try:
                if slow_lock is not None:
                    slow_lock.acquire()
                try:
                    with self._lock:
                        wait = self._pace_delay(ident, hs)
                    if wait > 0:
                        self._sleep(wait)
                    prep = self._prepare(ident, method, url, kind, headers, ctype, body is not None)
                    if prep.cookie:
                        self.transport.add_cookies(ident.client, prep.cookie, url)
                    extra_cookies = self._request_cookie_header(cookies)
                    if extra_cookies:
                        self.transport.add_cookies(ident.client, extra_cookies, url)
                    try:
                        raw = self.transport.send(ident.client, method, url, prep, body, timeout or self.timeout, allow_redirects)
                    except TransportError as exc:
                        error = exc
                    with self._lock:
                        self._after_send(ident, hs)
                finally:
                    if slow_lock is not None:
                        slow_lock.release()
                if raw is not None:
                    resp, reason = self._evaluate(ident, raw, started, attempt + 1, method, check)
                with self._lock:
                    self._record(ident, raw, reason, error, host, hs)
                    decision = decide(
                        self.policy,
                        rotations_done=rotations,
                        status=raw.status if raw is not None else None,
                        blocked=reason,
                        error_kind=error.kind if error is not None else None,
                        retry_after=parse_retry_after(resp.headers.get('retry-after')) if resp is not None else None,
                        same_identity_retries=same_used,
                        host=hs,
                        rng=self.rng,
                        max_rotations=rotation_cap,
                        rotates_ip=self.rotates_ip,
                    )
            except BaseException:
                # Failed in our own code: nothing says the identity is burnt.
                self._release(_pin, ident, retire=False)
                raise
            if not decision.retry:
                self._release(_pin, ident, retire=decision.retire)
                ok = resp is not None and reason is None and resp.status_code < 500
                self._stats.record_request(ok)
                if resp is None:
                    assert error is not None
                    raise error
                resp.blocked = reason
                return resp
            self._stats.record_retry()
            logger.debug('scrapely.http: retrying %s %s: %s', method, url, decision.reason)
            if decision.same_identity:
                same_used += 1
            else:
                rotations += 1
                self._stats.record_rotation(decision.reason, host)
                avoid = ident.profile
                self._release(_pin, ident, retire=True)
                ident = None
            if decision.delay > 0:
                self._sleep(decision.delay)
            attempt += 1

    def get(self, url: str, **kwargs: Any) -> Response:
        return self.request('GET', url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Response:
        return self.request('POST', url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> Response:
        return self.request('PUT', url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> Response:
        return self.request('PATCH', url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> Response:
        return self.request('DELETE', url, **kwargs)

    def head(self, url: str, **kwargs: Any) -> Response:
        return self.request('HEAD', url, **kwargs)

    def options(self, url: str, **kwargs: Any) -> Response:
        return self.request('OPTIONS', url, **kwargs)

    def session(self) -> 'HttpSession':
        """A pinned identity for multi-step flows (see ``AsyncClient.session``)."""
        return HttpSession(self)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for ident in list(self._live):
            self._retire(ident, force=True)
        self._idle.clear()
        _write_http_stats_sync(self._summary_and_stats())

    def __enter__(self) -> 'Client':
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class HttpSession:
    """A pinned identity on a sync ``Client`` (see ``Client.session()``)."""

    def __init__(self, client: Client) -> None:
        self._client = client
        self._pin = _Pin()
        self.cookies = _SessionCookies(self)

    @property
    def profile(self) -> Optional[str]:
        return self._pin.identity.profile.label if self._pin.identity else None

    @property
    def session_id(self) -> Optional[str]:
        return self._pin.identity.session_id if self._pin.identity else None

    @property
    def accept_language(self) -> Optional[str]:
        """Accept-Language of the current identity."""
        return self._pin.identity.accept_language if self._pin.identity else None

    @property
    def country(self) -> Optional[str]:
        """Exit country the current identity asked the proxy for (None when the proxy chooses)."""
        return self._pin.identity.country if self._pin.identity else None

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        return self._client.request(method, url, _pin=self._pin, **kwargs)

    def get(self, url: str, **kwargs: Any) -> Response:
        return self.request('GET', url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Response:
        return self.request('POST', url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> Response:
        return self.request('PUT', url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> Response:
        return self.request('PATCH', url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> Response:
        return self.request('DELETE', url, **kwargs)

    def head(self, url: str, **kwargs: Any) -> Response:
        return self.request('HEAD', url, **kwargs)

    def close(self) -> None:
        if self._pin.identity is not None:
            ident, self._pin.identity = self._pin.identity, None
            self._client._retire(ident, force=True)

    def __enter__(self) -> 'HttpSession':
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


# ─── run summary and HTTP_STATS ─────────────────────────────────────────────

_run_summary_registered = False


def _log_run_summary() -> None:
    if RUN_STATS.requests:
        logger.info('%s (run total)', RUN_STATS.summary())


def _register_run_summary() -> None:
    global _run_summary_registered
    if not _run_summary_registered:
        _run_summary_registered = True
        import atexit

        atexit.register(_log_run_summary)


HTTP_STATS_KEY = 'HTTP_STATS'


def _actor() -> Any:
    try:
        from ..actor import Actor
    except Exception:  # noqa: BLE001
        return None
    return Actor if getattr(Actor, '_initialized', False) else None


async def _write_http_stats_async(data: Dict[str, Any]) -> None:
    """Best effort: the run's aggregate stats into the default key-value store."""
    actor = _actor()
    if actor is None or not data.get('requests'):
        return
    try:
        await actor.set_value(HTTP_STATS_KEY, data)
    except Exception as exc:  # noqa: BLE001 - never break the run
        logger.debug('scrapely.http: could not write %s: %s', HTTP_STATS_KEY, exc)


def _write_http_stats_sync(data: Dict[str, Any]) -> None:
    actor = _actor()
    if actor is None or not data.get('requests'):
        return
    loop = getattr(actor, '_main_loop', None)
    try:
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if loop is not None and not loop.is_closed():
            if running is loop:
                loop.create_task(_write_http_stats_async(data))
            else:
                asyncio.run_coroutine_threadsafe(_write_http_stats_async(data), loop).result(timeout=15)
    except Exception as exc:  # noqa: BLE001
        logger.debug('scrapely.http: could not write %s: %s', HTTP_STATS_KEY, exc)
