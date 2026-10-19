"""Patch Crawlee's Playwright browser pool to use Scrapely remote CDP.

Mirrors the Node SDK ``patch-browser-pool.ts``:

- On import, monkey-patch ``PlaywrightBrowserPlugin.new_browser`` when Crawlee
  browsers + Playwright are available.
- Also monkey-patch ``camoufox.AsyncNewBrowser`` when Camoufox is installed,
  so CamoufoxPlugin-based actors also connect via CDP instead of launching a
  local Camoufox browser.
- Raw Playwright usage (no Crawlee BrowserPool) is covered too: the
  ``sync_playwright()`` / ``async_playwright()`` entry points are wrapped so
  every ``BrowserType.launch()`` / ``launch_persistent_context()`` on the
  returned object connects via CDP instead of launching a local browser. This
  is what makes SDK-less actors (plain ``p.chromium.launch()`` scripts) use
  the platform browser — the patch reaches them through the base image's
  sitecustomize import at interpreter start.
- Browser launch goes through ``chromium.connect_over_cdp(CDP_URL)`` when a
  CDP URL is configured (platform runs, or local runs that opt into a remote
  browser). Requested firefox/webkit/camoufox are not denied — they are
  redirected to the remote Chromium CDP browser (antibot-patched platform
  browser).
- Local development (no ``CDP_URL`` and not on the platform) falls back to a
  normal local browser launch, exactly like Crawlee without the patch. Local
  launch is disabled on the platform unless the build is unpatched
  (``SCRAPELY_BROWSER_PATCH=0``); otherwise a worker container without
  ``CDP_URL`` is a misconfiguration and must fail loudly.
- Unpatched builds (``SCRAPELY_BROWSER_PATCH=0``, set by the worker when the
  actor's browser patch was off at build time) keep their browser binaries:
  every patched entry point falls through to the original local launch, with
  sandbox-safe Chromium args when running on the platform.
- Soft no-op when Playwright/Crawlee browsers are not installed (e.g. HTTP-only
  actors).
- Crawlee is never imported by this module (it costs ~1 s on the platform). The
  Crawlee patches are applied at once when Crawlee's browser / Playwright
  crawler modules are already loaded, otherwise by a post-import hook the
  moment those modules finish loading — before actor code can use the classes.
  The Playwright, Camoufox and pyppeteer patches are applied eagerly on import.

Location and proxy handling (the remote browser never takes a proxy):

- The Scrapely remote browser routes through its own integrated proxy,
  selected by location params on the CDP URL (``proxy_country``,
  ``proxy_state``, ``proxy_city``) plus fingerprint params (``os``,
  ``timezone``, ``language``, ``adblock``, ``solve_cloudflare``).
  ``apply_proxy_info_to_cdp_url`` is the single chokepoint that builds it:
  ``proxy_url`` is always stripped (the worker-injected one's ``country-XX``
  is kept as the run default), a platform-gateway ProxyInfo contributes its
  country / subdivision, a custom (BYOP) proxy is warned about once and
  ignored, and ``BrowserConfiguration`` params always win.
- ``PlaywrightCrawler(browser_configuration=...)`` is always accepted; the
  configuration is stored on the crawler's BrowserPool. Raw
  ``launch(browser_configuration=...)`` is accepted too and never forwarded to
  a real local launch — locally it is simply ignored.
- Crawlee passes the crawler's ``ProxyConfiguration`` per *page* as
  ``proxy_info`` (``BrowserPool.new_page``), but the remote browser's location
  is fixed at *connect* time. The patch stashes ``proxy_info`` and the pool's
  browser configuration (per asyncio task, via ``ContextVar``) when a page
  requests a browser and consumes them in ``new_browser``: the first launch
  pins the connection. Each browser is tagged with its resolved location, and
  reusing it for a page that resolves to a different location logs a warning.
- ``PlaywrightBrowserController.new_page`` drops ``proxy_info`` while CDP mode
  is active: Crawlee would otherwise turn it into per-context Playwright
  ``ProxySettings`` on top of the remote browser's own proxy.
- The Camoufox patch (``AsyncNewBrowser``) reads the same relayed values.
"""

from __future__ import annotations

import contextvars
import functools
import importlib.abc
import logging
import os
import re
import sys
from typing import Any, List, Optional, Tuple
from urllib.parse import SplitResult, parse_qsl, unquote, urlencode, urlsplit, urlunsplit

from ._browser_locations import subdivision_to_state_name
from .cdp_retry import connect_with_retry_async, wrap_connect_over_cdp

logger = logging.getLogger(__name__)

_cdp_url: Optional[str] = None

# Per-launch proxy info and browser configuration, stashed by the patched
# ``BrowserPool._get_new_page`` and consumed by the patched
# ``PlaywrightBrowserPlugin.new_browser`` / Camoufox ``AsyncNewBrowser``.
# A ContextVar is private to each asyncio task, so concurrent page/browser
# launches cannot mix their configurations.
_launch_proxy_info: contextvars.ContextVar = contextvars.ContextVar(
    'scrapely_launch_proxy_info', default=None
)
_launch_browser_configuration: contextvars.ContextVar = contextvars.ContextVar(
    'scrapely_launch_browser_configuration', default=None
)


def get_cdp_url() -> Optional[str]:
    """Lazily get CDP URL from explicit setter or ``CDP_URL`` env var."""
    return _cdp_url or os.environ.get('CDP_URL')


def set_cdp_url(url: str) -> None:
    """Set CDP URL explicitly (for testing or advanced use cases).

    Normally the CDP URL is read from the ``CDP_URL`` environment variable
    lazily (injected by scrapely-server for platform runs).
    """
    global _cdp_url
    _cdp_url = url


def _is_at_home() -> bool:
    """Whether the process runs on the Scrapely platform (worker-injected env)."""
    return bool(os.environ.get('SCRAPELY_IS_AT_HOME') or os.environ.get('APIFY_IS_AT_HOME'))


def _browser_patch_disabled() -> bool:
    """Whether this run is from an unpatched build (launch the actor's own browsers)."""
    return os.environ.get('SCRAPELY_BROWSER_PATCH') == '0'


# Platform sandbox: no crash reporter; read-only rootfs + tmpfs /tmp loses crash dirs.
_SANDBOX_CHROME_ARGS = (
    '--disable-breakpad',
    '--disable-crash-reporter',
    '--disable-dev-shm-usage',
    '--crash-dumps-dir=/tmp',
)


def _with_sandbox_args(options: Optional[dict]) -> dict:
    """Copy of launch options with sandbox-safe Chromium args when on the platform."""
    out = dict(options or {})
    if not _is_at_home():
        return out
    args = list(out.get('args') or [])
    for arg in _SANDBOX_CHROME_ARGS:
        if arg not in args:
            args.append(arg)
    out['args'] = args
    return out


def _set_query_param(query: List[tuple], key: str, value: str) -> List[tuple]:
    """Replace ``key`` in a parsed query list in place (URLSearchParams.set semantics)."""
    out: List[tuple] = []
    inserted = False
    for k, v in query:
        if k == key:
            if not inserted:
                out.append((key, value))
                inserted = True
            # drop duplicates
        else:
            out.append((k, v))
    if not inserted:
        out.append((key, value))
    return out


_BYOP_IGNORED_WARNING = (
    '[scrapely] Custom proxies are ignored when using the Scrapely remote browser — it routes traffic '
    'through its own integrated proxy. Set the location with browser_configuration '
    '(Actor.create_browser_configuration(country_code=..., state=..., city=...)) or ProxyConfiguration '
    'country_code.'
)
_GROUPS_SESSION_INFO = (
    '[scrapely] Proxy groups and session IDs do not apply to the Scrapely remote browser; routing is '
    'location-based (proxy_country/proxy_state). Each browser connection is a fresh session.'
)

# Once-per-process messages (reset by tests via _reset_warnings()).
_warned: dict = {'byop': False, 'groups': False, 'subdivision': False}


def _reset_warnings() -> None:
    for key in _warned:
        _warned[key] = False


def _parse_proxy_url(proxy_url: str) -> Optional[SplitResult]:
    """Parse a proxy URL, accepting scheme-less ``host:port``."""
    for candidate in (proxy_url, f'http://{proxy_url}'):
        try:
            parts = urlsplit(candidate)
            if parts.hostname:
                return parts
        except ValueError:
            continue
    return None


def _platform_proxy_hostname() -> Optional[str]:
    """Platform gateway hostname injected by the worker (same vars as ProxyConfiguration)."""
    hostname = (os.environ.get('SCRAPELY_PROXY_HOSTNAME') or os.environ.get('APIFY_PROXY_HOSTNAME') or '').strip()
    return hostname.lower() or None


def _is_platform_proxy_url(proxy_url: str) -> bool:
    platform_host = _platform_proxy_hostname()
    if not platform_host:
        return False
    parts = _parse_proxy_url(proxy_url)
    return bool(parts and parts.hostname and parts.hostname.lower() == platform_host)


def _redact_proxy_url(proxy_url: str) -> str:
    """``scheme://***@host:port`` — never log proxy credentials."""
    parts = _parse_proxy_url(proxy_url)
    if not parts or not parts.hostname:
        return '<unparseable proxy URL>'
    try:
        port = f':{parts.port}' if parts.port else ''
    except ValueError:
        port = ''
    userinfo = '***@' if parts.username or parts.password else ''
    return f'{parts.scheme or "http"}://{userinfo}{parts.hostname}{port}'


_COUNTRY_FRAGMENT_RE = re.compile(r'^country-([A-Za-z]{2})(?:_([A-Za-z0-9]{1,3}))?$')


def _parse_proxy_url_location(proxy_url: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """``(country, subdivision)`` from the gateway grammar ``sub,groups-X,session-Y,country-US_CA``."""
    if not proxy_url:
        return None, None
    parts = _parse_proxy_url(str(proxy_url))
    if not parts or not parts.username:
        return None, None
    for fragment in unquote(parts.username).split(','):
        match = _COUNTRY_FRAGMENT_RE.match(fragment.strip())
        if match:
            subdivision = match.group(2)
            return match.group(1).upper(), subdivision.upper() if subdivision else None
    return None, None


def _resolve_launch_location(proxy_info: Any, *, log: bool = True) -> Tuple[Optional[str], Optional[str]]:
    """``(country, subdivision)`` requested by a platform-gateway proxy info; ``(None, None)`` for none or BYOP."""
    proxy_url = getattr(proxy_info, 'url', None) if proxy_info is not None else None
    if not proxy_url:
        return None, None

    if not _is_platform_proxy_url(str(proxy_url)):
        if log and not _warned['byop']:
            _warned['byop'] = True
            logger.warning('%s (ignored: %s)', _BYOP_IGNORED_WARNING, _redact_proxy_url(str(proxy_url)))
        return None, None

    if log and (getattr(proxy_info, 'groups', None) or getattr(proxy_info, 'session_id', None)):
        if not _warned['groups']:
            _warned['groups'] = True
            logger.info(_GROUPS_SESSION_INFO)
    grammar_country, grammar_subdivision = _parse_proxy_url_location(str(proxy_url))
    return (
        getattr(proxy_info, 'country_code', None) or grammar_country,
        getattr(proxy_info, 'subdivision_code', None) or grammar_subdivision,
    )


def _location_signature(proxy_info: Any, cdp_url: Optional[str]) -> str:
    """Comparable location of a connection, e.g. ``country=US,state=california``."""
    country, subdivision = _resolve_launch_location(proxy_info, log=False)
    if country:
        country = country.upper()
        state = subdivision_to_state_name(country, subdivision) if subdivision else None
    else:
        url_params = dict(parse_qsl(urlsplit(cdp_url).query)) if cdp_url else {}
        country = url_params.get('proxy_country')
        state = url_params.get('proxy_state')
    return f'country={country or ""},state={state or ""}'


def _validate_browser_configuration(browser_configuration: Any) -> None:
    if browser_configuration is not None and not callable(getattr(browser_configuration, 'to_cdp_params', None)):
        raise TypeError(
            'browser_configuration must be created with Actor.create_browser_configuration() '
            'or BrowserConfiguration().'
        )


def apply_proxy_info_to_cdp_url(cdp_url: str, proxy_info: Any, browser_configuration: Any = None) -> str:
    """Build the remote browser CDP URL for one browser connection.

    The Scrapely remote browser never takes a proxy: it routes through its own
    integrated proxy, selected by location params. Semantics (mirrors
    ``applyProxyInfoToCdpUrl`` in the Node SDK):

    - The worker-injected ``proxy_country`` is the run default; ``proxy_url``
      is never sent and is removed if present.
    - A platform-gateway ``proxy_info`` (crawler ProxyConfiguration)
      overrides ``proxy_country`` / ``proxy_state`` (ISO subdivision codes
      mapped to region names). A custom (BYOP) proxy is warned about once and
      ignored.
    - ``browser_configuration`` params always win.
    - When nothing changes, the CDP URL is returned byte-identical.
    """
    _validate_browser_configuration(browser_configuration)

    parts = urlsplit(cdp_url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    changed = False

    def set_param(key: str, value: str) -> None:
        nonlocal query, changed
        if dict(query).get(key) == value:
            return
        query = _set_query_param(query, key, value)
        changed = True

    if any(key == 'proxy_url' for key, _ in query):
        query = [(k, v) for k, v in query if k != 'proxy_url']
        changed = True

    country, subdivision = _resolve_launch_location(proxy_info)
    country = country.upper() if country else None
    state = None
    if subdivision:
        state = subdivision_to_state_name(country, subdivision)
        if not state and not _warned['subdivision']:
            _warned['subdivision'] = True
            logger.warning(
                '[scrapely] Subdivision "%s-%s" has no Scrapely browser mapping; routing by country only. '
                "Use browser_configuration state='<name>' to target a region.",
                country or '?',
                subdivision,
            )

    if country:
        set_param('proxy_country', country)
    if state:
        set_param('proxy_state', state)

    if browser_configuration is not None:
        for key, value in browser_configuration.to_cdp_params().items():
            set_param(key, value)

    if not changed:
        return cdp_url
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _apply_patch() -> None:
    """Monkey-patch Crawlee PlaywrightBrowserPlugin to connect over CDP."""
    try:
        from crawlee._utils.context import ensure_context
        from crawlee.browsers._browser_pool import BrowserPool
        from crawlee.browsers._playwright_browser_controller import PlaywrightBrowserController
        from crawlee.browsers._playwright_browser_plugin import PlaywrightBrowserPlugin
    except ImportError:
        # crawlee browsers / playwright extra not installed — skip
        return

    if getattr(PlaywrightBrowserPlugin, '_scrapely_cdp_patched', False):
        return

    # Stash the per-page proxy_info so the (proxy-less) new_browser() launch
    # can honor the crawler's ProxyConfiguration when building the CDP URL.
    _orig_get_new_page = BrowserPool._get_new_page

    async def _get_new_page_with_proxy(self: Any, page_id: Any, plugin: Any, proxy_info: Any) -> Any:
        token = _launch_proxy_info.set(proxy_info)
        config_token = _launch_browser_configuration.set(getattr(self, '_scrapely_browser_configuration', None))
        try:
            return await _orig_get_new_page(self, page_id, plugin, proxy_info)
        finally:
            _launch_browser_configuration.reset(config_token)
            _launch_proxy_info.reset(token)

    BrowserPool._get_new_page = _get_new_page_with_proxy  # type: ignore[method-assign]

    # Crawlee would turn proxy_info into per-context Playwright ProxySettings,
    # double-proxying on top of the service-level proxy selected through the
    # CDP URL — drop it while CDP mode is active. Before dropping, verify the
    # reused browser was launched for the location this page asked for: the
    # browser pool may reuse a connection pinned to a different (first-page)
    # location, which would silently route the page from the wrong region.
    _orig_controller_new_page = PlaywrightBrowserController.new_page

    async def _new_page_without_context_proxy(self: Any, browser_new_context_options: Any = None, proxy_info: Any = None) -> Any:
        cdp_url = get_cdp_url()
        if cdp_url and not _browser_patch_disabled():
            requested_info = _launch_proxy_info.get()
            launched = getattr(self, '_scrapely_location_signature', None)
            if requested_info is not None and getattr(requested_info, 'url', None) and launched is not None:
                requested = _location_signature(requested_info, cdp_url)
                if requested != launched and not getattr(self, '_scrapely_proxy_mismatch_warned', False):
                    self._scrapely_proxy_mismatch_warned = True
                    logger.warning(
                        'Browser location mismatch: a page requested "%s" but this browser '
                        'connection was launched with "%s" (the browser location is fixed at '
                        'connect time; the first page that triggered the launch pins it). The page '
                        'will use the launch location. Location rotation for browser crawlers works '
                        'per browser connection — pair one Session with one browser (as Crawlee '
                        'does) or accept the launch location.',
                        requested,
                        launched,
                    )
            proxy_info = None
        return await _orig_controller_new_page(self, browser_new_context_options, proxy_info)

    PlaywrightBrowserController.new_page = _new_page_without_context_proxy  # type: ignore[method-assign]

    _orig_new_browser = PlaywrightBrowserPlugin.new_browser

    @ensure_context
    async def new_browser(self: Any) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        browser_configuration = _launch_browser_configuration.get()
        if _browser_patch_disabled():
            _log_ignored_browser_configuration(browser_configuration)
            return await _orig_new_browser(self)

        cdp_url = get_cdp_url()
        if not cdp_url:
            if not _is_at_home():
                # Local development without CDP_URL: normal local browser launch.
                _log_ignored_browser_configuration(browser_configuration)
                return await _orig_new_browser(self)
            raise RuntimeError(
                'cdp_url is required. Local browser launch is disabled on the '
                'platform — all runs must use the remote browser.'
            )

        # The first page's proxy info / browser configuration pins the
        # connection's location params. The resolved location is tagged on the
        # controller so later page requests can detect (and loudly warn about)
        # a mismatch instead of silently reusing a connection pinned elsewhere.
        proxy_info = _launch_proxy_info.get()
        launch_signature = _location_signature(proxy_info, cdp_url)
        cdp_url = apply_proxy_info_to_cdp_url(cdp_url, proxy_info, browser_configuration)

        # Remote browser infrastructure is Chromium CDP (same as Node connectOverCDP).
        chromium = self._playwright.chromium
        wrap_connect_over_cdp(chromium, True, get_cdp_url)
        browser = await chromium.connect_over_cdp(cdp_url)

        controller = PlaywrightBrowserController(
            browser,
            use_incognito_pages=self._use_incognito_pages,
            max_open_pages_per_browser=self._max_open_pages_per_browser,
            fingerprint_generator=self._fingerprint_generator,
        )
        controller._scrapely_location_signature = launch_signature  # type: ignore[attr-defined]
        return controller

    PlaywrightBrowserPlugin.new_browser = new_browser  # type: ignore[method-assign]
    PlaywrightBrowserPlugin._scrapely_cdp_patched = True  # type: ignore[attr-defined]


def _log_ignored_browser_configuration(browser_configuration: Any) -> None:
    if browser_configuration is not None:
        logger.debug('browser_configuration ignored: browser launched locally (not the Scrapely remote browser).')


def _apply_crawler_patch() -> None:
    """Accept ``browser_configuration=`` on ``PlaywrightCrawler``.

    Always accepted (local runs and unpatched builds too) so actor code does not
    change between environments; it only takes effect on remote CDP launches.
    The configuration is stored on the crawler's BrowserPool and relayed to
    ``new_browser`` by the patched ``BrowserPool._get_new_page``.
    """
    try:
        # The defining module, not the ``crawlee.crawlers`` package: the
        # post-import hook runs this while that package is still initialising.
        from crawlee.crawlers._playwright._playwright_crawler import PlaywrightCrawler
    except ImportError:
        return

    if getattr(PlaywrightCrawler, '_scrapely_browser_config_patched', False):
        return

    _orig_init = PlaywrightCrawler.__init__

    @functools.wraps(_orig_init)
    def __init__(self: Any, *args: Any, browser_configuration: Any = None, **kwargs: Any) -> None:
        _validate_browser_configuration(browser_configuration)
        _orig_init(self, *args, **kwargs)
        if browser_configuration is not None:
            pool = getattr(self, '_browser_pool', None)
            if pool is not None:
                pool._scrapely_browser_configuration = browser_configuration

    PlaywrightCrawler.__init__ = __init__  # type: ignore[method-assign]
    PlaywrightCrawler._scrapely_browser_config_patched = True  # type: ignore[attr-defined]


def _apply_camoufox_patch() -> None:
    """Monkey-patch camoufox.AsyncNewBrowser to connect over CDP.

    CamoufoxPlugin (from crawlee's camoufox template) overrides
    ``PlaywrightBrowserPlugin.new_browser`` and calls
    ``AsyncNewBrowser(self._playwright, headless=True)`` directly, bypassing
    the PlaywrightBrowserPlugin patch above. By patching AsyncNewBrowser itself,
    we ensure that any Camoufox-based actor also connects via CDP — local
    launch is disabled, exactly like every other browser path.
    """
    try:
        import camoufox  # type: ignore[import-not-found]  # optional dependency
    except ImportError:
        # camoufox not installed — skip
        return

    if getattr(camoufox, '_scrapely_cdp_patched', False):
        return

    _orig_async_new_browser = camoufox.AsyncNewBrowser

    async def _patched_async_new_browser(playwright: Any, **kwargs: Any) -> Any:
        """Redirect Camoufox browser creation to CDP; local launch is disabled."""
        browser_configuration = _launch_browser_configuration.get()
        if _browser_patch_disabled():
            _log_ignored_browser_configuration(browser_configuration)
            return await _orig_async_new_browser(playwright, **kwargs)
        cdp_url = get_cdp_url()
        if not cdp_url:
            if not _is_at_home():
                # Local development without CDP_URL: normal local Camoufox launch.
                _log_ignored_browser_configuration(browser_configuration)
                return await _orig_async_new_browser(playwright, **kwargs)
            raise RuntimeError(
                'cdp_url is required. Local browser launch is disabled on the '
                'platform — all runs must use the remote browser.'
            )
        # Same as the PlaywrightBrowserPlugin patch above: the first page's
        # proxy info / browser configuration pins the connection's location.
        cdp_url = apply_proxy_info_to_cdp_url(cdp_url, _launch_proxy_info.get(), browser_configuration)
        chromium = playwright.chromium
        wrap_connect_over_cdp(chromium, True, get_cdp_url)
        return await chromium.connect_over_cdp(cdp_url)

    camoufox.AsyncNewBrowser = _patched_async_new_browser
    camoufox._scrapely_cdp_patched = True


# ─── Raw Playwright API → remote CDP ─────────────────────────────────────────
#
# Actors that use Playwright directly (no Crawlee BrowserPool, no Camoufox)
# would otherwise launch a local browser inside the worker container, silently
# bypassing the platform's remote browser (antibot patching, proxying, session
# pairing). The pool patch above cannot see those launches, so the
# sync_playwright()/async_playwright() entry points are wrapped and every
# BrowserType on the returned Playwright object gets launch() and
# launch_persistent_context() redirected to chromium.connect_over_cdp(CDP_URL).

class _ProxyUrlInfo:
    """Minimal ProxyInfo stand-in for apply_proxy_info_to_cdp_url()."""

    __slots__ = ('url',)

    def __init__(self, url: str) -> None:
        self.url = url


def _compose_launch_proxy_url(proxy: Any) -> Optional[str]:
    """Compose a full proxy URL from Playwright's launch ``proxy`` mapping."""
    if isinstance(proxy, dict):
        server = proxy.get('server')
        username = proxy.get('username')
        password = proxy.get('password')
    else:
        server = getattr(proxy, 'server', None)
        username = getattr(proxy, 'username', None)
        password = getattr(proxy, 'password', None)
    if not server:
        return None
    if username:
        parts = urlsplit(str(server))
        host = parts.hostname or ''
        port = f':{parts.port}' if parts.port else ''
        netloc = f'{username}:{password or ""}@{host}{port}'
        return urlunsplit((parts.scheme or 'http', netloc, parts.path, parts.query, parts.fragment))
    return str(server)


def _raw_launch_cdp_url(kwargs: dict, browser_configuration: Any = None) -> Optional[str]:
    """CDP URL for a raw launch (a launch ``proxy`` option is warned about and ignored)."""
    cdp_url = get_cdp_url()
    if not cdp_url:
        return None
    proxy_url = _compose_launch_proxy_url(kwargs.get('proxy'))
    return apply_proxy_info_to_cdp_url(
        cdp_url, _ProxyUrlInfo(proxy_url) if proxy_url else None, browser_configuration
    )


def _take_browser_configuration(kwargs: dict) -> Any:
    """Pop ``browser_configuration`` so it never reaches a real (local) launch."""
    browser_configuration = kwargs.pop('browser_configuration', None)
    _validate_browser_configuration(browser_configuration)
    if browser_configuration is not None and (_browser_patch_disabled() or not get_cdp_url()):
        _log_ignored_browser_configuration(browser_configuration)
    return browser_configuration


def _raw_connect_kwargs(kwargs: dict) -> dict:
    """Forward connection-level options playwright's CDP connect understands."""
    out: dict = {}
    if 'timeout' in kwargs:
        out['timeout'] = kwargs['timeout']
    if 'slow_mo' in kwargs:
        out['slow_mo'] = kwargs['slow_mo']
    return out


def _warn_dropped_launch_options(kind: str, browser_name: str, kwargs: dict) -> None:
    dropped = sorted(k for k in kwargs if k not in ('timeout', 'slow_mo'))
    if dropped:
        logger.warning(
            'playwright %s.%s redirected to the remote CDP browser; local launch options '
            'ignored: %s (the remote browser manages headless mode, binary and flags).',
            browser_name,
            kind,
            ', '.join(dropped),
        )


# Options that only make sense for a local launch and have no remote-CDP
# equivalent. Everything else is forwarded to browser.new_context() for
# launch_persistent_context() calls (viewport, locale, user_agent, ...).
_RAW_LAUNCH_ONLY_KEYS = frozenset({
    'user_data_dir',
    'headless',
    'channel',
    'args',
    'ignore_args',
    'ignore_default_args',
    'executable_path',
    'devtools',
    'env',
    'chromium_sandbox',
    'firefox_user_prefs',
    'proxy',
    'traces_dir',
})

_RAW_LAUNCH_DISABLED_MSG = (
    'cdp_url is required. Local browser launch is disabled on the '
    'platform — all runs must use the remote browser.'
)


def _persistent_context_kwargs(kwargs: dict) -> dict:
    """Context options forwarded to browser.new_context() after a redirect."""
    return {
        k: v
        for k, v in kwargs.items()
        if k not in _RAW_LAUNCH_ONLY_KEYS and k not in ('timeout', 'slow_mo')
    }


def _warn_persistent_context_redirect(name: str, kwargs: dict) -> None:
    if kwargs.get('user_data_dir'):
        logger.warning(
            'playwright %s.launch_persistent_context(user_data_dir=%r) redirected to the '
            'remote CDP browser — persistent profiles have no remote equivalent; a fresh '
            'context is returned and user_data_dir is ignored.',
            name,
            kwargs.get('user_data_dir'),
        )
    _warn_dropped_launch_options('launch_persistent_context', name, kwargs)


def _patch_raw_browser_type(browser_type: Any, chromium: Any, name: str, is_async: bool) -> None:
    """Redirect a BrowserType's launch()/launch_persistent_context() to CDP."""
    if getattr(browser_type, '_scrapely_raw_cdp_patched', False):
        return

    if is_async:
        orig_launch = browser_type.launch
        orig_lpc = browser_type.launch_persistent_context

        async def launch(**kwargs: Any) -> Any:
            browser_configuration = _take_browser_configuration(kwargs)
            if _browser_patch_disabled():
                return await orig_launch(**_with_sandbox_args(kwargs))
            cdp_url = _raw_launch_cdp_url(kwargs, browser_configuration)
            if not cdp_url:
                if not _is_at_home():
                    return await orig_launch(**kwargs)
                raise RuntimeError(_RAW_LAUNCH_DISABLED_MSG)
            _warn_dropped_launch_options('launch', name, kwargs)
            return await chromium.connect_over_cdp(cdp_url, **_raw_connect_kwargs(kwargs))

        # Playwright: launch_persistent_context(user_data_dir, **options)
        async def launch_persistent_context(*args: Any, **kwargs: Any) -> Any:
            browser_configuration = _take_browser_configuration(kwargs)
            if _browser_patch_disabled():
                return await orig_lpc(*args, **_with_sandbox_args(kwargs))
            cdp_url = _raw_launch_cdp_url(kwargs, browser_configuration)
            if not cdp_url:
                if not _is_at_home():
                    return await orig_lpc(*args, **kwargs)
                raise RuntimeError(_RAW_LAUNCH_DISABLED_MSG)
            _warn_persistent_context_redirect(name, kwargs)
            browser = await chromium.connect_over_cdp(cdp_url, **_raw_connect_kwargs(kwargs))
            return await browser.new_context(**_persistent_context_kwargs(kwargs))

        browser_type.launch = launch
        browser_type.launch_persistent_context = launch_persistent_context
    else:
        orig_launch = browser_type.launch
        orig_lpc = browser_type.launch_persistent_context

        def launch(**kwargs: Any) -> Any:
            browser_configuration = _take_browser_configuration(kwargs)
            if _browser_patch_disabled():
                return orig_launch(**_with_sandbox_args(kwargs))
            cdp_url = _raw_launch_cdp_url(kwargs, browser_configuration)
            if not cdp_url:
                if not _is_at_home():
                    return orig_launch(**kwargs)
                raise RuntimeError(_RAW_LAUNCH_DISABLED_MSG)
            _warn_dropped_launch_options('launch', name, kwargs)
            return chromium.connect_over_cdp(cdp_url, **_raw_connect_kwargs(kwargs))

        def launch_persistent_context(*args: Any, **kwargs: Any) -> Any:
            browser_configuration = _take_browser_configuration(kwargs)
            if _browser_patch_disabled():
                return orig_lpc(*args, **_with_sandbox_args(kwargs))
            cdp_url = _raw_launch_cdp_url(kwargs, browser_configuration)
            if not cdp_url:
                if not _is_at_home():
                    return orig_lpc(*args, **kwargs)
                raise RuntimeError(_RAW_LAUNCH_DISABLED_MSG)
            _warn_persistent_context_redirect(name, kwargs)
            browser = chromium.connect_over_cdp(cdp_url, **_raw_connect_kwargs(kwargs))
            return browser.new_context(**_persistent_context_kwargs(kwargs))

        browser_type.launch = launch
        browser_type.launch_persistent_context = launch_persistent_context

def _patch_playwright_object(playwright: Any, is_async: bool) -> Any:
    """Patch every BrowserType on a Playwright driver object for CDP redirect."""
    chromium = getattr(playwright, 'chromium', None)
    if chromium is None or getattr(chromium, 'connect_over_cdp', None) is None:
        return playwright
    # Retry transient connect failures (429/5xx/network) for the platform CDP
    # host: covers direct chromium.connect_over_cdp(CDP_URL) calls in actor
    # code and the redirected launches below, which call this same attribute.
    wrap_connect_over_cdp(chromium, is_async, get_cdp_url)
    for name in ('chromium', 'firefox', 'webkit'):
        browser_type = getattr(playwright, name, None)
        if browser_type is None:
            continue
        _patch_raw_browser_type(browser_type, chromium, name, is_async)
    return playwright


class _SyncPlaywrightContextManagerProxy:
    """Wraps sync_playwright()'s context manager; patches the driver on start/enter."""

    def __init__(self, cm: Any) -> None:
        self._cm = cm

    def start(self) -> Any:
        return _patch_playwright_object(self._cm.start(), is_async=False)

    def __enter__(self) -> Any:
        return _patch_playwright_object(self._cm.__enter__(), is_async=False)

    def __exit__(self, *exc: Any) -> Any:
        return self._cm.__exit__(*exc)


class _AsyncPlaywrightContextManagerProxy:
    """Wraps async_playwright()'s context manager; patches the driver on start/enter."""

    def __init__(self, cm: Any) -> None:
        self._cm = cm

    async def start(self) -> Any:
        return _patch_playwright_object(await self._cm.start(), is_async=True)

    async def __aenter__(self) -> Any:
        return _patch_playwright_object(await self._cm.__aenter__(), is_async=True)

    async def __aexit__(self, *exc: Any) -> Any:
        return await self._cm.__aexit__(*exc)


def _apply_raw_playwright_patch() -> None:
    """Wrap playwright's sync/async entry points to redirect raw launches to CDP."""
    try:
        import playwright.async_api as playwright_async_api
    except ImportError:
        playwright_async_api = None  # type: ignore[assignment]

    if playwright_async_api is not None and not getattr(
        playwright_async_api, '_scrapely_raw_cdp_patched', False
    ):
        orig_async = playwright_async_api.async_playwright

        def async_playwright_patched() -> Any:
            return _AsyncPlaywrightContextManagerProxy(orig_async())

        playwright_async_api.async_playwright = async_playwright_patched
        playwright_async_api._scrapely_raw_cdp_patched = True  # type: ignore[attr-defined]

    try:
        import playwright.sync_api as playwright_sync_api
    except ImportError:
        playwright_sync_api = None  # type: ignore[assignment]

    if playwright_sync_api is not None and not getattr(
        playwright_sync_api, '_scrapely_raw_cdp_patched', False
    ):
        orig_sync = playwright_sync_api.sync_playwright

        def sync_playwright_patched() -> Any:
            return _SyncPlaywrightContextManagerProxy(orig_sync())

        playwright_sync_api.sync_playwright = sync_playwright_patched
        playwright_sync_api._scrapely_raw_cdp_patched = True  # type: ignore[attr-defined]


def _apply_pyppeteer_patch() -> None:
    """Redirect raw pyppeteer launches to the remote CDP browser.

    pyppeteer (the Python puppeteer port) ships both ``launch`` (spawns a local
    Chromium) and ``connect(browserWSEndpoint=...)`` — the same launcher family
    as puppeteer, so it qualifies as CDP-attachable. Both the package-level
    ``pyppeteer.launch`` and ``pyppeteer.launcher.launch`` bindings are patched
    (the package re-exports the function, so patching one module attribute does
    not fix the other binding). Semantics match the raw playwright patch.
    """
    try:
        import pyppeteer as pyppeteer_mod
        import pyppeteer.launcher as pyppeteer_launcher
    except ImportError:
        # pyppeteer not installed — skip
        return

    if getattr(pyppeteer_launcher, '_scrapely_cdp_patched', False):
        return

    _orig_launch = pyppeteer_launcher.launch

    async def _patched_launch(options=None, **kwargs):
        browser_configuration = _take_browser_configuration(kwargs)
        if isinstance(options, dict) and 'browser_configuration' in options:
            options = dict(options)
            browser_configuration = _take_browser_configuration(options) or browser_configuration
        if _browser_patch_disabled():
            return await _orig_launch(_with_sandbox_args({**(options or {}), **kwargs}))
        cdp_url = get_cdp_url()
        if not cdp_url:
            if not _is_at_home():
                return await _orig_launch(options, **kwargs)
            raise RuntimeError(_RAW_LAUNCH_DISABLED_MSG)
        cdp_url = apply_proxy_info_to_cdp_url(cdp_url, None, browser_configuration)

        merged = dict(options or {})
        merged.update(kwargs)
        if merged:
            logger.warning(
                'pyppeteer.launch redirected to the remote CDP browser; local launch options '
                'ignored: %s (the remote browser manages headless mode, binary and flags).',
                ', '.join(sorted(merged.keys())),
            )
        return await connect_with_retry_async(
            lambda: pyppeteer_launcher.connect(browserWSEndpoint=cdp_url)
        )

    pyppeteer_launcher.launch = _patched_launch
    if hasattr(pyppeteer_mod, 'launch'):
        pyppeteer_mod.launch = _patched_launch
    pyppeteer_launcher._scrapely_cdp_patched = True
    pyppeteer_mod._scrapely_cdp_patched = True


# ─── Crawlee patches without importing Crawlee ───────────────────────────────
#
# Importing Crawlee costs ~1 s per run on the platform, so this module never
# imports it. The Crawlee patches mutate classes in place, so they only have to
# run before actor code uses those classes: they are applied immediately when
# the defining modules are already loaded, otherwise by a post-import hook the
# moment they finish executing (``import crawlee.browsers``,
# ``from crawlee.crawlers import PlaywrightCrawler``, a PlaywrightCrawler built
# by any template...). Behaviour is the same as applying them eagerly.

# Module name -> patch functions to run once that module has finished loading.
# ``crawlee.browsers`` imports BrowserPool, PlaywrightBrowserController and
# PlaywrightBrowserPlugin in its ``__init__``; any import of one of those
# submodules runs that ``__init__`` to completion first.
_CRAWLEE_POST_IMPORT_PATCHES: dict = {
    'crawlee.browsers': (_apply_patch,),
    'crawlee.crawlers._playwright._playwright_crawler': (_apply_crawler_patch,),
}


class _PostImportLoader(importlib.abc.Loader):
    """Delegating loader that runs the Scrapely patches after the module executes."""

    def __init__(self, loader: Any, patches: tuple) -> None:
        self._loader = loader
        self._patches = patches

    def create_module(self, spec: Any) -> Any:
        return self._loader.create_module(spec)

    def exec_module(self, module: Any) -> None:
        # Leave the module looking untouched (resource readers, reload, ...).
        spec = getattr(module, '__spec__', None)
        if spec is not None:
            spec.loader = self._loader
        try:
            module.__loader__ = self._loader
        except Exception:
            pass
        self._loader.exec_module(module)
        for patch in self._patches:
            patch()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._loader, name)


class _CrawleePostImportFinder(importlib.abc.MetaPathFinder):
    """Meta path finder that hooks the Crawlee modules in ``_CRAWLEE_POST_IMPORT_PATCHES``."""

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> Any:
        patches = _CRAWLEE_POST_IMPORT_PATCHES.get(fullname)
        if patches is None:
            return None
        for finder in sys.meta_path:
            if finder is self or isinstance(finder, _CrawleePostImportFinder):
                continue
            find_spec = getattr(finder, 'find_spec', None)
            if find_spec is None:
                continue
            spec = find_spec(fullname, path, target)
            if spec is not None:
                break
        else:
            return None
        if spec.loader is None or not hasattr(spec.loader, 'exec_module'):
            return spec
        # One-shot: the patches are idempotent, but a module loads only once.
        _CRAWLEE_POST_IMPORT_PATCHES.pop(fullname, None)
        spec.loader = _PostImportLoader(spec.loader, patches)
        return spec


def _install_crawlee_patches() -> None:
    """Apply the Crawlee patches now if Crawlee is loaded, else when it loads."""
    for module_name in list(_CRAWLEE_POST_IMPORT_PATCHES):
        if module_name in sys.modules:
            for patch in _CRAWLEE_POST_IMPORT_PATCHES.pop(module_name):
                patch()
    if _CRAWLEE_POST_IMPORT_PATCHES and not any(
        isinstance(finder, _CrawleePostImportFinder) for finder in sys.meta_path
    ):
        sys.meta_path.insert(0, _CrawleePostImportFinder())


_install_crawlee_patches()
_apply_camoufox_patch()
_apply_raw_playwright_patch()
_apply_pyppeteer_patch()