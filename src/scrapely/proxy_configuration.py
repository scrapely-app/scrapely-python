"""
Proxy configuration for Scrapely Actors.

Configures connection to proxy servers to prevent target websites
from blocking your crawlers based on IP address rate limits or blacklists.

Three modes are supported (JS SDK parity):

- Platform proxy: when the Scrapely server injects SCRAPELY_PROXY_*/APIFY_PROXY_*
  credentials (Evomi-backed), groups/country_code/password build
  gateway usernames automatically.
- BYOP: custom proxy_urls / new_url_function / tiered proxy lists win over the
  platform proxy.
- Mixed: the 'auto' (or 'http://auto') sentinel inside proxy_urls / tiered
  lists reserves a slot for the platform proxy alongside BYOP entries.
"""

import asyncio
import json
import logging
import os
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, cast

from .types import ProxyInfo

if TYPE_CHECKING:
    from .configuration import Configuration
    from .storage.api_client import ApiClient

logger = logging.getLogger(__name__)

# Session IDs are at most 50 chars of [0-9a-zA-Z._~].
MAX_SESSION_ID_LENGTH = 50
SESSION_ID_REGEX = re.compile(r'^[a-zA-Z0-9._~]+$')

# Two-letter ISO country codes; ISO 3166-2 subdivision codes
# are 1-3 uppercase alphanumeric characters (e.g. 'CA', 'NSW', '9').
COUNTRY_CODE_REGEX = re.compile(r'^[A-Z]{2}$')
SUBDIVISION_CODE_REGEX = re.compile(r'^[A-Z0-9]{1,3}$')

# Sentinel values reserving a platform-proxy slot inside proxy URL lists.
PLATFORM_SENTINELS = frozenset({'auto', 'http://auto'})

# Status-check constants (4s timeout, 2 attempts).
CHECK_ACCESS_REQUEST_TIMEOUT_MILLIS = 4000
CHECK_ACCESS_MAX_ATTEMPTS = 2

# Gateway upstream error codes (590-599, proxy-chain semantics). See docs/evomi-integration.md for the contract.
PROXY_STATUS_CODE_DESCRIPTIONS = {
    590: 'Non Successful: upstream responded with a non-200 status code',
    591: 'reserved for further use',
    592: 'Status Code Out Of Range: upstream responded with status code different than 100-999',
    593: 'Not Found: DNS lookup failed (EAI_NODATA or EAI_NONAME)',
    594: 'Connection Refused: upstream refused connection',
    595: 'Connection Reset: connection reset due to loss of connection or timeout',
    596: 'Broken Pipe: trying to write on a closed socket',
    597: 'Auth Failed: incorrect upstream credentials',
    598: 'reserved for further use',
    599: 'Upstream Error: generic upstream error',
}


def is_platform_sentinel(url: Optional[str]) -> bool:
    """True for platform-proxy sentinel values ('auto', 'http://auto')."""
    return url is not None and url in PLATFORM_SENTINELS


def _validate_session_id(session_id: str) -> None:
    """Validate a proxy session ID."""
    if len(session_id) > MAX_SESSION_ID_LENGTH:
        raise ValueError(f'Session ID must not be longer than {MAX_SESSION_ID_LENGTH} characters.')
    if not SESSION_ID_REGEX.match(session_id):
        raise ValueError(
            f'Session ID "{session_id}" contains unsupported characters. '
            'Only "a-z", "A-Z", "0-9", ".", "_" and "~" are allowed.'
        )


def _validate_country_options(country_code: Optional[str], subdivision_code: Optional[str]) -> None:
    """Validate country/subdivision options.

    Country must be a two-letter ISO code; subdivision an ISO 3166-2 code
    (1-3 uppercase alphanumeric characters) and requires a country.
    """
    if country_code is not None and not COUNTRY_CODE_REGEX.match(country_code):
        raise ValueError(
            f'ProxyConfiguration: "country_code" must be a two-letter ISO country code (got "{country_code}").'
        )
    if subdivision_code is not None:
        if not SUBDIVISION_CODE_REGEX.match(subdivision_code):
            raise ValueError(
                f'ProxyConfiguration: "subdivision_code" must be a 1-3 character ISO 3166-2 '
                f'subdivision code (got "{subdivision_code}").'
            )
        if not country_code:
            raise ValueError('ProxyConfiguration: "subdivision_code" requires "country_code" to be set.')


def _sanitize_fragment(value: str) -> str:
    """Strip URL-breaking characters from a username fragment.

    The gateway grammar is comma-based, so fragments themselves must never
    contain ',@:/?#' or whitespace (the username is NOT
    percent-encoded).
    """
    return re.sub(r'[:@/?#\s]', '', value)


def _input_use_platform_proxy(actor_proxy_input: Dict[str, Any]) -> Optional[bool]:
    """The proxy input field's platform-proxy switch, or None when it has none.

    `useScrapelyProxy` wins when it is a boolean; otherwise the legacy
    `useApifyProxy` key of existing inputs decides.
    """
    for key in ('useScrapelyProxy', 'useApifyProxy'):
        value = actor_proxy_input.get(key)
        if isinstance(value, bool):
            return value
    return None


def _input_proxy_option(actor_proxy_input: Dict[str, Any], name: str) -> Any:
    """`scrapelyProxy<name>` of the proxy input field when it is set, else the legacy `apifyProxy<name>`."""
    value = actor_proxy_input.get(f'scrapelyProxy{name}')
    return value if value is not None else actor_proxy_input.get(f'apifyProxy{name}')


_PROXY_OPTION_NAMES = ('Groups', 'Country', 'Subdivision')


def _is_proxy_input_object(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    keys = ['useScrapelyProxy', 'useApifyProxy']
    for name in _PROXY_OPTION_NAMES:
        keys += [f'scrapelyProxy{name}', f'apifyProxy{name}']
    return any(key in value for key in keys)


def with_proxy_key_aliases(actor_input: Any) -> Any:
    """Actor input with every proxy option under both its Scrapely and legacy name.

    For each top-level object that has a proxy key of either naming
    (``useScrapelyProxy`` / ``useApifyProxy``, ``scrapelyProxyGroups`` /
    ``apifyProxyGroups``, ``...Country``, ``...Subdivision``), every set value
    is written under both names, so code that reads either naming sees the
    user's choice. A boolean ``useScrapelyProxy`` and a set ``scrapelyProxy*``
    value win over their legacy counterparts. Nothing is removed: keys with
    unset or non-boolean values stay as sent. Returns ``actor_input`` itself
    when nothing changes; never mutates it.
    """
    if not isinstance(actor_input, dict):
        return actor_input
    result: Optional[Dict[str, Any]] = None
    for key, value in actor_input.items():
        if not _is_proxy_input_object(value):
            continue
        aliased = dict(value)
        use = _input_use_platform_proxy(value)
        if use is not None:
            aliased['useScrapelyProxy'] = use
            aliased['useApifyProxy'] = use
        for name in _PROXY_OPTION_NAMES:
            option = _input_proxy_option(value, name)
            if option is not None:
                aliased[f'scrapelyProxy{name}'] = option
                aliased[f'apifyProxy{name}'] = option
        if aliased != value:
            if result is None:
                result = dict(actor_input)
            result[key] = aliased
    return result if result is not None else actor_input


def _platform_env() -> Optional[Dict[str, Any]]:
    """Read platform proxy credentials from the environment (or None)."""
    hostname = os.environ.get('SCRAPELY_PROXY_HOSTNAME') or os.environ.get('APIFY_PROXY_HOSTNAME')
    password = os.environ.get('SCRAPELY_PROXY_PASSWORD') or os.environ.get('APIFY_PROXY_PASSWORD')
    if not hostname or not password:
        return None
    port_str = os.environ.get('SCRAPELY_PROXY_PORT') or os.environ.get('APIFY_PROXY_PORT')
    try:
        port = int(port_str) if port_str else 8000
        if not 0 < port < 65536:
            raise ValueError
    except ValueError:
        port = 8000
    return {
        'hostname': hostname,
        'port': port,
        'password': password,
        'username': os.environ.get('SCRAPELY_PROXY_USERNAME') or os.environ.get('APIFY_PROXY_USERNAME'),
        'status_url': os.environ.get('SCRAPELY_PROXY_STATUS_URL') or os.environ.get('APIFY_PROXY_STATUS_URL'),
    }


def _platform_missing_error() -> str:
    return (
        'The platform proxy was requested (via the "auto" sentinel, tiered proxies without '
        'proxy URLs, or by relying on the default platform configuration), but no platform '
        'proxy credentials (SCRAPELY_PROXY_*/APIFY_PROXY_*) were injected into this '
        'environment. Run the actor on Scrapely with proxy access enabled, or remove the '
        'platform proxy entries. To use the platform proxy locally or from outside '
        'Scrapely, copy your proxy credentials from the Scrapely dashboard (Proxy page) '
        'into the SCRAPELY_PROXY_HOSTNAME, SCRAPELY_PROXY_PORT, SCRAPELY_PROXY_USERNAME '
        'and SCRAPELY_PROXY_PASSWORD environment variables (or pass password=... '
        'explicitly). External platform proxy access requires an active plan or a '
        'prepaid (PAYG) balance of at least $1; free accounts get run-scoped '
        'credentials injected into their on-platform runs only.'
    )


class ProxyConfiguration:
    """Configuration for connecting to proxy servers.
    
    Proxy servers are used to prevent target websites from blocking your
    crawlers based on IP address rate limits or blacklists.
    
    Usage:
        proxy_config = await Actor.create_proxy_configuration()
        proxy_url = await proxy_config.new_url()
    """
    
    def __init__(
        self,
        groups: Optional[List[str]] = None,
        password: Optional[str] = None,
        country_code: Optional[str] = None,
        subdivision_code: Optional[str] = None,
        proxy_urls: Optional[List[str]] = None,
        new_url_function: Optional[Callable[..., Any]] = None,
        tiered_proxy_urls: Optional[List[List[Optional[str]]]] = None,
        tiered_proxy_config: Optional[List[Dict[str, Any]]] = None,
        actor_proxy_input: Optional[Dict] = None,
        _actor_config: Optional['Configuration'] = None,
        _api_client: Optional['ApiClient'] = None,
    ):
        """Initialize the proxy configuration.

        Args:
            groups: Proxy groups to use
            password: Proxy password (defaults to environment)
            country_code: Country code for proxy location (two-letter ISO code)
            subdivision_code: ISO 3166-2 subdivision code (e.g., 'CA' for
                California with country_code='US'); requires country_code
            proxy_urls: Custom proxy URLs to rotate through; may contain the
                'auto' / 'http://auto' sentinel to mix in the platform proxy
            new_url_function: Function that returns a new proxy URL; when it
                accepts a positional argument it receives the session ID
                zero-argument callables are also supported
            tiered_proxy_urls: Proxy URL lists stratified into tiers; lists may
                contain the 'auto' sentinel (and None for no-proxy tiers)
            tiered_proxy_config: Declarative tiers (proxy_urls / groups /
                country_code / password per tier); tiers without proxy_urls
                become lazy platform slots resolved per new_url() call with
                the tier's options and the session ID
            actor_proxy_input: Proxy configuration from Actor input
            _actor_config: Internal - Actor configuration
            _api_client: Internal - API client
        """
        self._groups = groups or []
        self._password = password
        self._country_code = country_code
        self._subdivision_code = subdivision_code
        self._proxy_urls = proxy_urls or []
        self._new_url_function = new_url_function
        self._actor_config = _actor_config
        self._api_client = _api_client
        self._initialized = False
        self._is_man_in_the_middle = False

        # Rotation / stickiness state (sessions are keyed by (session_id, tier)
        # so a pinned session never leaks across proxy tiers — Crawlee parity).
        self._url_index = 0
        self._session_urls: Dict[Tuple[str, int], str] = {}
        self._current_tier = 0

        # If actor_proxy_input is provided, extract settings from it
        if actor_proxy_input:
            if _input_use_platform_proxy(actor_proxy_input):
                self._groups = _input_proxy_option(actor_proxy_input, 'Groups') or []
                self._country_code = _input_proxy_option(actor_proxy_input, 'Country')
                self._subdivision_code = _input_proxy_option(actor_proxy_input, 'Subdivision')
            else:
                self._proxy_urls = actor_proxy_input.get('proxyUrls', []) or []
                # Tiered lists may also arrive through actor input (future-proof
                # for a tiered console UI / hand-written input JSON); explicit
                # constructor arguments win over the input dict.
                tiered_proxy_urls = tiered_proxy_urls or actor_proxy_input.get('tieredProxyUrls')
                raw_tiered_config = actor_proxy_input.get('tieredProxyConfig')
                if raw_tiered_config and not tiered_proxy_config:
                    # Actor input is camelCase; normalize tier
                    # keys to the snake_case the expansion logic reads.
                    tiered_proxy_config = [
                        {
                            'proxy_urls': t.get('proxyUrls', t.get('proxy_urls')),
                            'groups': t.get('groups'),
                            'country_code': t.get('countryCode', t.get('country_code')),
                            'subdivision_code': t.get('subdivisionCode', t.get('subdivision_code')),
                            'password': t.get('password'),
                            'new_url_function': t.get('newUrlFunction', t.get('new_url_function')),
                        }
                        for t in raw_tiered_config
                    ]

        # Validation: bad country/subdivision values throw at
        # construction time, not on the first new_url() call.
        _validate_country_options(self._country_code, self._subdivision_code)

        # Declarative tiers expand to tiered URL lists. Sentinel tiers keep a
        # lazy platform slot resolved with the tier's options; tiers without
        # proxy_urls ALSO keep a lazy 'auto' slot so new_url(session_id) builds
        # the platform URL with the tier's own options AND the session ID —
        # building it eagerly here would freeze it without a session fragment
        # and lose session stickiness (parity with the JS SDK's lazy fix).
        self._tier_options: Dict[int, Dict[str, Any]] = {}
        if tiered_proxy_config:
            expanded: List[List[Optional[str]]] = []
            env = _platform_env()
            for tier in tiered_proxy_config:
                if tier.get('new_url_function'):
                    raise ValueError(
                        'tiered_proxy_config tiers with new_url_function are not supported. '
                        'Use tiered_proxy_urls instead.'
                    )
                tier_urls = tier.get('proxy_urls')
                if tier_urls:
                    if any(is_platform_sentinel(u) for u in tier_urls):
                        if not env:
                            raise ValueError(_platform_missing_error())
                        self._tier_options[len(expanded)] = dict(tier)
                    expanded.append(list(tier_urls))
                else:
                    if not env:
                        raise ValueError(_platform_missing_error())
                    self._tier_options[len(expanded)] = dict(tier)
                    expanded.append(['auto'])
            self._tiered_proxy_urls = expanded
        else:
            self._tiered_proxy_urls = tiered_proxy_urls or []

        self._tier_url_indexes = [0] * len(self._tiered_proxy_urls)

        # Validation: real (non-sentinel) custom proxies may not
        # combine with platform-only options. Sentinel-only lists are platform
        # slots and combine legally with groups/country_code.
        has_real_custom = (
            any(u is not None and not is_platform_sentinel(u) for u in self._proxy_urls)
            or any(
                u is not None and not is_platform_sentinel(u)
                for tier in self._tiered_proxy_urls
                for u in tier
            )
            or new_url_function is not None
        )
        if has_real_custom and (self._groups or self._country_code or self._subdivision_code or password):
            raise ValueError(
                'Cannot combine custom proxies with Scrapely Proxy! It is not allowed to set '
                '"proxy_urls" or "new_url_function" combined with "groups", "country_code" '
                'or "subdivision_code".'
            )

        needs_platform = (
            not self._proxy_urls
            and not self._tiered_proxy_urls
            and new_url_function is None
        ) or (
            any(is_platform_sentinel(u) for u in self._proxy_urls)
            or any(
                is_platform_sentinel(u)
                for tier in self._tiered_proxy_urls
                for u in tier
            )
        )
        self._needs_platform = needs_platform
        if needs_platform and not _platform_env():
            raise ValueError(_platform_missing_error())

    @classmethod
    async def create(
        cls,
        groups: Optional[List[str]] = None,
        password: Optional[str] = None,
        country_code: Optional[str] = None,
        subdivision_code: Optional[str] = None,
        proxy_urls: Optional[List[str]] = None,
        new_url_function: Optional[Callable[..., Any]] = None,
        tiered_proxy_urls: Optional[List[List[Optional[str]]]] = None,
        tiered_proxy_config: Optional[List[Dict[str, Any]]] = None,
        actor_proxy_input: Optional[Dict] = None,
    ) -> Optional['ProxyConfiguration']:
        """Create a ProxyConfiguration instance.

        Args:
            groups: Proxy groups to use
            password: Proxy password
            country_code: Country code for proxy location
            subdivision_code: Subdivision code
            proxy_urls: Custom proxy URLs (may contain the 'auto' sentinel)
            new_url_function: Function returning new proxy URL; when it
                accepts a positional argument it receives the session ID
            tiered_proxy_urls: Proxy URL lists stratified into tiers
            tiered_proxy_config: Declarative tiers (proxy_urls / groups / ...)
            actor_proxy_input: Proxy configuration from Actor input

        Returns:
            ProxyConfiguration instance or None if no proxy should be used
        """
        # Platform proxy password: SCRAPELY_* env (injected by the Scrapely
        # server, Evomi-backed) takes precedence over the APIFY_* alias.
        if password is None:
            password = (
                os.environ.get('SCRAPELY_PROXY_PASSWORD')
                or os.environ.get('APIFY_PROXY_PASSWORD')
            )

        # Check if proxy should be used
        if (
            not groups and not proxy_urls and not new_url_function and not actor_proxy_input
            and not tiered_proxy_urls and not tiered_proxy_config
        ):
            # Nothing explicitly configured — fall back to the platform proxy
            # only when the server injected credentials into the environment.
            if not password:
                # JS SDK parity: warn (like createProxyConfiguration's
                # BYOP_WARNING) instead of returning None silently, so local
                # runs make it obvious why no proxy is being used.
                logger.warning(
                    'The Scrapely platform proxy is only available in on-platform runs when your '
                    'account has proxy access. Locally, either add proxy_urls to your actor input, '
                    'or copy your proxy credentials from the Scrapely dashboard (Proxy page) into the '
                    'SCRAPELY_PROXY_HOSTNAME, SCRAPELY_PROXY_PORT, SCRAPELY_PROXY_USERNAME and '
                    'SCRAPELY_PROXY_PASSWORD environment variables. Using the platform proxy from '
                    'outside Scrapely requires an active plan or a prepaid (PAYG) balance of at '
                    'least $1; free accounts get run-scoped credentials injected into their '
                    'on-platform runs only. No proxy configuration was created.'
                )
                return None

        return cls(
            groups=groups,
            password=password,
            country_code=country_code,
            subdivision_code=subdivision_code,
            proxy_urls=proxy_urls,
            new_url_function=new_url_function,
            tiered_proxy_urls=tiered_proxy_urls,
            tiered_proxy_config=tiered_proxy_config,
            actor_proxy_input=actor_proxy_input,
        )
    
    def _build_platform_url(
        self,
        session_id: Optional[str] = None,
        groups: Optional[List[str]] = None,
        country_code: Optional[str] = None,
        subdivision_code: Optional[str] = None,
        password: Optional[str] = None,
    ) -> str:
        """Build a platform proxy URL with the gateway username grammar.

        http://[SUB,]groups-G1+G2,session-xyz,country-US_CA:PASSWORD@HOST:PORT

        The subdivision merges into the country fragment (ISO 3166-2: ``country-US_CA``). The optional leading fragment is the
        subaccount username injected by the server
        (SCRAPELY_PROXY_USERNAME/APIFY_PROXY_USERNAME) so the gateway
        attributes traffic to the right Scrapely tenant; without it,
        the username falls back to 'auto' when no fragments apply.
        """
        from urllib.parse import quote

        env = _platform_env()
        if not env:
            raise ValueError(_platform_missing_error())

        groups = self._groups if groups is None else groups
        country_code = self._country_code if country_code is None else country_code
        subdivision_code = self._subdivision_code if subdivision_code is None else subdivision_code
        password = self._password if password is None else password
        password = password or env['password']

        _validate_country_options(country_code, subdivision_code)

        # Grammar order: groups, then session, then country (with the
        # subdivision merged in as `country-XX_YY`).
        username_parts = []
        if groups:
            username_parts.append(f"groups-{'+'.join(_sanitize_fragment(g) for g in groups)}")
        if session_id is not None:
            username_parts.append(f'session-{_sanitize_fragment(str(session_id))}')
        if country_code and subdivision_code:
            username_parts.append(
                f'country-{_sanitize_fragment(country_code)}_{_sanitize_fragment(subdivision_code)}'
            )
        elif country_code:
            username_parts.append(f'country-{_sanitize_fragment(country_code)}')
        fragments = ','.join(username_parts)

        if env['username']:
            username = _sanitize_fragment(env['username']) + (f',{fragments}' if fragments else '')
        else:
            username = fragments or 'auto'

        return f"http://{username}:{quote(password, safe='')}@{env['hostname']}:{env['port']}"

    def _next_url(self, session_id: Optional[str] = None, proxy_tier: Optional[int] = None) -> str:
        """Pick the next URL (round-robin), honoring tiers and sentinels."""
        if self._tiered_proxy_urls:
            tier = proxy_tier if proxy_tier is not None else self._current_tier
            if not 0 <= tier < len(self._tiered_proxy_urls):
                raise ValueError(f'Proxy tier {tier} is out of range (0..{len(self._tiered_proxy_urls) - 1}).')
            self._current_tier = tier
            urls = [u for u in self._tiered_proxy_urls[tier] if u is not None]
            if urls:
                url = urls[self._tier_url_indexes[tier] % len(urls)]
                self._tier_url_indexes[tier] += 1
                if is_platform_sentinel(url):
                    # Sentinel tiers resolve with the tier's own options.
                    tier_options = self._tier_options.get(tier, {})
                    return self._build_platform_url(
                        session_id=session_id,
                        groups=tier_options.get('groups'),
                        country_code=tier_options.get('country_code'),
                        subdivision_code=tier_options.get('subdivision_code'),
                        password=tier_options.get('password'),
                    )
                return url
            # Tier explicitly has no URLs (all None) — fall through to platform.

        if self._proxy_urls:
            url = self._proxy_urls[self._url_index % len(self._proxy_urls)]
            self._url_index += 1
            if is_platform_sentinel(url):
                return self._build_platform_url(session_id=session_id)
            return url

        return self._build_platform_url(session_id=session_id)

    def _fetch_proxy_status(self) -> Optional[Dict[str, Any]]:
        """Fetch the gateway status payload through the gateway.

        Sends ``GET {status_url}/?format=json`` *through the platform gateway``
        so the proxy handshake itself authenticates the run's credentials.
        Mirrors the JS SDK: 4s timeout, 2 attempts, returns ``None`` when the
        endpoint stays unreachable. Never raises. ``fromPayload`` is True when
        the gateway answered with its own status JSON rather than an HTTP error.
        """
        import urllib.error
        import urllib.request

        # The gateway URL can only be built with complete platform
        # credentials; without them the check is skipped, never fatal.
        env = _platform_env()
        if not env or not env['status_url']:
            return None
        status_url = env['status_url']
        from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

        parts = urlsplit(status_url)
        query = dict(parse_qsl(parts.query))
        query['format'] = 'json'
        url = urlunsplit(parts._replace(path=parts.path or '/', query=urlencode(query)))
        try:
            proxy_url = self._build_platform_url()
        except ValueError:
            return None

        for _attempt in range(CHECK_ACCESS_MAX_ATTEMPTS):
            try:
                opener = urllib.request.build_opener(
                    urllib.request.ProxyHandler({'http': proxy_url, 'https': proxy_url})
                )
                req = urllib.request.Request(url, headers={'Accept': 'application/json'})
                with opener.open(req, timeout=CHECK_ACCESS_REQUEST_TIMEOUT_MILLIS / 1000) as resp:
                    payload = json.loads(resp.read().decode('utf-8', 'replace'))
                return {
                    'connected': payload.get('connected') is True,
                    'connectionError': payload.get('connectionError') or '',
                    'isManInTheMiddle': payload.get('isManInTheMiddle') is True,
                    'fromPayload': True,
                }
            except urllib.error.HTTPError as e:
                if e.code == 407:
                    return {
                        'connected': False,
                        'connectionError': (
                            'Platform proxy authentication failed (HTTP 407). Check the injected '
                            'SCRAPELY_PROXY_*/APIFY_PROXY_* credentials.'
                        ),
                        'isManInTheMiddle': False,
                        'fromPayload': False,
                    }
                if 590 <= e.code <= 599:
                    description = PROXY_STATUS_CODE_DESCRIPTIONS.get(e.code, 'upstream error')
                    return {
                        'connected': False,
                        'connectionError': f'Platform proxy upstream error (HTTP {e.code}: {description}).',
                        'isManInTheMiddle': False,
                        'fromPayload': False,
                    }
                # Other HTTP errors behave like connection errors: retry.
            except Exception:  # noqa: BLE001 - retry connection errors
                pass
        return None

    async def _call_new_url_function(self, session_id: Optional[str] = None) -> str:
        """Invoke the user's ``new_url_function``.

        Functions accepting a positional argument receive the session ID;
        zero-argument callables keep working. Sync and async functions are
        both supported: an awaitable result is awaited.
        """
        import inspect as _inspect

        fn = self._new_url_function
        if fn is None:
            raise ValueError('No new_url_function configured')
        try:
            signature = _inspect.signature(fn)
            accepts_arg = bool(signature.parameters) and any(
                p.kind
                in (_inspect.Parameter.POSITIONAL_ONLY, _inspect.Parameter.POSITIONAL_OR_KEYWORD, _inspect.Parameter.VAR_POSITIONAL)
                for p in signature.parameters.values()
            )
        except (TypeError, ValueError):
            accepts_arg = False

        result = fn(session_id) if accepts_arg else fn()
        if _inspect.isawaitable(result):
            result = await result
        return cast(str, result)

    def _can_pick_country(self) -> bool:
        """True when each URL may get its own exit country (``new_url(country_code=...)``).

        Only for the plain platform proxy with no country set, no custom or
        tiered URLs and no datacenter group: the datacenter pool exits in the
        US only, and a datacenter request pinned to another country is routed
        and billed as residential.
        """
        if self._country_code or self._subdivision_code or self._proxy_urls or self._tiered_proxy_urls or self._new_url_function:
            return False
        if not self._needs_platform:
            return False
        return all(str(g).upper() == 'RESIDENTIAL' for g in self._groups)

    async def new_url(
        self,
        session_id: Optional[str] = None,
        request: Optional[Any] = None,  # Request object from crawlee (unused, kept for API compatibility)
        proxy_tier: Optional[int] = None,
        country_code: Optional[str] = None,
    ) -> str:
        """Get a new proxy URL.

        Async, so callers use ``await proxy_config.new_url(...)``.

        Args:
            session_id: Optional session ID for sticky sessions (validated:
                max 50 chars, only 0-9 a-z A-Z . _ ~). With
                rotating proxy_urls, the same session ID always maps to the
                same URL. When a ``new_url_function`` accepting an argument is
                configured, the session ID is passed to it.
            request: Request object from crawlee (unused, for API compatibility).
            proxy_tier: Optional tier to select when tiered proxies are used.
            country_code: Exit country for this URL only (two-letter ISO
                code). Only for the plain platform proxy without a country of
                its own (residential); ignored otherwise.

        Returns:
            Proxy URL string
        """
        if self._new_url_function:
            return await self._call_new_url_function(session_id)
        return self._new_url_sync(session_id, proxy_tier, country_code=country_code)

    def _new_url_sync(self, session_id: Optional[str] = None, proxy_tier: Optional[int] = None, country_code: Optional[str] = None) -> str:
        """Resolve a proxy URL from the configured proxy_urls / platform gateway."""
        if session_id is not None:
            _validate_session_id(str(session_id))

        if country_code and self._can_pick_country():
            country_code = country_code.upper()
            _validate_country_options(country_code, None)
            key: Tuple[str, int] = (f'{session_id}@{country_code}', -1)
            if session_id is not None and key in self._session_urls:
                return self._session_urls[key]
            url = self._build_platform_url(session_id=session_id, country_code=country_code)
            if session_id is not None:
                self._session_urls[key] = url
            return url

        # Session stickiness is scoped per tier (parity with Crawlee's tiered
        # handling): the same session ID on a different tier resolves to that
        # tier's own URL instead of leaking the pinned URL across tiers. The
        # effective tier mirrors _next_url()'s selection semantics.
        effective_tier = proxy_tier if proxy_tier is not None else self._current_tier
        cache_key: Tuple[str, int] = (str(session_id), effective_tier)

        if session_id is not None and cache_key in self._session_urls:
            return self._session_urls[cache_key]

        url = self._next_url(session_id=session_id, proxy_tier=proxy_tier)
        if session_id is not None:
            self._session_urls[cache_key] = url
        return url
    
    async def initialize(self, check_access: bool = True) -> None:
        """Initialize the proxy configuration.

        This method is called automatically by Actor.create_proxy_configuration().
        It validates the configuration and fetches any necessary data from the API.

        Args:
            check_access: Whether to run the best-effort platform status check.

        Raises:
            ValueError: If proxy configuration is invalid
            ConnectionError: If the platform proxy reports that this configuration
                cannot be used (for example an unknown proxy group), with the
                proxy's ``connectionError`` as the message
        """
        if self._initialized:
            return

        # Get password from environment if not provided
        # (SCRAPELY_* injected by the Scrapely server takes precedence)
        if self._password is None:
            self._password = (
                os.environ.get('SCRAPELY_PROXY_PASSWORD')
                or os.environ.get('APIFY_PROXY_PASSWORD')
            )

        # Validate configuration
        if self._proxy_urls and self._new_url_function:
            raise ValueError('Cannot specify both proxy_urls and new_url_function')

        # Access check: GET {status_url}/?format=json *through the
        # platform gateway* so the proxy handshake itself authenticates the run's
        # credentials. A "connected": false answer raises; timeouts and HTTP
        # errors only warn. Same as the JS Scrapely SDK (platformEnv): the
        # check only applies when this configuration
        # actually uses the platform proxy — BYOP-only configurations skip it
        # even when platform credentials happen to be injected.
        if check_access and self._needs_platform and (
            os.environ.get('SCRAPELY_PROXY_STATUS_URL')
            or os.environ.get('APIFY_PROXY_STATUS_URL')
        ):
            # Run the (blocking urllib) status check off the event loop — worst
            # case it blocks a worker thread for ~8s (2 attempts x 4s timeout),
            # never the actor's loop (JS SDK parity: its check is fully async).
            status = await asyncio.to_thread(self._fetch_proxy_status)
            if status is None:
                logger.warning(
                    'Platform proxy access check timed out. Watch out for errors with status '
                    'code 407. If you see some, it most likely means you do not have access to '
                    'either all or some of the proxies you are trying to use.'
                )
            else:
                self._is_man_in_the_middle = status['isManInTheMiddle']
                if not status['connected'] and status['fromPayload']:
                    raise ConnectionError(status['connectionError'])
                if not status['connected']:
                    logger.warning('Platform proxy access check failed: %s', status['connectionError'])
                else:
                    logger.info('Platform proxy access check succeeded.')

        self._initialized = True
        logger.debug('ProxyConfiguration initialized')

    async def new_proxy_info(
        self,
        session_id: Optional[str] = None,
        request: Optional[Any] = None,  # Request object from crawlee (unused but required for API compatibility)
        proxy_tier: Optional[int] = None,  # Proxy tier, honored when tiered proxies are configured
    ) -> Optional[ProxyInfo]:
        """Get proxy information including URL and credentials.

        This method matches crawlee's ProxyConfiguration.new_proxy_info() signature
        to support crawlee's _basic_crawler.py which calls:
            proxy_configuration.new_proxy_info(request=..., session=...)

        Args:
            session_id: Optional session ID for sticky sessions
            request: Request object from crawlee (unused, for API compatibility)
            proxy_tier: Proxy tier (honored when tiered proxies are configured)

        Returns:
            ProxyInfo object with connection details, or None if no proxy configured
        """
        url = await self.new_url(session_id, proxy_tier=proxy_tier)

        # Derive connection details from the generated URL (covers both custom
        # proxy URLs and the platform gateway), falling back to the injected
        # SCRAPELY_PROXY_*/APIFY_PROXY_* environment values.
        from urllib.parse import urlsplit

        parsed = urlsplit(url)
        env = _platform_env() or {}

        hostname = parsed.hostname or env.get('hostname', '')
        port = parsed.port or env.get('port', 8000)
        password = parsed.password or self._password or env.get('password', '')
        if parsed.username:
            username = parsed.username
        elif self._groups:
            username = f"groups-{'+'.join(self._groups)}"
        else:
            username = env.get('username') or 'auto'

        # Extract scheme from URL
        scheme = 'http'
        if url.startswith('https://'):
            scheme = 'https'
        elif url.startswith('socks5://'):
            scheme = 'socks5'
        elif url.startswith('socks4://'):
            scheme = 'socks4'

        # Attach groups/country_code/subdivision_code only when the URL is the
        # platform gateway.
        is_platform = bool(hostname and env and hostname == env.get('hostname'))
        groups = self._groups if is_platform else []
        country_code = self._country_code if is_platform else None
        subdivision_code = self._subdivision_code if is_platform else None

        return ProxyInfo(
            url=url,
            scheme=scheme,
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            session_id=session_id,
            proxy_tier=proxy_tier,
            groups=groups,
            country_code=country_code,
            subdivision_code=subdivision_code,
        )
