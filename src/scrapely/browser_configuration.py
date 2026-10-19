"""Scrapely remote browser configuration (Evomi websocket parameters).

Passed to browser crawlers as ``browser_configuration``, next to
``proxy_configuration``, or to a raw ``chromium.launch(browser_configuration=...)``.
Outside the Scrapely remote browser (local runs, unpatched builds) the option
is accepted and ignored, so the same code runs everywhere. Mirrors the Node
SDK ``browser-configuration.ts``.
"""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional

_BROWSER_OS_VALUES = ('windows', 'macos', 'android')
_MAX_TIMEZONE_LENGTH = 64
_MAX_LANGUAGE_LENGTH = 128
_COUNTRY_CODE_RE = re.compile(r'^[A-Z]{2}$')
_Q_VALUE_RE = re.compile(r';\s*q=', re.IGNORECASE)


def _fail(field: str, message: str) -> None:
    raise ValueError(f'BrowserConfiguration: "{field}" {message}')


def _require_str(field: str, value: Any) -> str:
    if not isinstance(value, str):
        _fail(field, f'must be a string (got {type(value).__name__}).')
    stripped = value.strip()
    if not stripped:
        _fail(field, 'must not be empty.')
    return stripped


def _normalize_location_name(field: str, value: Any) -> str:
    """Evomi location format: lowercase, dots for spaces (``New York`` → ``new.york``)."""
    return re.sub(r'\s+', '.', _require_str(field, value).lower())


def _require_bool(field: str, value: Any) -> bool:
    if not isinstance(value, bool):
        _fail(field, f'must be a boolean (got {type(value).__name__}).')
    return value


class BrowserConfiguration:
    """Location and fingerprint options of the Scrapely remote browser.

    The remote browser brings its own proxy — custom proxies are not supported
    for browsers.

    Example::

        browser_configuration = await Actor.create_browser_configuration(country_code='US', state='California')
        crawler = PlaywrightCrawler(browser_configuration=browser_configuration, request_handler=handler)

    Args:
        country_code: Exit country, ISO 3166-1 alpha-2 (``US``). Sent as ``proxy_country``.
        state: Region name, not an ISO code (``California``). Sent as ``proxy_state``.
        city: City name (``San Jose``). Sent as ``proxy_city``.
        os: Fingerprint operating system: ``windows``, ``macos`` or ``android``.
        timezone: IANA timezone (``Asia/Tokyo``). Defaults to the exit country's primary zone.
        language: Comma-separated language tags without q-values (``de-CH,de``).
        adblock: Built-in ad/tracker blocking. Browser default: off.
        solve_cloudflare: Automatic Cloudflare Turnstile solving. Browser default: on.
    """

    def __init__(
        self,
        *,
        country_code: Optional[str] = None,
        state: Optional[str] = None,
        city: Optional[str] = None,
        os: Optional[str] = None,
        timezone: Optional[str] = None,
        language: Optional[str] = None,
        adblock: Optional[bool] = None,
        solve_cloudflare: Optional[bool] = None,
    ) -> None:
        options: Dict[str, Any] = {}

        if country_code is not None:
            code = _require_str('country_code', country_code).upper()
            if not _COUNTRY_CODE_RE.match(code):
                _fail('country_code', f'must be a two-letter ISO 3166-1 country code (got "{country_code}").')
            options['country_code'] = code

        if state is not None:
            options['state'] = _normalize_location_name('state', state)
        if city is not None:
            options['city'] = _normalize_location_name('city', city)

        if os is not None:
            os_value = _require_str('os', os).lower()
            if os_value not in _BROWSER_OS_VALUES:
                _fail('os', f'must be one of {", ".join(_BROWSER_OS_VALUES)} (got "{os}").')
            options['os'] = os_value

        if timezone is not None:
            tz = _require_str('timezone', timezone)
            if len(tz) > _MAX_TIMEZONE_LENGTH:
                _fail('timezone', f'must be at most {_MAX_TIMEZONE_LENGTH} characters.')
            options['timezone'] = tz

        if language is not None:
            lang = _require_str('language', language)
            if len(lang) > _MAX_LANGUAGE_LENGTH:
                _fail('language', f'must be at most {_MAX_LANGUAGE_LENGTH} characters.')
            if _Q_VALUE_RE.search(lang):
                _fail('language', 'must be a plain tag list without q-values (e.g. "de-CH,de").')
            options['language'] = lang

        if adblock is not None:
            options['adblock'] = _require_bool('adblock', adblock)
        if solve_cloudflare is not None:
            options['solve_cloudflare'] = _require_bool('solve_cloudflare', solve_cloudflare)

        self._options: Mapping[str, Any] = MappingProxyType(options)

    @property
    def options(self) -> Mapping[str, Any]:
        """Normalized options (read-only)."""
        return self._options

    def to_cdp_params(self) -> Dict[str, str]:
        """Remote browser websocket parameters set by this configuration."""
        o = self._options
        params: Dict[str, str] = {}
        for option, param in (
            ('country_code', 'proxy_country'),
            ('state', 'proxy_state'),
            ('city', 'proxy_city'),
            ('os', 'os'),
            ('timezone', 'timezone'),
            ('language', 'language'),
        ):
            if o.get(option):
                params[param] = o[option]
        for option, param in (('adblock', 'adblock'), ('solve_cloudflare', 'solve_cloudflare')):
            if option in o:
                params[param] = 'true' if o[option] else 'false'
        return params

    def __repr__(self) -> str:
        return f'BrowserConfiguration({dict(self._options)!r})'
