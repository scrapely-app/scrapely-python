"""scrapely.http: an HTTP client that looks like a real browser and handles blocks.

Every request goes out with a current browser fingerprint whose TLS, HTTP/2,
User-Agent, client hints and header order all come from one backend profile.
Requests use identities (profile + proxy session + cookie jar) that are
retired together when a site blocks them; the request is retried on a fresh
identity at once, without waiting: the block was tied to the old identity.
``polite=True`` turns on Retry-After, per-host backoff and a slow mode for
actors that must be gentle with a site.

Quick start::

    from scrapely import Actor
    from scrapely.http import AsyncClient

    proxy = await Actor.create_proxy_configuration(groups=["RESIDENTIAL"], country_code="DE")
    async with AsyncClient(proxy_configuration=proxy) as http:
        r = await http.get("https://example.com/")
        data = (await http.get("https://example.com/api/items", kind="json")).json()

Migrating from curl_cffi is one line: ``from scrapely.http import AsyncSession``.
"""

from ._client import AsyncClient, AsyncHttpSession, Client, HttpSession
from ._compat import AsyncSession, Session
from ._detect import detect_block
from ._options import FingerprintOptions
from ._policy import BackoffPolicy
from ._profiles import BrowserProfile, BrowserSpec, NoMatchingProfile, load_table
from ._response import Headers, HTTPError, Response
from ._stats import HttpStats
from ._transport import TransportError

# curl_cffi / requests-style name for transport errors.
RequestsError = TransportError

__all__ = [
    'AsyncClient',
    'AsyncHttpSession',
    'AsyncSession',
    'BackoffPolicy',
    'BrowserProfile',
    'BrowserSpec',
    'Client',
    'FingerprintOptions',
    'HTTPError',
    'Headers',
    'HttpSession',
    'HttpStats',
    'NoMatchingProfile',
    'RequestsError',
    'Response',
    'Session',
    'TransportError',
    'detect_block',
    'load_table',
]
