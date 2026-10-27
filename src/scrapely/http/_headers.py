"""Request headers: caller headers placed at the browser's native position.

The backend profile is the only source of the User-Agent, client hints,
sec-fetch values, Accept-Encoding and header order. Callers may add
Accept-Language, Cookie, Referer, Origin, Content-Type, Authorization, Accept
(for API calls) and their own application headers; those go into the
family's order template instead of being appended at the end.
"""

from __future__ import annotations

import json as _json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlencode, urlsplit

from ._profiles import BrowserProfile, ProfileTable

KINDS = ('navigate', 'xhr', 'json')

# Headers that make up the browser fingerprint. Callers can't override them
# (unless allow_header_override=True): a hand-written UA or client hint on top of
# a TLS profile is exactly the mismatch anti-bot vendors look for.
FINGERPRINT_HEADERS = frozenset(
    {
        'user-agent',
        'sec-ch-ua',
        'sec-ch-ua-mobile',
        'sec-ch-ua-platform',
        'sec-ch-ua-full-version',
        'sec-ch-ua-full-version-list',
        'sec-ch-ua-arch',
        'sec-ch-ua-bitness',
        'sec-ch-ua-model',
        'sec-ch-ua-platform-version',
        'sec-ch-ua-wow64',
        'sec-fetch-site',
        'sec-fetch-mode',
        'sec-fetch-user',
        'sec-fetch-dest',
        'accept-encoding',
        'upgrade-insecure-requests',
        'priority',
        'te',
    }
)
# Set by the HTTP stack itself; silently dropped from caller headers.
TRANSPORT_HEADERS = frozenset({'host', 'connection', 'content-length', 'transfer-encoding', 'keep-alive', 'proxy-connection'})

# ─── Accept-Language ────────────────────────────────────────────────────────

_COUNTRY_LANG: Dict[str, str] = {
    'DE': 'de', 'AT': 'de', 'CH': 'de', 'LI': 'de', 'LU': 'fr',
    'FR': 'fr', 'BE': 'fr', 'MC': 'fr', 'SN': 'fr', 'CI': 'fr', 'MA': 'fr', 'TN': 'fr', 'DZ': 'fr',
    'ES': 'es', 'MX': 'es', 'AR': 'es', 'CO': 'es', 'CL': 'es', 'PE': 'es', 'VE': 'es', 'EC': 'es',
    'GT': 'es', 'CU': 'es', 'BO': 'es', 'DO': 'es', 'HN': 'es', 'PY': 'es', 'SV': 'es', 'NI': 'es',
    'CR': 'es', 'PA': 'es', 'UY': 'es', 'PR': 'es',
    'IT': 'it', 'SM': 'it', 'PT': 'pt', 'BR': 'pt', 'AO': 'pt', 'MZ': 'pt',
    'NL': 'nl', 'PL': 'pl', 'CZ': 'cs', 'SK': 'sk', 'HU': 'hu', 'RO': 'ro', 'MD': 'ro', 'BG': 'bg',
    'GR': 'el', 'CY': 'el', 'TR': 'tr', 'RU': 'ru', 'BY': 'ru', 'KZ': 'ru', 'UA': 'uk', 'SE': 'sv',
    'NO': 'nb', 'DK': 'da', 'FI': 'fi', 'IS': 'is', 'EE': 'et', 'LV': 'lv', 'LT': 'lt', 'SI': 'sl',
    'HR': 'hr', 'RS': 'sr', 'BA': 'bs', 'ME': 'sr', 'MK': 'mk', 'AL': 'sq',
    'JP': 'ja', 'KR': 'ko', 'CN': 'zh', 'TW': 'zh', 'HK': 'zh', 'MO': 'zh',
    'VN': 'vi', 'TH': 'th', 'ID': 'id', 'MY': 'ms', 'IL': 'he',
    'SA': 'ar', 'AE': 'ar', 'EG': 'ar', 'QA': 'ar', 'KW': 'ar', 'BH': 'ar', 'OM': 'ar', 'JO': 'ar',
    'LB': 'ar', 'IQ': 'ar',
    'IR': 'fa', 'BD': 'bn', 'GE': 'ka', 'AM': 'hy', 'AZ': 'az',
}
_ENGLISH_COUNTRIES = frozenset(
    {'US', 'GB', 'UK', 'AU', 'CA', 'IE', 'NZ', 'IN', 'SG', 'ZA', 'PH', 'NG', 'KE', 'PK', 'GH', 'JM', 'MT'}
)
DEFAULT_ACCEPT_LANGUAGE = 'en-US,en;q=0.9'


def accept_language_for(country_code: Optional[str] = None, locales: Sequence[str] = ()) -> str:
    """Accept-Language for the given locales, else for the proxy country, else US English.

    ``["de-DE"]`` → ``de-DE,de;q=0.9``; country ``DE`` → ``de-DE,de;q=0.9,en;q=0.8``.
    """
    if locales:
        parts: List[str] = []
        for loc in locales:
            loc = str(loc).strip().replace('_', '-')
            if not loc:
                continue
            if loc not in parts:
                parts.append(loc)
            base = loc.split('-')[0]
            if base != loc and base not in parts:
                parts.append(base)
        out = []
        for i, p in enumerate(parts):
            q = round(max(1.0 - 0.1 * i, 0.1), 1)
            out.append(p if i == 0 else f'{p};q={q}')
        return ','.join(out) or DEFAULT_ACCEPT_LANGUAGE
    cc = (country_code or '').strip().upper()
    if not cc:
        return DEFAULT_ACCEPT_LANGUAGE
    if cc == 'UK':
        cc = 'GB'
    if cc == 'US':
        return DEFAULT_ACCEPT_LANGUAGE
    if cc in _ENGLISH_COUNTRIES:
        return f'en-{cc},en;q=0.9'
    lang = _COUNTRY_LANG.get(cc)
    if not lang:
        return DEFAULT_ACCEPT_LANGUAGE
    return f'{lang}-{cc},{lang};q=0.9,en;q=0.8'


# ─── sec-fetch-site ─────────────────────────────────────────────────────────

_TWO_LEVEL_SUFFIXES = frozenset(
    {
        'co.uk', 'org.uk', 'ac.uk', 'gov.uk', 'com.au', 'net.au', 'org.au', 'co.nz', 'co.jp', 'ne.jp',
        'or.jp', 'co.kr', 'or.kr', 'com.br', 'com.mx', 'com.ar', 'com.co', 'com.pe', 'com.tr', 'com.tw',
        'com.hk', 'com.sg', 'com.my', 'com.ph', 'com.vn', 'co.in', 'co.id', 'co.za', 'co.il', 'com.cn',
        'com.ua', 'com.pl', 'com.eg', 'com.sa', 'com.ng', 'co.th', 'in.th', 'com.ec', 'com.uy', 'com.ve',
    }
)


def _site(host: str) -> str:
    host = (host or '').lower().rstrip('.')
    labels = host.split('.')
    if len(labels) >= 3 and '.'.join(labels[-2:]) in _TWO_LEVEL_SUFFIXES:
        return '.'.join(labels[-3:])
    return '.'.join(labels[-2:])


def _origin(url: str) -> Tuple[str, str, int]:
    p = urlsplit(url)
    port = p.port or (443 if p.scheme == 'https' else 80)
    return (p.scheme.lower(), (p.hostname or '').lower(), port)


def origin_string(url: str) -> str:
    p = urlsplit(url)
    return f'{p.scheme}://{p.netloc}'.lower() if p.scheme and p.netloc else ''


def fetch_site(initiator: Optional[str], target: str) -> str:
    """The sec-fetch-site value for a request from ``initiator`` (a URL or origin) to ``target``."""
    if not initiator:
        return 'none'
    a, b = _origin(initiator), _origin(target)
    if a == b:
        return 'same-origin'
    if a[1] and _site(a[1]) == _site(b[1]) and a[0] == b[0]:
        return 'same-site'
    return 'cross-site'


# ─── request assembly ───────────────────────────────────────────────────────

_JSON_ACCEPT = 'application/json, text/plain, */*'


class PreparedHeaders:
    """Headers, their order and whether the backend's default headers are used."""

    __slots__ = ('headers', 'order', 'default_headers', 'ignored', 'body', 'cookie')

    def __init__(self) -> None:
        self.headers: Dict[str, str] = {}
        self.order: List[str] = []
        self.default_headers = True
        self.ignored: List[str] = []
        self.body: Optional[bytes] = None
        self.cookie: Optional[str] = None


def encode_body(
    data: Any = None,
    json: Any = None,
    content: Any = None,
) -> Tuple[Optional[bytes], Optional[str]]:
    """Body bytes and the content type they imply (None when the caller must set it)."""
    if json is not None:
        return _json.dumps(json, separators=(',', ':'), ensure_ascii=False).encode('utf-8'), 'application/json'
    if content is not None:
        return (content.encode('utf-8') if isinstance(content, str) else bytes(content)), None
    if data is None:
        return None, None
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data), None
    if isinstance(data, str):
        return data.encode('utf-8'), None
    if isinstance(data, Mapping):
        return urlencode(list(data.items()), doseq=True).encode('ascii'), 'application/x-www-form-urlencoded'
    if isinstance(data, (list, tuple)):
        return urlencode(list(data), doseq=True).encode('ascii'), 'application/x-www-form-urlencoded'
    raise TypeError(f'unsupported data type {type(data).__name__}')


def _caller_headers(headers: Any) -> List[Tuple[str, str]]:
    if not headers:
        return []
    items: Iterable[Tuple[Any, Any]] = headers.items() if isinstance(headers, Mapping) else headers
    out = []
    for k, v in items:
        if v is None:
            continue
        out.append((str(k).strip(), str(v)))
    return out


def prepare(
    *,
    table: ProfileTable,
    profile: BrowserProfile,
    kind: str,
    method: str,
    url: str,
    headers: Any,
    accept_language: str,
    body_content_type: Optional[str],
    has_body: bool,
    allow_header_override: bool = False,
) -> PreparedHeaders:
    """Build the header set for one request on one profile."""
    if kind not in KINDS:
        raise ValueError(f'kind must be one of {", ".join(KINDS)}; got {kind!r}')
    method = method.upper()
    prep = PreparedHeaders()
    caller: Dict[str, str] = {}
    caller_case: Dict[str, str] = {}
    for name, value in _caller_headers(headers):
        low = name.lower()
        if low in TRANSPORT_HEADERS:
            continue
        if low in FINGERPRINT_HEADERS and not allow_header_override:
            prep.ignored.append(low)
            continue
        caller[low] = value
        caller_case[low] = name
    if 'cookie' in caller:
        prep.cookie = caller.pop('cookie')
    if kind == 'navigate' and 'accept' in caller and not allow_header_override:
        # A browser's page-load Accept is fixed; a custom one marks the request.
        # Use kind="xhr"/"json" for API calls that need their own Accept.
        caller.pop('accept')
        prep.ignored.append('accept')
    if body_content_type and 'content-type' not in caller:
        caller['content-type'] = body_content_type

    referer = caller.get('referer')
    native = profile.headers
    family = profile.family
    al = caller.pop('accept-language', None) or accept_language

    out: Dict[str, str] = {}
    if referer:
        # Default referrer policy (strict-origin-when-cross-origin): only the
        # origin goes to another origin.
        if fetch_site(referer, url) not in ('same-origin', 'none'):
            caller['referer'] = origin_string(referer) + '/'
    if kind == 'navigate':
        # Backend default headers; we only adjust values that depend on the request.
        prep.default_headers = True
        if al != native.get('accept-language'):
            out['accept-language'] = al
        site = fetch_site(referer, url)
        if site != 'none':
            out['sec-fetch-site'] = site
        if method not in ('GET', 'HEAD') and has_body:
            out['origin'] = caller.pop('origin', None) or origin_string(referer or url)
            if family in ('chrome', 'edge'):
                out['cache-control'] = caller.pop('cache-control', None) or 'max-age=0'
        order = table.order(family, 'navigate')
    else:
        # API call (fetch/XHR): the full set from the profile's native values,
        # without the navigation-only headers (upgrade-insecure-requests,
        # sec-fetch-user).
        prep.default_headers = False
        initiator = caller.get('origin') or caller.get('referer') or origin_string(url) + '/'
        if 'referer' not in caller:
            caller['referer'] = origin_string(initiator) + '/'
        site = fetch_site(initiator, url)
        for h in ('user-agent', 'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform', 'accept-encoding'):
            if h in native:
                out[h] = native[h]
        page_accept = caller.pop('accept', None) or (_JSON_ACCEPT if kind == 'json' else None)
        out['accept'] = page_accept or '*/*'
        if 'origin' in caller or site != 'same-origin' or method not in ('GET', 'HEAD'):
            out['origin'] = caller.pop('origin', None) or origin_string(initiator)
        out['sec-fetch-site'] = site
        out['sec-fetch-mode'] = 'cors'
        out['sec-fetch-dest'] = 'empty'
        out['accept-language'] = al
        if family in ('chrome', 'edge'):
            out['priority'] = 'u=1, i'
        elif family == 'firefox':
            out['priority'] = 'u=4'
            out['te'] = 'trailers'
        elif family == 'safari':
            out['priority'] = 'u=3, i'
        # Chrome puts an Accept set by the page right after user-agent, the
        # default one after sec-ch-ua-mobile (real Chrome 156 captures).
        order = table.order(family, 'xhr_page_accept' if page_accept else 'xhr')

    for k, v in caller.items():
        out[k] = v
    if allow_header_override:
        for k in FINGERPRINT_HEADERS:
            if k in caller:
                out[k] = caller[k]

    # Order: the family template first, then anything else the caller sent, in the
    # caller's order, placed where the template marks custom headers ("*").
    known = [h for h in order if h != '*']
    extras = [h for h in out if h not in known and h != 'cookie']
    final: List[str] = []
    for h in order:
        if h == '*':
            final.extend(extras)
        else:
            final.append(h)
    if '*' not in order:
        final.extend(extras)
    prep.headers = out
    prep.order = final
    return prep
