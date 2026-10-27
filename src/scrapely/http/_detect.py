"""Block detection: status + body shape + vendor markers.

Real pages also carry vendor beacons (``__CF$cv$params``, ``_pxAppId``,
``px-captcha``, DataDome cookies), so a marker alone never makes a block.
A block needs an error status, or a small body that is clearly an
interstitial. Successful pages are only checked for the few interstitials
that are served with 200.
"""

from __future__ import annotations

import re
from typing import Mapping, Optional, Sequence

# Vendors / reasons, used as stats keys.
CLOUDFLARE = 'cloudflare'
PERIMETERX = 'perimeterx'
DATADOME = 'datadome'
AKAMAI = 'akamai'
AMAZON = 'amazon'
RATE_LIMIT = '429'
CAPTCHA_511 = '511'
FORBIDDEN = '403'

_PX_SMALL = 60_000
_CF_SMALL = 150_000
_DD_SMALL = 100_000

_PX_CODE_RE = re.compile(r'PXCR\d{4,}|ERRCODE\s*PX', re.I)
_PX_CAPTCHA_RE = re.compile(r'px-captcha|_pxCaptcha|captcha\.px-cdn\.net|/captcha/captcha\.js\?a=c&u=', re.I)
_PX_HUMAN_RE = re.compile(r'needs a human touch|press\s*&(?:amp;)?\s*hold|Robot or human\?', re.I)
_DD_RE = re.compile(r'captcha-delivery\.com|var\s+dd\s*=\s*\{|geo\.captcha-delivery', re.I)
_CF_CHALLENGE_RE = re.compile(
    r'cf_chl_opt|/cdn-cgi/challenge-platform/h/[bg]/orchestrate|<title>Just a moment\.\.\.</title>|'
    r'cf-browser-verification|cf_captcha_kind|challenges\.cloudflare\.com/turnstile',
    re.I,
)
_CF_BLOCK_RE = re.compile(r'Attention Required! \| Cloudflare|cf-error-details|Sorry, you have been blocked', re.I)
_CF_BEACON_RE = re.compile(r'__CF\$cv\$params')
_AKAMAI_RE = re.compile(r'errors\.edgesuite\.net|Reference&#32;&#35;\d|Reference #\d+\.\w+', re.I)
_ACCESS_DENIED_RE = re.compile(r'<title>\s*Access Denied\s*</title>|<H1>Access Denied</H1>', re.I)
_AMAZON_RE = re.compile(r'/errors/validateCaptcha|api-services-support@amazon\.com', re.I)
_CAPTCHA_WORD_RE = re.compile(r'captcha', re.I)


def _h(headers: Mapping[str, str], name: str) -> str:
    v = headers.get(name)
    if v is None:
        v = headers.get(name.title())
    return str(v or '')


def _set_cookies(headers: Mapping[str, str], set_cookies: Sequence[str]) -> str:
    return ' '.join(set_cookies) if set_cookies else _h(headers, 'set-cookie')


def detect_block(
    status: int,
    headers: Mapping[str, str],
    body: bytes,
    set_cookies: Sequence[str] = (),
) -> Optional[str]:
    """The block reason for a response, or None when it is a normal page.

    ``headers`` is a case-insensitive mapping (lowercase keys are enough).
    Returns one of ``cloudflare``, ``perimeterx``, ``datadome``, ``akamai``,
    ``amazon``, ``429``, ``511`` or ``403``.
    """
    size = len(body or b'')
    # Decode a bounded head only; markers live in interstitials, which are small.
    text = (body[:200_000] if body else b'').decode('utf-8', 'replace')
    server = _h(headers, 'server').lower()
    is_cf = 'cloudflare' in server or bool(_h(headers, 'cf-ray'))

    # Cloudflare marks challenges explicitly.
    if _h(headers, 'cf-mitigated').lower() == 'challenge':
        return CLOUDFLARE

    if status in (403, 503, 429) and (is_cf or _CF_CHALLENGE_RE.search(text)):
        if _CF_CHALLENGE_RE.search(text) or _CF_BLOCK_RE.search(text):
            return CLOUDFLARE
        # JS-detection shell: a small page that is nothing but the beacon.
        if _CF_BEACON_RE.search(text) and size < 10_000:
            return CLOUDFLARE

    if status in (403, 429) or status >= 500:
        if size < _PX_SMALL and (_PX_CODE_RE.search(text) or _PX_HUMAN_RE.search(text) or _PX_CAPTCHA_RE.search(text)):
            return PERIMETERX
    elif status == 200 and size < _PX_SMALL and (_PX_CODE_RE.search(text) or (_PX_HUMAN_RE.search(text) and _PX_CAPTCHA_RE.search(text))):
        # PerimeterX block pages served with 200 ("Robot or human?").
        # px-captcha/_pxAppId alone are in normal pages and don't count.
        return PERIMETERX

    if status in (401, 403, 405, 429) and size < _DD_SMALL:
        cookies = _set_cookies(headers, set_cookies).lower()
        if _DD_RE.search(text) or 'datadome=' in cookies or _h(headers, 'x-datadome') or _h(headers, 'x-dd-b'):
            return DATADOME

    if status == 403 and (_AKAMAI_RE.search(text) or ('akamai' in server and _ACCESS_DENIED_RE.search(text))):
        return AKAMAI

    if size < 20_000 and status in (200, 503) and _AMAZON_RE.search(text) and _CAPTCHA_WORD_RE.search(text):
        return AMAZON

    if status == 429:
        return RATE_LIMIT
    if status == 511:
        return CAPTCHA_511
    if status == 403:
        return FORBIDDEN
    return None


def is_retryable_status(status: int) -> bool:
    """Server and gateway errors worth a retry on a fresh identity (not blocks)."""
    return status in (500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 527, 530) or 590 <= status <= 599


def parse_retry_after(value: Optional[str], now: Optional[float] = None) -> Optional[float]:
    """Seconds to wait from a Retry-After header (delta-seconds or an HTTP date)."""
    if not value:
        return None
    value = value.strip()
    try:
        secs = float(value)
        return max(0.0, secs)
    except ValueError:
        pass
    try:
        import time
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(value)
        ref = now if now is not None else time.time()
        return max(0.0, dt.timestamp() - ref)
    except Exception:  # noqa: BLE001 - unparseable header
        return None

