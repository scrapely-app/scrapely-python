"""Retry remote-browser (CDP) connects that fail for transient reasons.

The remote browser answers ``429`` when it is at capacity and ``503`` when it
cannot start a browser or validate the session right now, both with a
``Retry-After`` delay (also repeated in the response body, because Playwright
only puts the status line and body into its error message). Connection errors
and timeouts are transient too. Those are retried with exponential backoff,
waiting at least the ``Retry-After`` delay when the error carries one. Any
other status (``400``/``401``/``402``/``403``/``404``) fails at once.

Tuning (environment):

- ``SCRAPELY_CDP_CONNECT_ATTEMPTS``: total attempts, default ``5`` (``1`` disables retries).
- ``SCRAPELY_CDP_RETRY_BASE_SECS``: first backoff delay, default ``1``; doubles per attempt.
- ``SCRAPELY_CDP_RETRY_MAX_SECS``: cap for one backoff delay, default ``30``.
- ``SCRAPELY_CDP_RETRY_AFTER_MAX_SECS``: longest ``Retry-After`` the SDK waits
  out, default ``120``. A longer ``Retry-After`` ends the retries (the SDK never
  retries earlier than the server asked).
"""

from __future__ import annotations

import asyncio
import functools
import logging
import os
import random
import re
import time
from typing import Any, Awaitable, Callable, Optional, Tuple, TypeVar
from urllib.parse import urlsplit

logger = logging.getLogger('scrapely.browser')

T = TypeVar('T')

_RETRY_WRAPPED_ATTR = '_scrapely_cdp_retry_wrapped'

_REASONS = (
    'Too Many Requests|Internal Server Error|Bad Gateway|Service Unavailable|Gateway Timeout|'
    'Request Timeout|Bad Request|Unauthorized|Payment Required|Forbidden|Not Found'
)
_STATUS_RE = re.compile(rf'\b([1-5]\d\d) (?:{_REASONS})\b')
_WS_STATUS_RE = re.compile(r'Unexpected server response:?\s*(\d{3})', re.IGNORECASE)
_RETRY_AFTER_RE = re.compile(r'Retry-After:?\s*(\d+(?:\.\d+)?)', re.IGNORECASE)
_TRANSIENT_MARKERS = (
    'econnrefused',
    'econnreset',
    'etimedout',
    'epipe',
    'eai_again',
    'ehostunreach',
    'enetunreach',
    'socket hang up',
    'timeout',
    'timed out',
    'connection closed',
    'connection refused',
    'connection reset',
    'websocket error',
    'net::err_',
    'target closed',
)


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, ''))
    except ValueError:
        return default
    return value if value > 0 else default


def _max_attempts() -> int:
    return max(1, int(_env_float('SCRAPELY_CDP_CONNECT_ATTEMPTS', 5)))


def parse_status(message: str) -> Optional[int]:
    """HTTP status from a Playwright/Puppeteer connect error message, if any."""
    match = _WS_STATUS_RE.search(message) or _STATUS_RE.search(message)
    return int(match.group(1)) if match else None


def parse_retry_after(message: str) -> Optional[float]:
    match = _RETRY_AFTER_RE.search(message)
    if not match:
        return None
    return float(match.group(1))


def classify_connect_error(err: BaseException) -> Tuple[bool, Optional[int], Optional[float]]:
    """Return ``(retryable, status, retry_after_secs)`` for a connect error."""
    message = f'{type(err).__name__}: {err}'
    status = parse_status(message)
    retry_after = parse_retry_after(message)
    if status is not None:
        return (status == 408 or status == 429 or status >= 500), status, retry_after
    if isinstance(err, (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError)):
        return True, None, retry_after
    lowered = message.lower()
    return any(marker in lowered for marker in _TRANSIENT_MARKERS), None, retry_after


def backoff_delay(attempt: int, retry_after: Optional[float]) -> float:
    """Delay before retry number ``attempt`` (1-based), honoring Retry-After."""
    base = _env_float('SCRAPELY_CDP_RETRY_BASE_SECS', 1.0)
    cap = _env_float('SCRAPELY_CDP_RETRY_MAX_SECS', 30.0)
    delay = min(cap, base * (2 ** (attempt - 1)))
    delay = delay * (0.75 + random.random() * 0.5)
    if retry_after is not None:
        delay = max(delay, retry_after)
    return delay


def _retry_after_too_long(retry_after: Optional[float]) -> bool:
    return retry_after is not None and retry_after > _env_float('SCRAPELY_CDP_RETRY_AFTER_MAX_SECS', 120.0)


def _describe(err: BaseException, status: Optional[int]) -> str:
    if status is not None:
        return f'HTTP {status}'
    # First line only: connect errors can carry the session URL with ids.
    text = str(err).strip().splitlines()[0] if str(err).strip() else type(err).__name__
    return re.sub(r'(wss?|https?)://\S+', '<cdp-url>', text)[:200]


def _log_retry(err: BaseException, status: Optional[int], attempt: int, attempts: int, delay: float) -> None:
    logger.warning(
        'Remote browser connect failed (%s); retrying in %.1fs (attempt %d/%d)',
        _describe(err, status),
        delay,
        attempt + 1,
        attempts,
    )


async def connect_with_retry_async(connect: Callable[[], Awaitable[T]]) -> T:
    """Run an async CDP connect, retrying transient failures."""
    attempts = _max_attempts()
    attempt = 1
    while True:
        try:
            return await connect()
        except asyncio.CancelledError:
            raise
        except Exception as err:
            retryable, status, retry_after = classify_connect_error(err)
            # Never retry earlier than the server asked; give up instead of waiting that long.
            if not retryable or attempt >= attempts or _retry_after_too_long(retry_after):
                raise
            delay = backoff_delay(attempt, retry_after)
            _log_retry(err, status, attempt, attempts, delay)
            await asyncio.sleep(delay)
            attempt += 1


def connect_with_retry_sync(connect: Callable[[], T]) -> T:
    """Run a blocking CDP connect, retrying transient failures."""
    attempts = _max_attempts()
    attempt = 1
    while True:
        try:
            return connect()
        except Exception as err:
            retryable, status, retry_after = classify_connect_error(err)
            # Never retry earlier than the server asked; give up instead of waiting that long.
            if not retryable or attempt >= attempts or _retry_after_too_long(retry_after):
                raise
            delay = backoff_delay(attempt, retry_after)
            _log_retry(err, status, attempt, attempts, delay)
            time.sleep(delay)
            attempt += 1


def is_platform_cdp_url(url: Any, platform_cdp_url: Optional[str]) -> bool:
    """True when ``url`` points at the same host as the platform's CDP_URL."""
    if not isinstance(url, str) or not platform_cdp_url:
        return False
    try:
        target, platform = urlsplit(url), urlsplit(platform_cdp_url)
    except ValueError:
        return False
    return bool(target.netloc) and target.netloc.lower() == platform.netloc.lower()


def wrap_connect_over_cdp(chromium: Any, is_async: bool, get_platform_cdp_url: Callable[[], Optional[str]]) -> None:
    """Make ``chromium.connect_over_cdp`` retry transient failures for the platform CDP host.

    Covers actor code that connects to ``CDP_URL`` directly as well as the
    SDK's own redirected launches. Connects to other hosts are left alone.
    Idempotent.
    """
    original = getattr(chromium, 'connect_over_cdp', None)
    if original is None or getattr(original, _RETRY_WRAPPED_ATTR, False):
        return

    def _target(args: Tuple[Any, ...], kwargs: dict) -> Any:
        if args:
            return args[0]
        return kwargs.get('endpoint_url') or kwargs.get('endpointURL')

    if is_async:

        @functools.wraps(original)
        async def connect_over_cdp(*args: Any, **kwargs: Any) -> Any:
            if not is_platform_cdp_url(_target(args, kwargs), get_platform_cdp_url()):
                return await original(*args, **kwargs)
            return await connect_with_retry_async(lambda: original(*args, **kwargs))

    else:

        @functools.wraps(original)
        def connect_over_cdp(*args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
            if not is_platform_cdp_url(_target(args, kwargs), get_platform_cdp_url()):
                return original(*args, **kwargs)
            return connect_with_retry_sync(lambda: original(*args, **kwargs))

    setattr(connect_over_cdp, _RETRY_WRAPPED_ATTR, True)
    try:
        chromium.connect_over_cdp = connect_over_cdp
    except (AttributeError, TypeError):
        pass


def is_retry_wrapped(fn: Any) -> bool:
    return bool(getattr(fn, _RETRY_WRAPPED_ATTR, False))
