"""
Base API client for Scrapely storage operations.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from typing import Any, Dict, Optional, Union

import httpx

logger = logging.getLogger(__name__)


class ApiClientError(Exception):
    """HTTP error from the Scrapely API."""

    def __init__(self, message: str, status_code: int, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


# PUT is left out: a KV record PUT is counted (and billed) as a write each time
# it is applied, so a lost response must not be re-sent blindly. Record PUTs
# carry an Idempotency-Key instead (see is_kv_record_url), which the API
# de-duplicates, so they are re-sent after a lost answer (e.g. an edge 502).
IDEMPOTENT_METHODS = frozenset({'GET', 'HEAD', 'DELETE', 'OPTIONS'})
# HAProxy's own 503 page: no backend was available, so nothing was forwarded.
NO_BACKEND_MARKER = 'No server is available to handle this request'


def is_retryable_response(
    status: int, idempotent: bool, error_type: Optional[str] = None, body_text: Optional[str] = None
) -> bool:
    """Whether a non-2xx answer is retried.

    429 always: the run API limiter and dataset backpressure refuse it before
    anything is written. Idempotent methods: also 500/502/503/504. Others
    (dataset push, KV PUT, named storage create) only on a 503 that certainly
    was not processed: the rate limiter refused it ("rate-limit-unavailable",
    before the handler), another request held the storage name
    ("storage-name-busy", nothing changed), or HAProxy had no backend (its own
    page, nothing forwarded). Any other 503 may mean "outcome unknown" (a
    direct dataset write whose commit was not confirmed), and re-sending could
    store the items twice.
    """
    if status == 429:
        return True
    if idempotent:
        return status in (500, 502, 503, 504)
    if status != 503:
        return False
    return error_type in ('rate-limit-unavailable', 'storage-name-busy') or NO_BACKEND_MARKER in (body_text or '')


def unwrap_data(value: Any) -> Any:
    """Unwrap v2 response envelopes ({ data: ... })."""
    if isinstance(value, list):
        return value
    if isinstance(value, dict) and 'data' in value:
        return value['data']
    return value


def _error_type(response: httpx.Response) -> Optional[str]:
    try:
        body = response.json()
    except Exception:
        return None
    if isinstance(body, dict) and isinstance(body.get('error'), dict):
        t = body['error'].get('type')
        return t if isinstance(t, str) else None
    return None


# Default time budget for retrying one API call (429, 503, network errors).
DEFAULT_API_RETRY_WINDOW_SECS = 300.0
_MAX_BACKOFF_SECS = 30.0

# Response header the API sets on every dataset push that sent an
# Idempotency-Key, once it de-duplicates such pushes. An older API ignores the
# key, and re-sending a push there could store the rows twice, so ambiguous
# failures of keyed writes are retried only while the latest keyed answer from
# that API origin carried this header. A 2xx without it (an older instance,
# e.g. mid-deploy or after a rollback) switches the retries off again.
IDEMPOTENCY_SUPPORT_HEADER = 'X-Scrapely-Idempotency'
_KEY_IN_PROGRESS = 'idempotency-key-in-progress'
_OUTCOME_UNKNOWN = 'push-outcome-unknown'
_keyed_writes_deduped: Dict[str, bool] = {}
_keyed_writes_override: Optional[bool] = None


def is_kv_record_url(url: str) -> bool:
    """A key-value store record URL (``.../key-value-stores/<id>/records/<key>``)."""
    try:
        path = httpx.URL(url).path
    except Exception:
        path = url
    return '/key-value-stores/' in path and '/records/' in path


def _origin_of(url: str) -> str:
    """What is learned per API: its origin, separately for record PUTs and other keyed writes."""
    try:
        u = httpx.URL(url)
        origin = f'{u.scheme}://{u.netloc.decode()}'
    except Exception:
        origin = url
    return origin + '#kv-record' if is_kv_record_url(url) else origin


def api_dedupes_keyed_writes(url: Optional[str] = None) -> bool:
    """Whether the API at ``url`` (default: the last one seen) de-duplicates keyed writes.

    Record PUTs count as de-duplicated until a keyed record PUT was stored
    without the support header: re-applying the same body to the same record
    leaves the same record, so the worst case on an API that ignores the key
    is one extra counted write, while not re-sending fails the run.
    """
    if _keyed_writes_override is not None:
        return _keyed_writes_override
    if url is not None:
        learned = _keyed_writes_deduped.get(_origin_of(url))
        if learned is None and is_kv_record_url(url):
            return True
        return learned is True
    return bool(_keyed_writes_deduped) and list(_keyed_writes_deduped.values())[-1] is True


def _reset_keyed_write_support(value: Optional[bool] = None) -> None:
    """Test hook: forget what was learned about the API, or force True for every origin."""
    global _keyed_writes_override
    _keyed_writes_deduped.clear()
    _keyed_writes_override = True if value is True else None


def _note_keyed_answer(url: str, response: httpx.Response) -> None:
    global _keyed_writes_override
    origin = _origin_of(url)
    if IDEMPOTENCY_SUPPORT_HEADER in response.headers:
        _keyed_writes_deduped.pop(origin, None)
        _keyed_writes_deduped[origin] = True
    elif response.is_success:
        # Stored by a handler that ignored the key: this API (instance) does not de-duplicate.
        _keyed_writes_deduped.pop(origin, None)
        _keyed_writes_deduped[origin] = False
        _keyed_writes_override = None


def _mark_outcome_unknown(exc: BaseException) -> BaseException:
    """Mark an error whose request an earlier attempt may have applied."""
    try:
        setattr(exc, 'outcome_unknown', True)
    except Exception:
        pass
    return exc


def api_retry_window_secs() -> float:
    """How long one API call keeps retrying before it gives up.

    ``SCRAPELY_API_RETRY_WINDOW_SECS`` when set, else ``ApiClient.retry_window_secs``
    (5 minutes by default).
    """
    raw = os.environ.get('SCRAPELY_API_RETRY_WINDOW_SECS')
    if raw:
        try:
            value = float(raw)
            if value >= 0:
                return value
        except ValueError:
            pass
    return ApiClient.retry_window_secs


def _parse_retry_after(retry_after: Optional[str], now: Optional[float]) -> Optional[float]:
    if retry_after is None or not retry_after.strip():
        return None
    secs: Optional[float] = None
    try:
        secs = float(retry_after)
    except ValueError:
        try:
            from email.utils import parsedate_to_datetime

            secs = max(0.0, parsedate_to_datetime(retry_after).timestamp() - (time.time() if now is None else now))
        except Exception:
            secs = None
    if secs is None or secs < 0 or secs != secs:
        return None
    # Not capped: a Retry-After past the retry window ends the retries instead.
    return secs


def server_retry_delay(attempt: int, retry_after: Optional[str], now: Optional[float] = None) -> float:
    """Delay in seconds before retry number ``attempt + 1``.

    Exponential backoff (0.5 s doubling, capped at 30 s) with equal jitter, and
    never earlier than the server's Retry-After (seconds or HTTP date) plus a little jitter so throttled clients don't all come back in
    the same instant.
    """
    exp: float = min(_MAX_BACKOFF_SECS, 0.5 * float(2 ** min(attempt, 16)))
    backoff = exp / 2 + random.random() * (exp / 2)
    ra = _parse_retry_after(retry_after, now)
    if ra is None:
        return backoff
    return max(ra + random.random() * min(1.0, ra * 0.2 + 0.1), backoff)


class AdaptiveRateLimiter:
    """Adaptive throttling based on X-RateLimit-* headers (TS AdaptiveRateLimiter)."""

    def __init__(self) -> None:
        self.limit = 1000
        self.remaining = 1000
        self.reset_at = 0.0
        self.low_water_mark = 50
        self.min_delay = 0.05
        self.max_delay = 1.0
        self.last_request_time = 0.0
        self._rate_limit_errors: Dict[int, int] = {}

    def update_from_headers(self, headers: httpx.Headers) -> None:
        limit_header = headers.get('X-RateLimit-Limit')
        remaining_header = headers.get('X-RateLimit-Remaining')
        reset_header = headers.get('X-RateLimit-Reset')
        if limit_header:
            try:
                self.limit = int(limit_header) or 600
            except ValueError:
                pass
        if remaining_header:
            try:
                self.remaining = int(remaining_header)
            except ValueError:
                pass
        if reset_header:
            try:
                self.reset_at = float(reset_header) * 1000
            except ValueError:
                pass

    async def wait_for_slot(self) -> None:
        now = time.time() * 1000
        if now >= self.reset_at:
            self.remaining = self.limit

        delay = self.min_delay
        if self.remaining < self.low_water_mark:
            ratio = self.remaining / self.low_water_mark if self.low_water_mark else 0
            delay = min(self.max_delay, self.min_delay + (1 - ratio) * 0.5)
        elif self.remaining < self.limit * 0.2:
            delay = 0.1

        time_since_last = (now - self.last_request_time) / 1000
        if time_since_last < delay:
            await asyncio.sleep(delay - time_since_last)

        self.last_request_time = time.time() * 1000

    def record_rate_limited(self) -> None:
        self._rate_limit_errors[429] = self._rate_limit_errors.get(429, 0) + 1

    def get_rate_limit_errors(self) -> Dict[int, int]:
        return dict(self._rate_limit_errors)


class ApiClient:
    """Base HTTP client for Scrapely API operations.

    Handles authentication, rate limiting, and error handling for all API calls.
    """

    _rate_limiter = AdaptiveRateLimiter()
    # How long one call keeps retrying 429/503/network errors before it gives
    # up (SCRAPELY_API_RETRY_WINDOW_SECS overrides it). Long enough that a
    # throttled run slows down instead of failing, and covers an API redeploy.
    retry_window_secs = DEFAULT_API_RETRY_WINDOW_SECS

    def __init__(
        self,
        api_url: str,
        token: str,
        run_id: str,
        timeout: float = 30.0,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.api_url = api_url.rstrip('/')
        self.token = token
        self.run_id = run_id
        self.timeout = timeout
        self._transport = transport
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> 'ApiClient':
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            kwargs: Dict[str, Any] = {
                'timeout': self.timeout,
                'headers': {
                    'Authorization': f'Bearer {self.token}',
                    'X-Scrapely-Run-Id': self.run_id,
                },
            }
            if self._transport is not None:
                kwargs['transport'] = self._transport
            self._client = httpx.AsyncClient(**kwargs)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    def get_rate_limit_errors(self) -> Dict[int, int]:
        return self._rate_limiter.get_rate_limit_errors()

    def _url(self, path: str) -> str:
        if path.startswith('http://') or path.startswith('https://'):
            return path
        if not path.startswith('/'):
            path = f'/{path}'
        return f'{self.api_url}{path}'

    async def _raise_for_status(self, response: httpx.Response) -> None:
        if response.is_success:
            return
        body: Any = None
        message = response.reason_phrase
        try:
            body = response.json()
            if isinstance(body, dict):
                error = body.get('error') or {}
                if isinstance(error, dict) and error.get('message'):
                    message = error['message']
                elif body.get('message'):
                    message = body['message']
        except Exception:
            body = response.text
            if body:
                message = body
        raise ApiClientError(message, response.status_code, body)

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_data: Any = None,
        content: Optional[Union[bytes, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        raise_for_status: bool = True,
        do_not_retry_timeouts: bool = False,
        idempotent: Optional[bool] = None,
        retry_deadline: Optional[float] = None,
        idempotency_key: Optional[str] = None,
    ) -> httpx.Response:
        """Make an HTTP request with the API retry policy.

        Retries, with exponential backoff and jitter that honours Retry-After,
        until the retry window (5 minutes by default) runs out:

        - 429 (run API rate limit, dataset backpressure): always; nothing was applied.
        - 503 from the limiter or HAProxy: always; nothing was forwarded.
        - 500/502/503/504, read/write timeouts and dropped connections:
          idempotent calls, and writes with an ``idempotency_key`` once the API
          is known to de-duplicate them (a re-send returns the original answer).
        - Refused connections and connect/pool timeouts: always; the request
          never left.

        Any other write (KV PUT, a keyed push to an API that did not say it
        de-duplicates) whose request may have reached the API is never re-sent:
        a re-send could apply it twice. Such failures surface to the caller
        instead, with ``outcome_unknown = True`` when an attempt may have been
        applied.

        ``retry_deadline`` (a ``time.monotonic()`` value) ends the retries
        earlier, so a caller with a time budget (e.g. the exit flush) gets an
        answer instead of being cancelled mid-retry.
        """
        client = await self._ensure_client()
        url = self._url(path)
        logger.debug('%s %s params=%s', method, url, params)

        request_headers = dict(headers or {})
        if json_data is not None and 'Content-Type' not in request_headers:
            request_headers['Content-Type'] = 'application/json'
        if json_data is not None:
            # Unknown objects (datetimes, LLM result
            # objects, ...) are stored as strings instead of raising.
            content = json.dumps(json_data, ensure_ascii=False, default=str).encode('utf-8')

        if idempotent is None:
            idempotent = method.upper() in IDEMPOTENT_METHODS
        if idempotency_key is not None:
            request_headers['Idempotency-Key'] = idempotency_key

        def safe_to_resend() -> bool:
            # Idempotent calls, and keyed writes once the API showed it
            # de-duplicates them (learned from any keyed answer).
            return bool(idempotent) or (idempotency_key is not None and api_dedupes_keyed_writes(url))

        # An attempt of a non-idempotent call may have been applied (timeout,
        # dropped connection, 5xx): the final error must not claim "nothing applied".
        maybe_applied = False

        def fail(exc: BaseException) -> BaseException:
            return _mark_outcome_unknown(exc) if maybe_applied and not idempotent else exc

        deadline = time.monotonic() + max(0.0, api_retry_window_secs())
        if retry_deadline is not None:
            deadline = min(deadline, retry_deadline)
        attempt = 0

        async def backoff(why: str, retry_after: Optional[str]) -> bool:
            """Sleep before the next attempt; False once the retry window is used up."""
            nonlocal attempt
            # Retrying sooner than asked would only be refused again: give up
            # when the next try would land past the window / deadline.
            wait = server_retry_delay(attempt, retry_after)
            if time.monotonic() + wait > deadline:
                return False
            attempt += 1
            logger.warning('%s %s %s; retry %d in %.1fs', method, path, why, attempt, wait)
            await asyncio.sleep(wait)
            return True

        while True:
            await self._rate_limiter.wait_for_slot()
            request_kwargs: Dict[str, Any] = {}
            if retry_deadline is not None:
                # A caller with a time budget: no attempt may outlive it.
                request_kwargs['timeout'] = max(0.5, min(self.timeout, retry_deadline - time.monotonic()))
            try:
                response = await client.request(
                    method,
                    url,
                    params=params,
                    content=content,
                    headers=request_headers or None,
                    **request_kwargs,
                )
            except (httpx.ConnectTimeout, httpx.PoolTimeout, httpx.ConnectError) as exc:
                # Never reached the API: safe for every method.
                if isinstance(exc, (httpx.ConnectTimeout, httpx.PoolTimeout)) and do_not_retry_timeouts:
                    raise fail(exc)
                if await backoff(f'failed ({type(exc).__name__})', None):
                    continue
                raise fail(exc)
            except httpx.TimeoutException as exc:
                # Read/write timeout: the request may have been applied.
                maybe_applied = True
                if safe_to_resend() and not do_not_retry_timeouts and await backoff(f'timed out ({type(exc).__name__})', None):
                    continue
                raise fail(exc)
            except httpx.TransportError as exc:
                # Reset mid-request: re-sent only when that is safe.
                maybe_applied = True
                if safe_to_resend() and await backoff(f'failed ({type(exc).__name__})', None):
                    continue
                raise fail(exc)

            self._rate_limiter.update_from_headers(response.headers)
            if response.status_code == 429:
                self._rate_limiter.record_rate_limited()
            if idempotency_key is not None:
                _note_keyed_answer(url, response)

            if not response.is_success:
                status = response.status_code
                error_type = _error_type(response) if status == 409 or status >= 500 else None
                body_text = response.text if status == 503 else None
                # A 5xx other than the limiter's / HAProxy's 503 may come after the write was applied.
                if not idempotent and status >= 500 and not is_retryable_response(status, False, error_type, body_text):
                    maybe_applied = True
                if idempotency_key is not None and error_type == _OUTCOME_UNKNOWN:
                    # Stored or not, nobody knows: the API refuses repeats of this key. Final.
                    maybe_applied = True
                else:
                    # 409 for a keyed write: an earlier attempt is still being stored.
                    in_progress = idempotency_key is not None and status == 409 and error_type == _KEY_IN_PROGRESS
                    if in_progress or is_retryable_response(status, safe_to_resend(), error_type, body_text):
                        if await backoff(f'answered {status}', response.headers.get('Retry-After')):
                            continue
                        if in_progress:
                            maybe_applied = True

            if raise_for_status:
                try:
                    await self._raise_for_status(response)
                except ApiClientError as exc:
                    raise fail(exc)
            return response

    async def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        return await self.request('GET', path, params=params, raise_for_status=raise_for_status)

    async def head(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        return await self.request('HEAD', path, params=params, raise_for_status=raise_for_status)

    async def post(
        self,
        path: str,
        json_data: Any = None,
        content: Optional[Union[bytes, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        *,
        raise_for_status: bool = True,
        idempotent: Optional[bool] = None,
        retry_deadline: Optional[float] = None,
        idempotency_key: Optional[str] = None,
    ) -> httpx.Response:
        return await self.request(
            'POST',
            path,
            params=params,
            json_data=json_data,
            content=content,
            headers=headers,
            raise_for_status=raise_for_status,
            idempotent=idempotent,
            retry_deadline=retry_deadline,
            idempotency_key=idempotency_key,
        )

    async def put(
        self,
        path: str,
        json_data: Any = None,
        content: Optional[Union[bytes, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        *,
        raise_for_status: bool = True,
        idempotent: Optional[bool] = None,
        idempotency_key: Optional[str] = None,
    ) -> httpx.Response:
        return await self.request(
            'PUT',
            path,
            params=params,
            json_data=json_data,
            content=content,
            headers=headers,
            raise_for_status=raise_for_status,
            idempotent=idempotent,
            idempotency_key=idempotency_key,
        )

    async def delete(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Any = None,
        *,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        return await self.request(
            'DELETE',
            path,
            params=params,
            json_data=json_data,
            raise_for_status=raise_for_status,
        )

    async def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        response = await self.get(path, params=params)
        return unwrap_data(response.json())

    async def post_json(
        self,
        path: str,
        data: Any = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        *,
        idempotent: Optional[bool] = None,
        idempotency_key: Optional[str] = None,
    ) -> Any:
        response = await self.post(
            path, json_data=data, params=params, headers=headers, idempotent=idempotent, idempotency_key=idempotency_key
        )
        if not response.content:
            return {}
        try:
            return unwrap_data(response.json())
        except Exception:
            return {}

    async def put_json(
        self,
        path: str,
        data: Any = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        response = await self.put(path, json_data=data, params=params, headers=headers)
        if not response.content:
            return {}
        try:
            return unwrap_data(response.json())
        except Exception:
            return {}
