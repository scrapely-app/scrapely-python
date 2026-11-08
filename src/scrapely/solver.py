"""Anti-bot solver helpers for Scrapely Actors.

The platform runs the challenge solver behind
``/v2/solver``. Actors never hold a solver key: the platform injects
``SCRAPELY_SOLVER_URL`` and ``SCRAPELY_SOLVER_API_KEY`` (the run's own token)
into runs of Actors that may use it.

Three layers, use what you need:

* ``solver_status(vendor)`` / ``await Actor.solver_status(vendor)``: a free,
  cached status check. When a vendor is ``down`` take your fallback (for
  example the hosted browser) or fail the items that need a solve at once.
* ``Solver``: solves one challenge on your own HTTP session. The server tells
  the SDK which requests to send (challenge document, scripts, submission);
  they go out through *your* session so the clearance is bound to your exit IP.
  Cookies land in the session's jar; the clearance is verified and reported.
* ``SolvePolicy``: retries on fresh proxy sessions with backoff and a breaker
  (the policy the solver Actors already use), plus ``VendorUnavailable``,
  which is raised at once, without retries, when the platform says the
  vendor is down.

Typical use::

    from scrapely.solver import Solver, SolvePolicy, SolveFailed, VendorUnavailable

    solver = Solver(vendor="datadome", accept_language="es-ES,es;q=0.9", timezone="Europe/Madrid")
    policy = SolvePolicy()

    def attempt(i):
        session = new_session(i)                      # fresh exit per try
        r = session.get(url)
        if solver.is_blocked(r):
            solver.solve(session, url, r, session_id=f"s{i}")
            r = session.get(url)
        return r

    try:
        page = policy.run(attempt, label=url)
    except VendorUnavailable:
        ...  # vendor down: fail this item at once (or use your fallback)
    except SolveFailed:
        ...  # every try failed: record a per-item error, keep going

When a vendor is down and the Actor has no fallback, the run must still
start and finish: call ``await Actor.set_solver_degraded_status()`` once, fail
the items that need a solve, keep everything else.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import random
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

import httpx

__all__ = [
    "AkamaiSensor",
    "Clearance",
    "DEGRADED_STATUS_MESSAGE",
    "IpBanned",
    "SolveAttemptError",
    "SolveFailed",
    "SolvePolicy",
    "Solver",
    "SolverAuthError",
    "SolverRateLimited",
    "SolverRequestInvalid",
    "SensorResult",
    "SolverUnavailable",
    "VendorUnavailable",
    "abck_state",
    "check_solver_response",
    "degraded_status_message",
    "detect_vendor",
    "find_pixel_collector",
    "find_sbsd_bundle",
    "find_sensor_script",
    "is_blocked",
    "solver_env",
    "solver_status",
    "solver_status_async",
]

DEGRADED_STATUS_MESSAGE = "Site protection temporarily unavailable, results may be incomplete"

# Codes the platform answers with when a vendor cannot be solved right now.
UNAVAILABLE_CODES = ("vendor_degraded", "vendor_down", "provider_unavailable", "vendor_unsupported", "solver_not_enabled")

_COMPAT_SUFFIXES = ("/compat/v1",)
_STATUS_TTL_SECS = 15.0
_MAX_BODY = 4 * 1024 * 1024


# ─── environment ─────────────────────────────────────────────────────────────


def solver_env() -> tuple[str, str] | None:
    """Solver URL and key (the run token), injected by the platform. None when not set.

    Never hardcode the host or the key, and never log the key.
    """
    url = (os.environ.get("SCRAPELY_SOLVER_URL") or "").strip().rstrip("/")
    key = (os.environ.get("SCRAPELY_SOLVER_API_KEY") or "").strip()
    if not url or not key:
        return None
    return url, key


def _gateway() -> tuple[str, str] | None:
    """Base of the platform solver API (…/v2/solver) and the run token, or None.

    Only available when the platform serves the solver (the URL ends with the
    compat route); a raw solver URL means the old direct mode.
    """
    env = solver_env()
    if env is None:
        return None
    url, key = env
    for suffix in _COMPAT_SUFFIXES:
        if url.endswith(suffix):
            return url[: -len(suffix)], key
    return None


# ─── errors ──────────────────────────────────────────────────────────────────


class SolveAttemptError(RuntimeError):
    """One try failed. Worth another try on a fresh exit."""


class IpBanned(SolveAttemptError):
    """The address is refused outright. Only a new exit helps."""


class SolverRateLimited(SolveAttemptError):
    """The solver answered 429. Not retried for this item."""

    def __init__(self, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        suffix = f", retry after {retry_after:g}s" if retry_after is not None else ""
        super().__init__(f"solver rate limit hit (HTTP 429{suffix})")


class SolverRequestInvalid(SolveAttemptError):
    """The solver rejected the request itself (HTTP 400). Not retried."""


class SolverAuthError(SolveAttemptError):
    """The solver refused the key (HTTP 401/403). Opens the breaker."""


class SolveFailed(RuntimeError):
    """Every try for one item failed."""

    def __init__(self, message: str, attempts: int, last_error: BaseException | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


class SolverUnavailable(SolveFailed):
    """The breaker is open: the solver failed for everything recently."""


class VendorUnavailable(SolverUnavailable):
    """The platform says this vendor cannot be solved right now (fail fast, no retries).

    ``code`` is one of ``vendor_down``, ``vendor_degraded``,
    ``provider_unavailable``, ``vendor_unsupported`` or ``solver_not_enabled``.
    """

    def __init__(self, code: str, vendor: str | None = None, retry_after: float | None = None, message: str | None = None) -> None:
        self.code = code
        self.vendor = vendor
        self.retry_after = retry_after
        super().__init__(message or f"anti-bot solver unavailable ({code}{f', {vendor}' if vendor else ''})", 0)


def _json(body: str) -> dict:
    try:
        parsed = json.loads(body or "")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def check_solver_response(status: int, body: str = "", retry_after: str | None = None) -> None:
    """Raise the right error for a non-2xx solver answer; return on 2xx."""
    if 200 <= status < 300:
        return
    detail = (body or "")[:200].replace("\n", " ")
    seconds: float | None = None
    try:
        seconds = float(retry_after) if retry_after is not None else None
    except (TypeError, ValueError):
        seconds = None
    if status == 503:
        parsed = _json(body)
        code = str(parsed.get("code") or "")
        if code in UNAVAILABLE_CODES:
            raise VendorUnavailable(code, parsed.get("vendor"), seconds if seconds is not None else parsed.get("retryAfterSecs"))
    if status == 429:
        raise SolverRateLimited(seconds)
    if status in (401, 403):
        parsed = _json(body)
        if parsed.get("code") == "solver_not_enabled":
            raise VendorUnavailable("solver_not_enabled", None, None, "the anti-bot solver is not enabled for this Actor")
        raise SolverAuthError(f"solver refused the key (HTTP {status})")
    if status == 400:
        raise SolverRequestInvalid(f"solver rejected the request (HTTP 400): {detail}")
    raise SolveAttemptError(f"solver HTTP {status}: {detail}")


_NOT_RETRIED = (SolverRateLimited, SolverRequestInvalid, SolverAuthError)


# ─── retry policy ────────────────────────────────────────────────────────────


class SolvePolicy:
    """Up to `attempts` tries per item, each on a fresh proxy session, with backoff and a breaker.

    * HTTP 5xx, timeouts, transport errors, empty solutions and a page still
      blocked after the clearance are failed tries;
    * HTTP 429 ends the tries for that item;
    * HTTP 401/403 opens the breaker at once;
    * after `trip_after` consecutive failed tries the breaker opens and
      further solves fail fast with SolverUnavailable; after `cooldown`
      seconds one probe is let through;
    * VendorUnavailable (the platform says the vendor is down) is raised at
      once, without retries and without touching the breaker.
    """

    def __init__(
        self,
        attempts: int = 4,
        base_delay: float = 1.5,
        max_delay: float = 15.0,
        trip_after: int = 12,
        cooldown: float = 180.0,
        sleep: Callable[[float], None] = time.sleep,
        async_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self.attempts = max(1, min(5, attempts))
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.trip_after = max(1, trip_after)
        self.cooldown = cooldown
        self._sleep = sleep
        self._async_sleep = async_sleep
        self._clock = clock
        self._rng = rng
        self._lock = threading.Lock()
        self.consecutive_failures = 0
        self.opened_at: float | None = None
        self.calls = 0
        self.solved = 0
        self.failed_items = 0
        self.unavailable_items = 0
        self.last_error = ""

    @property
    def down(self) -> bool:
        with self._lock:
            return self.opened_at is not None and self._clock() - self.opened_at < self.cooldown

    @property
    def ever_tripped(self) -> bool:
        return self.opened_at is not None

    def _admit(self) -> None:
        with self._lock:
            if self.opened_at is None:
                return
            if self._clock() - self.opened_at < self.cooldown:
                raise SolverUnavailable(
                    f"challenge solver unavailable after {self.trip_after} consecutive failed solves"
                    + (f" (last error: {self.last_error})" if self.last_error else ""),
                    0,
                )
            self.opened_at = self._clock()
            self.consecutive_failures = self.trip_after - 1

    def _success(self) -> None:
        with self._lock:
            self.calls += 1
            self.solved += 1
            self.consecutive_failures = 0
            self.opened_at = None

    def _failure(self, exc: BaseException) -> None:
        with self._lock:
            self.calls += 1
            self.consecutive_failures += 1
            self.last_error = _short(exc)
            if isinstance(exc, SolverAuthError) or self.consecutive_failures >= self.trip_after:
                self.opened_at = self._clock()

    def _unavailable(self) -> None:
        with self._lock:
            self.unavailable_items += 1

    def delay(self, index: int) -> float:
        raw = min(self.max_delay, self.base_delay * (2 ** index))
        return raw * (0.5 + self._rng())

    def run(self, attempt: Callable[[int], Any], label: str = "page") -> Any:
        """Call attempt(i) until it returns. Sync version (for worker threads)."""
        last: BaseException | None = None
        for index in range(self.attempts):
            self._admit()
            try:
                result = attempt(index)
            except VendorUnavailable:
                self._unavailable()
                raise
            except Exception as exc:  # noqa: BLE001 - every failure is one failed try
                last = exc
                self._failure(exc)
                if isinstance(exc, _NOT_RETRIED):
                    break
                if index + 1 < self.attempts and not self.down:
                    self._sleep(self.delay(index))
                continue
            self._success()
            return result
        return self._give_up(label, last)

    async def run_async(self, attempt: Callable[[int], Any], label: str = "page") -> Any:
        """Async version. attempt(i) may be a coroutine function or a plain one."""
        last: BaseException | None = None
        for index in range(self.attempts):
            self._admit()
            try:
                result = attempt(index)
                if inspect.isawaitable(result):
                    result = await result
            except asyncio.CancelledError:
                raise
            except VendorUnavailable:
                self._unavailable()
                raise
            except Exception as exc:  # noqa: BLE001
                last = exc
                self._failure(exc)
                if isinstance(exc, _NOT_RETRIED):
                    break
                if index + 1 < self.attempts and not self.down:
                    await self._async_sleep(self.delay(index))
                continue
            self._success()
            return result
        return self._give_up(label, last)

    def _give_up(self, label: str, last: BaseException | None) -> Any:
        with self._lock:
            self.failed_items += 1
        if isinstance(last, SolverUnavailable):
            raise last
        raise SolveFailed(f"anti-bot challenge on {label} was not solved: {_short(last)}", self.attempts, last)

    def summary(self) -> dict[str, int]:
        return {
            "solverCalls": self.calls,
            "solved": self.solved,
            "failedItems": self.failed_items,
            "unavailableItems": self.unavailable_items,
        }


def _short(exc: BaseException | None) -> str:
    if exc is None:
        return "unknown error"
    text = str(exc) or type(exc).__name__
    return text[:200]


# ─── detection ───────────────────────────────────────────────────────────────

_DD_RE = re.compile(r"var\s+dd\s*=\s*\{")
_SBSD_RE = re.compile(r'<script[^>]+src="([^"]+\?v=[0-9a-f-]{36}[^"]*)"', re.I)
_PX_RE = re.compile(r"_pxappid|px-captcha|perimeterx|/_px\d?/", re.I)
_KASADA_RE = re.compile(r"kpsdk", re.I)


def detect_vendor(status: int, body: str) -> str | None:
    """The protection behind a *challenge* response: datadome, akamai-sbsd, perimeterx, kasada or None.

    Ordinary pages that merely load a vendor's script are not challenges:
    DataDome, PerimeterX and Kasada need an error status (4xx), and an SBSD
    bundle counts only on a short interstitial page.
    """
    body = body or ""
    sample = body[:20000]
    head = sample[:4000].lower()
    challenged = status >= 400
    if challenged and ("captcha-delivery.com" in head or _DD_RE.search(sample) or "datadome" in head):
        return "datadome"
    if len(body) <= 20000 and _SBSD_RE.search(sample):
        return "akamai-sbsd"
    if challenged and _PX_RE.search(sample):
        return "perimeterx"
    if challenged and _KASADA_RE.search(sample):
        return "kasada"
    return None


def _status_body(response: Any) -> tuple[int, str]:
    if isinstance(response, tuple):
        return int(response[0]), str(response[1] or "")
    if isinstance(response, dict):
        return int(response.get("status") or 0), str(response.get("body") or "")
    status = getattr(response, "status_code", None)
    if status is None:
        status = getattr(response, "status", 0)
    text = getattr(response, "text", "")
    if callable(text):  # aiohttp-style
        text = ""
    return int(status or 0), str(text or "")


def is_blocked(response: Any, vendor: str | None = None) -> bool:
    """True when the response is an anti-bot challenge (optionally of one vendor)."""
    status, body = _status_body(response)
    found = detect_vendor(status, body)
    return found is not None if vendor in (None, "auto") else found == vendor


def degraded_status_message(skipped: int | None = None, total: int | None = None) -> str:
    """The standard status message for runs whose site protection cannot be solved right now."""
    if skipped is not None and total is not None:
        return f"{DEGRADED_STATUS_MESSAGE} ({skipped} of {total} items skipped)"
    return DEGRADED_STATUS_MESSAGE


# ─── status ──────────────────────────────────────────────────────────────────

_status_cache: dict[str, tuple[float, dict]] = {}
_status_lock = threading.Lock()


def _unknown_status(code: str | None = None) -> dict:
    # Without the platform gateway nothing is known: let the Actor proceed as before.
    return {"status": "unknown", "usable": code is None, "code": code, "successRate30m": None, "since": None, "recheckAfterSecs": 300}


def _status_from_answer(vendor: str, status: int, body: str) -> dict:
    if status == 200:
        data = _json(body)
        v = (data.get("vendors") or {}).get(vendor)
        if isinstance(v, dict):
            return v
    return _unknown_status()


def _cached(vendor: str) -> dict | None:
    with _status_lock:
        hit = _status_cache.get(vendor)
    if hit and time.monotonic() - hit[0] < _STATUS_TTL_SECS:
        return hit[1]
    return None


def _store(vendor: str, value: dict) -> dict:
    with _status_lock:
        _status_cache[vendor] = (time.monotonic(), value)
    return value


def solver_status(vendor: str, timeout: float = 10.0) -> dict:
    """Vendor status from the platform (free, cached for 15 s). Never raises.

    Returns a dict with ``status`` (healthy, degraded, down, unsupported or
    unknown), ``usable`` (False means: take your fallback or fail the items
    that need a solve), ``code``, ``successRate30m``, ``since`` and
    ``recheckAfterSecs``.
    """
    vendor = (vendor or "").strip().lower()
    hit = _cached(vendor)
    if hit is not None:
        return hit
    gw = _gateway()
    if gw is None:
        return _unknown_status(None if solver_env() else "solver_not_enabled")
    base, key = gw
    try:
        r = httpx.get(f"{base}/status", params={"vendors": vendor}, headers={"authorization": f"Bearer {key}"}, timeout=timeout)
        return _store(vendor, _status_from_answer(vendor, r.status_code, r.text))
    except Exception:  # noqa: BLE001 - a failed check must never stop the run
        return _unknown_status()


async def solver_status_async(vendor: str, timeout: float = 10.0) -> dict:
    """Async ``solver_status``."""
    vendor = (vendor or "").strip().lower()
    hit = _cached(vendor)
    if hit is not None:
        return hit
    gw = _gateway()
    if gw is None:
        return _unknown_status(None if solver_env() else "solver_not_enabled")
    base, key = gw
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(f"{base}/status", params={"vendors": vendor}, headers={"authorization": f"Bearer {key}"})
        return _store(vendor, _status_from_answer(vendor, r.status_code, r.text))
    except Exception:  # noqa: BLE001
        return _unknown_status()


# ─── solving ─────────────────────────────────────────────────────────────────


@dataclass
class Clearance:
    """A solved challenge: cookies already set on the session, and the verification response."""

    vendor: str
    solve_id: str | None
    cookies: list[dict] = field(default_factory=list)
    ttl_hint_secs: int = 0
    cached: bool = False
    verified: bool | None = None
    response: Any = None


def _is_httpx(session: Any) -> bool:
    return type(session).__module__.split(".")[0] == "httpx"


def _cookie_applies(cookie: Any, host: str, path: str, secure: bool, now: float) -> bool:
    """RFC 6265 matching: the cookies the session itself would send to this URL."""
    domain = (getattr(cookie, "domain", "") or "").lower()
    if not domain:
        return False
    bare = domain.lstrip(".")
    host_only = not domain.startswith(".") and not getattr(cookie, "domain_specified", False)
    if host_only:
        if host != bare:
            return False
    elif host != bare and not host.endswith("." + bare):
        return False
    cpath = getattr(cookie, "path", "/") or "/"
    if not (path == cpath or path.startswith(cpath.rstrip("/") + "/") or cpath == "/"):
        return False
    if getattr(cookie, "secure", False) and not secure:
        return False
    expires = getattr(cookie, "expires", None)
    return not (expires is not None and expires <= now)


def _cookie_header(session: Any, url: str) -> str | None:
    """The Cookie header the session would send to `url` (host, path, secure and expiry rules)."""
    u = httpx.URL(url)
    host = (u.host or "").lower()
    path = u.path or "/"
    secure = u.scheme == "https"
    jar = getattr(getattr(session, "cookies", None), "jar", None)
    pairs: list[str] = []
    now = time.time()
    try:
        for c in jar or []:
            if _cookie_applies(c, host, path, secure, now):
                pairs.append(f"{c.name}={c.value}")
    except Exception:  # noqa: BLE001
        return None
    return "; ".join(pairs) or None


def _set_cookies(session: Any, cookies: list[dict]) -> None:
    jar = getattr(session, "cookies", None)
    if jar is None:
        return
    for c in cookies:
        try:
            jar.set(c["name"], c["value"], domain=c.get("domain") or "", path=c.get("path") or "/")
        except Exception:  # noqa: BLE001 - a jar without domain support
            jar.set(c["name"], c["value"])


def _step_allowed(step_url: str, target_url: str) -> bool:
    """A step may only go to the target page's own host or to captcha-delivery.com, over https."""
    try:
        step = httpx.URL(step_url)
        target = httpx.URL(target_url)
    except Exception:  # noqa: BLE001
        return False
    if step.scheme != "https":
        return False
    host = (step.host or "").lower()
    if host == (target.host or "").lower():
        return True
    return host == "captcha-delivery.com" or host.endswith(".captcha-delivery.com")


def _checked(req: dict, target_url: str) -> dict:
    if not isinstance(req, dict) or not _step_allowed(str(req.get("url") or ""), target_url):
        raise SolveAttemptError("the solver asked for a request outside the target site; refused")
    if req.get("method") not in ("GET", "POST"):
        raise SolveAttemptError("the solver asked for an unsupported request method; refused")
    return req


# Session libraries whose responses can be streamed (httpx: .stream(); the others: stream=True).
_STREAM_LIBS = ("requests", "curl_cffi")


def _session_lib(session: Any) -> str:
    return type(session).__module__.split(".")[0]


def _capped_text(buf: bytearray, encoding: str | None) -> str:
    return bytes(buf[: _MAX_BODY + 1]).decode(encoding or "utf-8", errors="replace")


def _send_sync(session: Any, req: dict, timeout: float) -> tuple[int, str]:
    body = req.get("body")
    kwargs: dict[str, Any] = {"headers": req.get("headers") or {}, "timeout": timeout}
    if body is not None:
        kwargs["content" if _is_httpx(session) else "data"] = body.encode() if isinstance(body, str) else body
    # A step answer is read up to _MAX_BODY + 1 bytes: the target site decides its size.
    lib = _session_lib(session)
    if lib == "httpx":
        with session.stream(req["method"], req["url"], **kwargs) as r:
            buf = bytearray()
            for chunk in r.iter_bytes():
                buf += chunk
                if len(buf) > _MAX_BODY:
                    break
            return int(r.status_code), _capped_text(buf, r.encoding)
    if lib in _STREAM_LIBS:
        r = session.request(req["method"], req["url"], stream=True, **kwargs)
        try:
            buf = bytearray()
            for chunk in r.iter_content(65536):
                buf += chunk
                if len(buf) > _MAX_BODY:
                    break
            return int(r.status_code), _capped_text(buf, r.encoding)
        finally:
            r.close()
    r = session.request(req["method"], req["url"], **kwargs)
    return _status_body(r)


async def _send_async(session: Any, req: dict, timeout: float) -> tuple[int, str]:
    body = req.get("body")
    kwargs: dict[str, Any] = {"headers": req.get("headers") or {}, "timeout": timeout}
    if body is not None:
        kwargs["content" if _is_httpx(session) else "data"] = body.encode() if isinstance(body, str) else body
    lib = _session_lib(session)
    if lib == "httpx":
        async with session.stream(req["method"], req["url"], **kwargs) as r:
            buf = bytearray()
            async for chunk in r.aiter_bytes():
                buf += chunk
                if len(buf) > _MAX_BODY:
                    break
            return int(r.status_code), _capped_text(buf, r.encoding)
    if lib == "curl_cffi":
        r = await session.request(req["method"], req["url"], stream=True, **kwargs)
        try:
            buf = bytearray()
            async for chunk in r.aiter_content():
                buf += chunk
                if len(buf) > _MAX_BODY:
                    break
            return int(r.status_code), _capped_text(buf, r.encoding)
        finally:
            await r.aclose()
    r = await session.request(req["method"], req["url"], **kwargs)
    return _status_body(r)


class Solver:
    """Solves anti-bot challenges on the Actor's own HTTP session through the platform.

    Args:
        vendor: ``"datadome"``, ``"akamai-sbsd"`` or ``"auto"`` (detected from the blocked response).
        identity: the browser profile your TLS impersonation claims (``"chrome-146-macos"``
            matches curl_cffi ``impersonate="chrome146"``).
        accept_language / timezone / languages: locale of that browser (match the site and proxy country).
        fetch: optional ``fetch(session, request) -> (status, body)`` to send the step requests
            yourself (sync), or ``afetch`` for async.
        request_timeout: seconds per step request on your session.
    """

    def __init__(
        self,
        vendor: str = "auto",
        identity: str = "chrome-146-macos",
        accept_language: str | None = None,
        timezone: str | None = None,
        languages: str | None = None,
        timeout_ms: int = 60_000,
        request_timeout: float = 30.0,
        fetch: Callable[[Any, dict], tuple[int, str]] | None = None,
        afetch: Callable[[Any, dict], Awaitable[tuple[int, str]]] | None = None,
    ) -> None:
        self.vendor = vendor
        self.identity = {"profile": identity}
        if accept_language:
            self.identity["acceptLanguage"] = accept_language
        if timezone:
            self.identity["timezone"] = timezone
        if languages:
            self.identity["languages"] = languages
        self.timeout_ms = timeout_ms
        self.request_timeout = request_timeout
        self._fetch = fetch
        self._afetch = afetch
        self.solves = 0
        self.cleared = 0

    @staticmethod
    def configured() -> bool:
        """True when the platform solver API is available to this run."""
        return _gateway() is not None

    def is_blocked(self, response: Any) -> bool:
        return is_blocked(response, None if self.vendor == "auto" else self.vendor)

    # -- wire ------------------------------------------------------------------

    def _start_payload(self, url: str, blocked: Any, session: Any, session_id: str | None) -> dict:
        status, body = _status_body(blocked)
        payload: dict[str, Any] = {
            "vendor": self.vendor,
            "url": url,
            "blocked": {"status": status, "body": body[:_MAX_BODY]},
            "identity": self.identity,
            "timeoutMs": self.timeout_ms,
            "session": {"cookieHeader": _cookie_header(session, url)},
        }
        if session_id:
            payload["session"]["id"] = str(session_id)
        return payload

    @staticmethod
    def _handle(status: int, text: str, headers: Any, vendor: str) -> dict:
        data = _json(text)
        if status == 503 and str(data.get("code") or "") in UNAVAILABLE_CODES:
            raise VendorUnavailable(str(data["code"]), data.get("vendor") or vendor, data.get("retryAfterSecs"))
        if status == 429:
            ra = headers.get("retry-after") if headers is not None else None
            raise SolverRateLimited(float(ra) if ra else data.get("retryAfterSecs"))
        if status in (401, 403):
            if data.get("code") == "solver_not_enabled":
                raise VendorUnavailable("solver_not_enabled", vendor, None, "the anti-bot solver is not enabled for this Actor")
            raise SolverAuthError(f"solver refused the run token (HTTP {status}, {data.get('code') or ''})")
        if status == 404 and data.get("code") == "not_found":
            raise SolveAttemptError("the solve expired; start again")
        if status >= 400:
            raise SolveAttemptError(f"solver HTTP {status}: {text[:160]}")
        return data

    @staticmethod
    def _raise_failed(data: dict) -> None:
        reason = str(data.get("reason") or "rejected")
        message = str(data.get("message") or reason)
        if reason == "ip_banned":
            raise IpBanned(message)
        if reason in ("no_challenge", "bad_request") and not data.get("retryable", True):
            raise SolverRequestInvalid(message)
        raise SolveAttemptError(message)

    def _finish(self, session: Any, data: dict) -> Clearance:
        cookies = data.get("cookies") or []
        _set_cookies(session, cookies)
        self.cleared += 1
        return Clearance(
            vendor=self.vendor,
            solve_id=data.get("solveId"),
            cookies=cookies,
            ttl_hint_secs=int(data.get("ttlHintSecs") or 0),
            cached=bool(data.get("cached")),
        )

    # -- sync ------------------------------------------------------------------

    def solve(self, session: Any, url: str, blocked: Any, *, session_id: str | None = None, verify: Any = True) -> Clearance:
        """Solve the challenge in `blocked` (a response of `session` for `url`).

        verify: True re-fetches `url` and checks it is no longer blocked (the
        result is reported to the platform); a callable ``verify(session) ->
        bool`` does your own check; False skips it (call ``report()`` later).
        Raises SolveAttemptError / IpBanned (try again on a fresh exit),
        VendorUnavailable (fail fast), SolverRateLimited, SolverAuthError.
        """
        gw = _gateway()
        if gw is None:
            raise VendorUnavailable("solver_not_enabled", self.vendor, None, "the platform solver is not available to this run")
        base, key = gw
        headers = {"authorization": f"Bearer {key}"}
        self.solves += 1
        with httpx.Client(timeout=self.timeout_ms / 1000 + 30) as api:
            r = api.post(f"{base}/solve", json=self._start_payload(url, blocked, session, session_id), headers=headers)
            data = self._handle(r.status_code, r.text, r.headers, self.vendor)
            for _ in range(8):
                action = data.get("action")
                if action == "done":
                    clearance = self._finish(session, data)
                    break
                if action == "failed":
                    self._raise_failed(data)
                if action not in ("fetch", "submit"):
                    raise SolveAttemptError(f"unexpected solver answer: {action}")
                results = []
                for req in data.get("requests") or []:
                    req = _checked(req, url)
                    try:
                        status, body = (self._fetch or (lambda s, q: _send_sync(s, q, self.request_timeout)))(session, req)
                    except Exception as exc:  # noqa: BLE001
                        raise SolveAttemptError(f"challenge request failed: {type(exc).__name__}") from exc
                    results.append({"id": req["id"], "status": status, "body": body[:_MAX_BODY]})
                r = api.post(
                    f"{base}/solve",
                    json={"solveId": data.get("solveId"), "results": results, "session": {"id": session_id, "cookieHeader": _cookie_header(session, url)}},
                    headers=headers,
                )
                data = self._handle(r.status_code, r.text, r.headers, self.vendor)
            else:
                raise SolveAttemptError("the solve did not finish")
            if verify is False:
                return clearance
            ok, response = self._verify_sync(session, url, verify)
            clearance.verified = ok
            clearance.response = response
            self._report_sync(api, base, headers, clearance.solve_id, ok)
        if not ok:
            raise SolveAttemptError("still blocked after the clearance was applied")
        return clearance

    def _verify_sync(self, session: Any, url: str, verify: Any) -> tuple[bool, Any]:
        if callable(verify):
            return bool(verify(session)), None
        try:
            r = session.get(url, timeout=self.request_timeout)
        except Exception:  # noqa: BLE001
            return False, None
        return not is_blocked(r), r

    @staticmethod
    def _report_sync(api: httpx.Client, base: str, headers: dict, solve_id: str | None, ok: bool) -> None:
        if not solve_id:
            return
        try:
            api.post(f"{base}/solves/{solve_id}/outcome", json={"verified": ok}, headers=headers)
        except Exception:  # noqa: BLE001 - reporting is best effort
            pass

    def report(self, clearance: Clearance, verified: bool) -> None:
        """Report your own verification of a clearance (after ``solve(..., verify=False)``)."""
        gw = _gateway()
        if gw is None or not clearance.solve_id:
            return
        base, key = gw
        clearance.verified = verified
        with httpx.Client(timeout=15) as api:
            self._report_sync(api, base, {"authorization": f"Bearer {key}"}, clearance.solve_id, verified)

    # -- async -----------------------------------------------------------------

    async def solve_async(self, session: Any, url: str, blocked: Any, *, session_id: str | None = None, verify: Any = True) -> Clearance:
        """Async ``solve`` (curl_cffi AsyncSession, httpx.AsyncClient, or `afetch`)."""
        gw = _gateway()
        if gw is None:
            raise VendorUnavailable("solver_not_enabled", self.vendor, None, "the platform solver is not available to this run")
        base, key = gw
        headers = {"authorization": f"Bearer {key}"}
        self.solves += 1
        async with httpx.AsyncClient(timeout=self.timeout_ms / 1000 + 30) as api:
            r = await api.post(f"{base}/solve", json=self._start_payload(url, blocked, session, session_id), headers=headers)
            data = self._handle(r.status_code, r.text, r.headers, self.vendor)
            for _ in range(8):
                action = data.get("action")
                if action == "done":
                    clearance = self._finish(session, data)
                    break
                if action == "failed":
                    self._raise_failed(data)
                if action not in ("fetch", "submit"):
                    raise SolveAttemptError(f"unexpected solver answer: {action}")
                results = []
                for req in data.get("requests") or []:
                    req = _checked(req, url)
                    try:
                        if self._afetch is not None:
                            status, body = await self._afetch(session, req)
                        else:
                            status, body = await _send_async(session, req, self.request_timeout)
                    except Exception as exc:  # noqa: BLE001
                        raise SolveAttemptError(f"challenge request failed: {type(exc).__name__}") from exc
                    results.append({"id": req["id"], "status": status, "body": body[:_MAX_BODY]})
                r = await api.post(
                    f"{base}/solve",
                    json={"solveId": data.get("solveId"), "results": results, "session": {"id": session_id, "cookieHeader": _cookie_header(session, url)}},
                    headers=headers,
                )
                data = self._handle(r.status_code, r.text, r.headers, self.vendor)
            else:
                raise SolveAttemptError("the solve did not finish")
            if verify is False:
                return clearance
            if callable(verify):
                result = verify(session)
                ok = bool(await result) if inspect.isawaitable(result) else bool(result)
                response = None
            else:
                try:
                    response = await session.get(url, timeout=self.request_timeout)
                    ok = not is_blocked(response)
                except Exception:  # noqa: BLE001
                    ok, response = False, None
            clearance.verified = ok
            clearance.response = response
            if clearance.solve_id:
                try:
                    await api.post(f"{base}/solves/{clearance.solve_id}/outcome", json={"verified": ok}, headers=headers)
                except Exception:  # noqa: BLE001
                    pass
        if not ok:
            raise SolveAttemptError("still blocked after the clearance was applied")
        return clearance


# ─── Akamai Bot Manager on your own session (sensor WebSocket lane + SBSD) ──


_SCRIPT_SRC_RE = re.compile(r"<script[^>]+src=[\"']([^\"']+)[\"']", re.I)
_SBSD_NONCE_RE = re.compile(r"(?:^|&)v=[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?:&|$)", re.I)
_PIXEL_PATH_RE = re.compile(r"^/akam/\d+/")
_LONG_PATH_RE = re.compile(r"<script[^>]+src=[\"']((?:/[a-zA-Z0-9\-_]+){5,10}(?:\?v=[^\"']*)?)[\"']", re.I)
_AKAM_PIXEL_TAG_RE = re.compile(r"akam/13.*?top:\s?-999px.*?src=\"(.*?)\"", re.S)
_AKAM_KEYWORD_RE = re.compile(r"<script[^>]+src=[\"']([^\"']*/(?:akam|_abck|bm-)[^\"']+)[\"']", re.I)
_STRIP_SCRIPTS_RE = re.compile(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", re.I)
# Headers the submission must carry from YOUR client (its fingerprint), never the solver's copy.
_OWN_HEADERS = {"user-agent", "cookie", "host", "content-length", "accept-language", "accept-encoding", "connection",
                "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform"}


def _urljoin(base: str, ref: str) -> str:
    from urllib.parse import urljoin

    return urljoin(base, ref)


def _is_sbsd_bundle(url: str) -> bool:
    u = httpx.URL(url)
    return u.path.startswith("/.well-known/sbsd") or bool(_SBSD_NONCE_RE.search(u.query.decode() if isinstance(u.query, bytes) else str(u.query or "")))


def _is_sensor_candidate(url: str) -> bool:
    return not _is_sbsd_bundle(url) and not _PIXEL_PATH_RE.match(httpx.URL(url).path)


def _sbsd_src(html: str, base_url: str) -> tuple[str, str] | None:
    for m in _SCRIPT_SRC_RE.finditer(html or ""):
        u = _urljoin(base_url, m.group(1))
        if _is_sbsd_bundle(u):
            return m.group(1), u
    return None


def find_sbsd_bundle(html: str, base_url: str) -> str | None:
    """URL of the SBSD bundle script a page loads (``src`` with a UUID ``v=``, or ``/.well-known/sbsd``), or None."""
    found = _sbsd_src(html, base_url)
    return found[1] if found else None


def find_sensor_script(html: str, base_url: str) -> str | None:
    """URL of the Akamai ``_abck`` sensor script in a page, or None (the provider examples' three patterns)."""
    html = html or ""
    m = _AKAM_PIXEL_TAG_RE.search(html)
    if m:
        u = _urljoin(base_url, m.group(1))
        if _is_sensor_candidate(u):
            return u
    for m in _LONG_PATH_RE.finditer(html):
        u = _urljoin(base_url, m.group(1))
        if _is_sensor_candidate(u):
            return u
    m = _AKAM_KEYWORD_RE.search(html)
    if m:
        u = _urljoin(base_url, m.group(1))
        if _is_sensor_candidate(u):
            return u
    return None


_PIXEL_COLLECTOR_RE = re.compile(r"<script[^>]+src=[\"']([^\"']*/akam/\d+/[0-9a-f]{6,})[\"']", re.I)


def find_pixel_collector(html: str, base_url: str) -> str | None:
    """URL of Akamai's pixel collector script (``/akam/<n>/<hex>``, it POSTs to ``/akam/<n>/pixel_<hex>``), or None."""
    m = _PIXEL_COLLECTOR_RE.search(html or "")
    return _urljoin(base_url, m.group(1)) if m else None


def pixel_target(collector_url: str) -> str:
    """The endpoint a pixel collector POSTs to: its own directory, ``pixel_`` + its basename, no query."""
    u = httpx.URL(collector_url)
    head, _, name = u.path.rpartition("/")
    return f"{u.scheme}://{u.host}{head}/pixel_{name}"


def abck_state(value: str | None) -> str | None:
    """The state field of an ``_abck`` cookie: ``'0'`` accepted, ``'-1'`` keep going, None when absent."""
    parts = (value or "").split("~")
    return parts[1] if len(parts) > 1 else None


def _cookie_dict(session: Any) -> dict[str, str]:
    jar = getattr(session, "cookies", None)
    if jar is None:
        return {}
    try:
        if hasattr(jar, "jar"):
            return {c.name: c.value for c in jar.jar}
        return dict(jar.items())
    except Exception:  # noqa: BLE001
        return {}


def _ws_connect(url: str, headers: dict[str, str]) -> Any:
    """websockets.connect with request headers (``additional_headers`` from 13, ``extra_headers`` before)."""
    import websockets

    try:
        return websockets.connect(url, additional_headers=headers, max_size=32 * 1024 * 1024, open_timeout=30)
    except TypeError:
        return websockets.connect(url, extra_headers=headers, max_size=32 * 1024 * 1024, open_timeout=30)


def _refused_upgrade(exc: BaseException) -> tuple[int, str, str | None]:
    """(status, body, Retry-After) of a refused WebSocket upgrade, across websockets versions."""
    response = getattr(exc, "response", None)
    if response is not None:
        status = int(getattr(response, "status_code", 0) or 0)
        raw = getattr(response, "body", b"") or b""
        headers = getattr(response, "headers", None)
        return status, raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw), headers.get("retry-after") if headers is not None else None
    status = int(getattr(exc, "status_code", 0) or 0)
    headers = getattr(exc, "headers", None)
    return status, "", headers.get("retry-after") if headers is not None else None


def _is_scrapely_http(session: Any) -> bool:
    return type(session).__module__.startswith("scrapely.http")


async def _session_send(session: Any, method: str, url: str, *, headers: dict, body: str | bytes | None, kind: str, timeout: float) -> tuple[int, str]:
    kwargs: dict[str, Any] = {"headers": headers, "timeout": timeout}
    if body is not None:
        data = body.encode() if isinstance(body, str) else body
        kwargs["content" if (_is_httpx(session) or _is_scrapely_http(session)) else "data"] = data
    if _is_scrapely_http(session):
        # One identity for the whole solve: no rotation, and a 403 is an answer, not a block to route around.
        kwargs.update(kind=kind, max_rotations=0, is_blocked=lambda r: None)
    r = await session.request(method, url, **kwargs)
    return _status_body(r)


@dataclass
class SensorResult:
    """Outcome of ``AkamaiSensor.solve_abck``: whether the provider reported ``_abck`` accepted."""

    accepted: bool
    rounds: int = 0
    submissions: int = 0
    abck: str | None = None
    error: str | None = None


class AkamaiSensor:
    """Akamai Bot Manager solved on YOUR HTTP session, the way the provider's own examples do it.

    Two independent channels, both answered through the session you pass, so
    every request carries your client's fingerprint, exit IP and cookie jar:

    * ``solve_sbsd(session, url, html)``: the page loads an SBSD bundle (a
      script whose ``src`` has a UUID ``v=``, often a ~440-byte "Access Denied"
      bootstrap). The bundle is fetched on your session, the provider computes
      the ledger (``/akamai/sbsd/generate-session``), and its rows are POSTed to
      the bundle path on your session.
    * ``solve_abck(session, url, html)``: the classic ``_abck`` sensor over the
      provider's stateful WebSocket (``/akamai/session``, relayed by the
      platform for runs). The provider generates each sensor; it is POSTed on
      your session; rounds continue until ``_abck`` is ``~0~``.

    ``solve(session, url, html)`` does SBSD when the page carries a bundle, then ``_abck``.

    Use a session that keeps its identity on a block (``http.session(keep_identity=True)``
    of ``scrapely.http``, or one curl_cffi / httpx client): the solve, the
    retries and the page fetches must share one identity, jar and exit. The
    ``profile_id`` must be the browser your client claims (``chrome-154-macos``
    for scrapely.http Chrome 154 on macOS, ``chrome-146-macos`` for curl_cffi
    ``impersonate="chrome146"``); a mismatch shows as ``_abck`` stuck at ``~-1~``.

    Example::

        http = AsyncClient(proxy_configuration=proxy, fingerprint_options=FingerprintOptions(
            browsers=[{"name": "chrome", "min_version": 154, "max_version": 154}], operating_systems=["macos"]))
        session = http.session(keep_identity=True)
        akamai = AkamaiSensor(profile_id="chrome-154-macos")
        home = await session.get("https://www.example.com/")
        result = await akamai.solve(session, "https://www.example.com/", home.text)
        page = await session.get("https://www.example.com/item/1")
    """

    def __init__(
        self,
        profile_id: str = "chrome-154-macos",
        *,
        telemetry: dict | None = None,
        timeout: float = 90.0,
        request_timeout: float = 30.0,
        max_rounds: int = 12,
    ) -> None:
        self.profile_id = profile_id
        self.telemetry = telemetry
        self.timeout = timeout
        self.request_timeout = request_timeout
        self.max_rounds = max_rounds
        self.sessions = 0
        self.accepted = 0

    @staticmethod
    def _endpoint() -> tuple[str, str]:
        env = solver_env()
        if env is None:
            raise VendorUnavailable("solver_not_enabled", "akamai", None, "the platform solver is not available to this run")
        return env

    # -- SBSD --------------------------------------------------------------------

    async def solve_sbsd(self, session: Any, url: str, html: str) -> bool:
        """Answers the SBSD bundle of `html` (the page `url` as your session got it). False when there is none."""
        found = _sbsd_src(html, url)
        if not found:
            return False
        src_attr, bundle_url = found
        base, key = self._endpoint()
        origin = f"{httpx.URL(url).scheme}://{httpx.URL(url).host}"
        _, source = await _session_send(session, "GET", bundle_url, headers={"referer": url, "sec-fetch-dest": "script", "sec-fetch-mode": "no-cors", "sec-fetch-site": "same-origin", "accept": "*/*"}, body=None, kind="xhr", timeout=self.request_timeout)
        cookie_header = "; ".join(f"{k}={v}" for k, v in _cookie_dict(session).items())
        async with httpx.AsyncClient(timeout=self.timeout) as api:
            r = await api.post(
                f"{base}/akamai/sbsd/generate-session",
                headers={"x-api-key": key},
                json={"bundle": {"scriptSrc": src_attr, "source": source}, "document": {"url": url, "html": html, "cookieHeader": cookie_header}, "profile": {"id": self.profile_id}},
            )
        check_solver_response(r.status_code, r.text, r.headers.get("retry-after"))
        data = _json(r.text)
        rows = sorted([s for s in data.get("submissions") or [] if isinstance(s, dict) and s.get("resolvedUrl")], key=lambda s: s.get("index", 0))
        if not rows:
            raise SolveAttemptError(f"the SBSD ledger had no rows ({str(data.get('error') or data.get('outcome') or 'no reason')[:120]})")
        for row in rows:
            target = _urljoin(url, row["resolvedUrl"])
            if not _step_allowed(target, url):
                raise SolveAttemptError("the SBSD ledger pointed outside the target site; refused")
            await _session_send(session, "POST", target, headers={"content-type": "text/plain;charset=UTF-8", "origin": origin, "referer": url, "sec-fetch-dest": "empty", "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin", "accept": "*/*"}, body=row.get("body") or "", kind="xhr", timeout=self.request_timeout)
        return True

    # -- pixel channel (/akam/<n>/<hex> collector) -----------------------------------

    async def solve_pixel(self, session: Any, url: str, html: str) -> bool:
        """Answers Akamai's pixel channel of `html`: the collector script is fetched on your session, the solver
        writes the POST the collector would send (``/akamai/pixel/generate``), and it is POSTed to the collector's
        ``pixel_`` endpoint on your session. False when the page has no collector."""
        collector = find_pixel_collector(html, url)
        if not collector:
            return False
        base, key = self._endpoint()
        origin = f"{httpx.URL(url).scheme}://{httpx.URL(url).host}"
        _, source = await _session_send(session, "GET", collector, headers={"referer": url, "sec-fetch-dest": "script", "sec-fetch-mode": "no-cors", "sec-fetch-site": "same-origin", "accept": "*/*"}, body=None, kind="xhr", timeout=self.request_timeout)
        cookie_header = "; ".join(f"{k}={v}" for k, v in _cookie_dict(session).items())
        async with httpx.AsyncClient(timeout=self.timeout) as api:
            r = await api.post(
                f"{base}/akamai/pixel/generate",
                headers={"x-api-key": key, "accept": "application/json"},
                json={"document": {"url": url, "html": html, **({"cookieHeader": cookie_header} if cookie_header else {})},
                      "profile": {"id": self.profile_id}, "script": {"source": source, "url": collector}},
            )
        check_solver_response(r.status_code, r.text, r.headers.get("retry-after"))
        data = _json(r.text)
        request = data.get("request") if isinstance(data.get("request"), dict) else {}
        if not data.get("ok") or not isinstance(request.get("body"), str):
            err = data.get("error") if isinstance(data.get("error"), dict) else {}
            raise SolveAttemptError(f"the pixel route refused ({err.get('code') or r.status_code})")
        # The collector's own endpoint, not the solver's reconstruction of it (the provider examples do the same).
        target = pixel_target(collector)
        await _session_send(session, "POST", target, headers={"content-type": request.get("contentType") or "application/x-www-form-urlencoded", "origin": origin, "referer": url, "sec-fetch-dest": "empty", "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin", "accept": "*/*"}, body=request["body"], kind="xhr", timeout=self.request_timeout)
        return True

    # -- _abck sensor (WebSocket) ---------------------------------------------------

    async def solve_abck(self, session: Any, url: str, html: str, *, script_url: str | None = None) -> SensorResult:
        """Runs the ``_abck`` sensor rounds for the page `url` (its `html` as your session got it)."""
        import websockets

        script_url = script_url or find_sensor_script(html, url)
        if not script_url:
            return SensorResult(accepted=False, error="no Akamai sensor script in the page")
        base, key = self._endpoint()
        origin = f"{httpx.URL(url).scheme}://{httpx.URL(url).host}"
        _, script = await _session_send(session, "GET", script_url, headers={"referer": url, "sec-fetch-dest": "script", "sec-fetch-mode": "no-cors", "sec-fetch-site": "same-origin", "accept": "*/*"}, body=None, kind="xhr", timeout=self.request_timeout)
        init: dict[str, Any] = {
            "type": "init", "url": url, "script": script, "scriptUrl": script_url, "html": _STRIP_SCRIPTS_RE.sub("", html or ""),
            "cookies": _cookie_dict(session), "profileId": self.profile_id,
        }
        if self.telemetry:
            init["profile"] = self.telemetry
        ws_url = re.sub(r"^http", "ws", base, flags=re.I) + "/akamai/session"
        result = SensorResult(accepted=False)
        self.sessions += 1
        deadline = time.monotonic() + self.timeout
        try:
            async with _ws_connect(ws_url, {"x-api-key": key}) as ws:
                await ws.send(json.dumps(init))
                while True:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        result.error = f"not accepted within {self.timeout:g}s"
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=left)
                    except asyncio.TimeoutError:
                        result.error = f"not accepted within {self.timeout:g}s"
                        break
                    msg = _json(raw if isinstance(raw, str) else raw.decode("utf-8", "replace"))
                    kind = msg.get("type")
                    if kind == "submission":
                        result.submissions += 1
                        if result.submissions > self.max_rounds:
                            result.error = f"not accepted after {self.max_rounds} rounds"
                            break
                        target = str(msg.get("url") or "")
                        headers = {k: v for k, v in (msg.get("headers") or {}).items() if k.lower() not in _OWN_HEADERS}
                        headers.setdefault("origin", origin)
                        headers.setdefault("referer", url)
                        if not _step_allowed(target, url):
                            await ws.send(json.dumps({"type": "submission_response", "id": msg.get("id"), "status": 0, "body": "Client error: refused a submission outside the target site"}))
                            continue
                        try:
                            status, body = await _session_send(session, str(msg.get("method") or "POST"), target, headers=headers, body=msg.get("body") or "", kind="xhr", timeout=self.request_timeout)
                        except Exception as exc:  # noqa: BLE001
                            status, body = 0, f"Client error: {type(exc).__name__}"
                        await ws.send(json.dumps({"type": "submission_response", "id": msg.get("id"), "status": status, "body": body, "cookies": _cookie_dict(session)}))
                    elif kind == "cookie_update":
                        result.rounds = int(msg.get("round") or result.rounds + 1)
                        result.abck = abck_state((msg.get("cookies") or {}).get("_abck")) or result.abck
                        if msg.get("accepted"):
                            result.accepted = True
                            break
                    elif kind == "status" and str(msg.get("state") or "") == "accepted":
                        result.accepted = True
                        break
                    elif kind == "error":
                        result.error = str(msg.get("message") or msg.get("code") or "solver error")[:300]
                        if msg.get("code") == "invalid_request":
                            raise SolverRequestInvalid(result.error)
        except websockets.exceptions.InvalidHandshake as exc:  # a refused upgrade carries the gateway's JSON error
            status, body, retry_after = _refused_upgrade(exc)
            if status:
                check_solver_response(status, body, retry_after)
            raise SolveAttemptError(f"solver session refused ({status or type(exc).__name__})") from exc
        except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as exc:
            if not result.accepted:
                result.error = result.error or f"solver session failed: {type(exc).__name__}"
        result.abck = abck_state(_cookie_dict(session).get("_abck")) or result.abck
        if result.accepted:
            self.accepted += 1
        return result

    async def solve(self, session: Any, url: str, html: str) -> SensorResult:
        """SBSD when the page carries a bundle, then the ``_abck`` sensor (on the page as it is after SBSD)."""
        if find_sbsd_bundle(html, url):
            await self.solve_sbsd(session, url, html)
            if not find_sensor_script(html, url) or len(html) < 5000:
                _, html = await _session_send(session, "GET", url, headers={"referer": url}, body=None, kind="navigate", timeout=self.request_timeout)
        return await self.solve_abck(session, url, html)
