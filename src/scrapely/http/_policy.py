"""Retry, rotation and backoff decisions, per-host circuit breaker, identities.

Pure logic with no I/O, shared by the async and the sync client and unit
tested with a fake clock.
"""

from __future__ import annotations

import random
import re
import string
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Optional

from ._detect import PERIMETERX, RATE_LIMIT, is_retryable_status
from ._profiles import BrowserProfile


@dataclass
class BackoffPolicy:
    """Opt-in politeness (``polite=True`` or ``backoff=BackoffPolicy(...)``).

    The default client never waits: a block is tied to the identity that got
    it, so it rotates to a fresh identity at once. Actors that must be gentle
    with a site can turn these back on.

    Attributes:
        honour_retry_after: wait for a 429's ``Retry-After`` (capped at ``max_retry_after``).
        max_retry_after: longest Retry-After wait, in seconds.
        backoff_base / backoff_cap: per-host exponential backoff (1, 2, 4 … s) for
            429s without Retry-After, 5xx and transport errors.
        jitter: up to this many seconds are added to every wait.
        circuit_breaker: a host that blocked more than 70% of its last 20
            attempts gets one request at a time, 2–5 s apart.
        px_same_identity_retries: retries of a PerimeterX 403 on the same
            identity before rotating.
    """

    honour_retry_after: bool = True
    max_retry_after: float = 60.0
    backoff_base: float = 1.0
    backoff_cap: float = 30.0
    jitter: float = 1.0
    circuit_breaker: bool = True
    px_same_identity_retries: int = 1


@dataclass
class RetryPolicy:
    max_rotations: int = 6  # fresh identities per request after the first attempt
    # With a fixed proxy URL (or none) a new identity can't get a new exit IP:
    # a block is rotated (new profile + jar) this many times at most.
    fixed_exit_block_rotations: int = 1
    backoff: Optional[BackoffPolicy] = None


@dataclass
class Decision:
    retry: bool
    same_identity: bool = False
    delay: float = 0.0
    retire: bool = False
    reason: str = ''


class HostState:
    """Per-host counters: the opt-in backoff level and circuit breaker (last ``window`` attempts)."""

    def __init__(
        self,
        window: int = 20,
        min_samples: int = 10,
        open_ratio: float = 0.7,
        close_ratio: float = 0.4,
        breaker: bool = True,
    ) -> None:
        self.outcomes: Deque[bool] = deque(maxlen=window)
        self.min_samples = min_samples
        self.open_ratio = open_ratio
        self.close_ratio = close_ratio
        self.breaker = breaker
        self.backoff_level = 0
        self.slow = False
        self.next_at = 0.0
        self.warned = False

    def blocked_ratio(self) -> float:
        return (sum(self.outcomes) / len(self.outcomes)) if self.outcomes else 0.0

    def record(self, blocked: bool) -> bool:
        """Record one attempt; True when this attempt switched the host to slow mode."""
        self.outcomes.append(bool(blocked))
        if not self.breaker or len(self.outcomes) < self.min_samples:
            return False
        ratio = self.blocked_ratio()
        if not self.slow and ratio > self.open_ratio:
            self.slow = True
            return True
        if self.slow and ratio < self.close_ratio:
            self.slow = False
        return False

    def next_backoff(self, policy: BackoffPolicy, rng: random.Random) -> float:
        """1, 2, 4 … s (capped) plus jitter; grows per host until a success."""
        delay = min(policy.backoff_cap, policy.backoff_base * (2 ** self.backoff_level))
        self.backoff_level = min(self.backoff_level + 1, 16)
        return delay + rng.uniform(0.0, policy.jitter)

    def success(self) -> None:
        self.backoff_level = 0


def rotation_reason(status: Optional[int], blocked: Optional[str], error_kind: Optional[str]) -> str:
    """Stats key for a rotation: the block vendor/reason, the transport error kind or ``HTTP <status>``."""
    if blocked is not None:
        return blocked
    if error_kind is not None:
        return error_kind
    return f'HTTP {status}'


def decide(
    policy: RetryPolicy,
    *,
    rotations_done: int,
    status: Optional[int],
    blocked: Optional[str],
    error_kind: Optional[str],
    retry_after: Optional[float],
    same_identity_retries: int,
    host: HostState,
    rng: random.Random,
    max_rotations: Optional[int] = None,
    rotates_ip: bool = True,
) -> Decision:
    """What to do after one attempt.

    Default: any block (429, 403 block page, captcha, challenge, 511, vendor
    markers), 5xx or transport error rotates to a brand-new identity at once,
    with no wait, up to ``max_rotations`` times. Retry-After, backoff and the
    PerimeterX same-identity retry only apply with ``policy.backoff``.

    ``rotates_ip`` is False with a fixed proxy URL or no proxy: a fresh identity
    then changes the profile and jar but not the exit IP, so a block rotates
    ``fixed_exit_block_rotations`` times at most (on staging, further tries on
    the same exit were blocked too and only cost time). Errors keep the full cap.
    """
    limit = policy.max_rotations if max_rotations is None else max_rotations
    failed = blocked is not None or error_kind is not None or (status is not None and is_retryable_status(status))
    if not failed:
        host.success()
        return Decision(retry=False)
    bp = policy.backoff
    if bp is not None and blocked == PERIMETERX and status == 403 and same_identity_retries < bp.px_same_identity_retries:
        return Decision(retry=True, same_identity=True, delay=rng.uniform(0.4, 1.0), reason='perimeterx same-identity retry')
    if rotations_done >= limit:
        return Decision(retry=False, retire=True, reason='rotations exhausted')
    reason = rotation_reason(status, blocked, error_kind)
    if blocked is not None and not rotates_ip and rotations_done >= policy.fixed_exit_block_rotations:
        return Decision(retry=False, retire=True, reason=f'blocked ({blocked}) on a fixed exit IP')
    delay = 0.0
    if bp is not None:
        if blocked == RATE_LIMIT or status == 429:
            if bp.honour_retry_after and retry_after is not None:
                delay = min(retry_after, bp.max_retry_after) + rng.uniform(0.0, bp.jitter)
            else:
                delay = host.next_backoff(bp, rng)
        elif blocked is None:
            delay = host.next_backoff(bp, rng)
    return Decision(retry=True, delay=delay, retire=True, reason=reason)


# ─── identities ─────────────────────────────────────────────────────────────

_ID_CHARS = string.ascii_lowercase + string.digits


def new_session_id(rng: random.Random) -> str:
    """A proxy session id: 12 chars of [a-z0-9] (sticky proxies need ≥ 8)."""
    return 'h' + ''.join(rng.choice(_ID_CHARS) for _ in range(11))


@dataclass(eq=False)
class Identity:
    """One browser identity: profile + proxy session + cookie jar, retired together."""

    session_id: str
    profile: BrowserProfile
    proxy_url: Optional[str]
    client: Any
    accept_language: str
    created_at: float = field(default_factory=time.monotonic)
    usage: int = 0
    last_at: float = 0.0
    next_delay: float = 0.0
    retired: bool = False
    in_flight: int = 0  # requests using it right now (a session's identity is shared)
    closed: bool = False

    def expired(self, now: float, max_usage: int, max_age: float) -> bool:
        if self.retired:
            return True
        if max_usage and self.usage >= max_usage:
            return True
        if max_age and now - self.created_at >= max_age:
            return True
        return False


_COUNTRY_IN_PROXY_RE = re.compile(r'(?:^|[,_:])country-([A-Za-z]{2})(?:[_,:@]|$)')


def country_from_proxy(proxy: Any) -> Optional[str]:
    """The proxy's country: a ProxyConfiguration's country_code, or ``country-XX`` in a proxy URL."""
    if proxy is None:
        return None
    if not isinstance(proxy, str):
        for attr in ('country_code', '_country_code'):
            value = getattr(proxy, attr, None)
            if isinstance(value, str) and value:
                return value.upper()
        return None
    from urllib.parse import unquote, urlsplit

    try:
        parts = urlsplit(proxy)
        creds = f'{unquote(parts.username or "")}:{unquote(parts.password or "")}'
    except ValueError:
        return None
    m = _COUNTRY_IN_PROXY_RE.search(creds)
    return m.group(1).upper() if m else None
