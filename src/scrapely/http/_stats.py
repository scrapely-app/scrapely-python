"""Per-client and per-run counters for the HTTP client."""

from __future__ import annotations

import threading
from collections import Counter
from typing import Any, Dict


class HttpStats:
    """Counters: requests, blocks per vendor, retries, identities, bytes, per-profile ok/blocked."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests = 0  # caller-level requests
        self.attempts = 0  # wire attempts (incl. retries)
        self.ok = 0
        self.failed = 0
        self.retries = 0
        self.rotations = 0
        self.rotations_by: Counter = Counter()
        self.rotations_by_host: Counter = Counter()
        self.blocked = 0
        self.blocked_by: Counter = Counter()
        self.errors_by: Counter = Counter()
        self.status_codes: Counter = Counter()
        self.identities = 0
        self.identities_retired = 0
        self.bytes = 0
        self.profiles: Dict[str, Dict[str, int]] = {}
        self.circuit_open_hosts: set = set()

    def _profile(self, label: str) -> Dict[str, int]:
        row = self.profiles.get(label)
        if row is None:
            row = self.profiles[label] = {'ok': 0, 'blocked': 0, 'errors': 0}
        return row

    def record_attempt(self, profile: str, status: int, size: int, blocked: str | None) -> None:
        with self._lock:
            self.attempts += 1
            self.bytes += size
            self.status_codes[str(status)] += 1
            if blocked:
                self.blocked += 1
                self.blocked_by[blocked] += 1
                self._profile(profile)['blocked'] += 1
            else:
                self._profile(profile)['ok'] += 1

    def record_error(self, profile: str, kind: str) -> None:
        with self._lock:
            self.attempts += 1
            self.errors_by[kind] += 1
            self._profile(profile)['errors'] += 1

    def record_request(self, success: bool) -> None:
        with self._lock:
            self.requests += 1
            if success:
                self.ok += 1
            else:
                self.failed += 1

    def record_rotation(self, reason: str, host: str) -> None:
        with self._lock:
            self.rotations += 1
            self.rotations_by[reason] += 1
            self.rotations_by_host[host] += 1

    def record_retry(self) -> None:
        with self._lock:
            self.retries += 1

    def record_identity(self, opened: bool = True) -> None:
        with self._lock:
            if opened:
                self.identities += 1
            else:
                self.identities_retired += 1

    def record_circuit(self, host: str) -> None:
        with self._lock:
            self.circuit_open_hosts.add(host)

    def merge(self, other: 'HttpStats') -> None:
        with self._lock, other._lock:
            self.requests += other.requests
            self.attempts += other.attempts
            self.ok += other.ok
            self.failed += other.failed
            self.retries += other.retries
            self.rotations += other.rotations
            self.rotations_by.update(other.rotations_by)
            self.rotations_by_host.update(other.rotations_by_host)
            self.blocked += other.blocked
            self.blocked_by.update(other.blocked_by)
            self.errors_by.update(other.errors_by)
            self.status_codes.update(other.status_codes)
            self.identities += other.identities
            self.identities_retired += other.identities_retired
            self.bytes += other.bytes
            for label, row in other.profiles.items():
                mine = self._profile(label)
                for k, v in row.items():
                    mine[k] = mine.get(k, 0) + v
            self.circuit_open_hosts |= other.circuit_open_hosts

    def as_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'requests': self.requests,
                'attempts': self.attempts,
                'ok': self.ok,
                'failed': self.failed,
                'retries': self.retries,
                'rotations': self.rotations,
                'rotationsBy': dict(self.rotations_by),
                'rotationsByHost': dict(self.rotations_by_host),
                'blocked': self.blocked,
                'blockedBy': dict(self.blocked_by),
                'errors': dict(self.errors_by),
                'statusCodes': dict(self.status_codes),
                'identities': self.identities,
                'identitiesRetired': self.identities_retired,
                'bytes': self.bytes,
                'profiles': {k: dict(v) for k, v in sorted(self.profiles.items())},
                'slowedHosts': sorted(self.circuit_open_hosts),
            }

    def summary(self) -> str:
        """One line, e.g. ``HTTP: 120 requests, 3 blocked (cloudflare 2, 429 1), 3 rotations (cloudflare 2, 429 1), 5 identities, ...``."""
        d = self.as_dict()
        blocked = ''
        if d['blockedBy']:
            blocked = ' (' + ', '.join(f'{k} {v}' for k, v in sorted(d['blockedBy'].items(), key=lambda kv: -kv[1])) + ')'
        parts = [
            f"HTTP: {d['requests']} requests",
            f"{d['blocked']} blocked{blocked}",
            f"{d['rotations']} rotations"
            + (
                ' (' + ', '.join(f'{k} {v}' for k, v in sorted(d['rotationsBy'].items(), key=lambda kv: -kv[1])) + ')'
                if d['rotationsBy']
                else ''
            ),
            f"{d['identities']} identities",
        ]
        if d['failed']:
            parts.append(f"{d['failed']} failed")
        if d['errors']:
            parts.append('errors ' + ', '.join(f'{k} {v}' for k, v in sorted(d['errors'].items())))
        if d['profiles']:
            parts.append(
                'profiles ok/blocked '
                + ' '.join(f"{k} {v['ok']}/{v['blocked']}" for k, v in d['profiles'].items())
            )
        if d['slowedHosts']:
            parts.append('slowed hosts ' + ', '.join(d['slowedHosts']))
        return ', '.join(parts)


# Aggregate over every client of this process (one run), written as HTTP_STATS.
RUN_STATS = HttpStats()
