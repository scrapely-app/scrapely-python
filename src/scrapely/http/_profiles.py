"""The browser profile table (``profiles.json``) and profile selection.

Every profile is a coherent unit taken from the HTTP backend: the TLS and
HTTP/2 fingerprint, the User-Agent, the client hints and the header order all
describe the same browser on the same OS. The table records the backend's own
native header values; the unit tests check them against what the backend
actually sends, so a backend upgrade that changes them fails CI instead of
shipping a mismatch.
"""

from __future__ import annotations

import json
import random
import re
from dataclasses import dataclass, field
from functools import lru_cache
from importlib import resources
from typing import Any, Dict, List, Optional, Sequence, Tuple

FAMILIES = ('chrome', 'edge', 'firefox', 'safari')
OPERATING_SYSTEMS = ('windows', 'macos', 'linux', 'android', 'ios')
DEVICES = ('desktop', 'mobile')


@dataclass(frozen=True)
class BrowserProfile:
    """One impersonation profile: browser family + version + OS, backed by a backend profile."""

    id: str
    family: str
    version: int
    version_label: str
    os: str
    device: str
    wreq_profile: str
    wreq_platform: str
    verified: bool
    weight: float
    headers: Dict[str, str] = field(default_factory=dict, compare=False, hash=False)
    note: str = field(default='', compare=False, hash=False)
    # Headers whose value differs from the backend profile's own default
    # (a newer browser version on the same TLS/HTTP2 profile: User-Agent,
    # client hints, Accept). They are sent explicitly on every request.
    header_overrides: Tuple[str, ...] = field(default=(), compare=False, hash=False)

    @property
    def label(self) -> str:
        """Short label for logs and stats, e.g. ``chrome154-win``."""
        return self.id


@dataclass(frozen=True)
class ProfileTable:
    schema: int
    backend: str
    backend_version: str
    generated: str
    reference_versions: Dict[str, int]
    curl_cffi_targets: Dict[str, str]
    header_orders: Dict[str, Dict[str, List[str]]]
    profiles: Tuple[BrowserProfile, ...]

    def newest(self, family: str) -> Optional[BrowserProfile]:
        rows = [p for p in self.profiles if p.family == family]
        return max(rows, key=lambda p: p.version) if rows else None

    def order(self, family: str, kind: str) -> List[str]:
        orders = self.header_orders.get(family) or self.header_orders['chrome']
        return list(orders.get(kind) or orders['navigate'])


def _parse_table(raw: Dict[str, Any]) -> ProfileTable:
    profiles = []
    for row in raw['profiles']:
        profiles.append(
            BrowserProfile(
                id=row['id'],
                family=row['family'],
                version=int(row['version']),
                version_label=str(row.get('version_label') or row['version']),
                os=row['os'],
                device=row['device'],
                wreq_profile=row['wreq_profile'],
                wreq_platform=row['wreq_platform'],
                verified=bool(row.get('verified')),
                weight=float(row.get('weight') or 0.0),
                headers={k.lower(): v for k, v in (row.get('headers') or {}).items()},
                note=row.get('note') or '',
                header_overrides=tuple(h.lower() for h in (row.get('header_overrides') or ())),
            )
        )
    return ProfileTable(
        schema=int(raw['schema']),
        backend=raw['backend'],
        backend_version=raw['backend_version'],
        generated=raw['generated'],
        reference_versions={k: int(v) for k, v in raw['reference_versions'].items()},
        curl_cffi_targets=dict(raw.get('curl_cffi_targets') or {}),
        header_orders={f: {k: list(v) for k, v in kinds.items()} for f, kinds in raw['header_orders'].items()},
        profiles=tuple(profiles),
    )


@lru_cache(maxsize=1)
def load_table() -> ProfileTable:
    """The profile table shipped with the SDK (``scrapely/http/profiles.json``)."""
    text = resources.files('scrapely.http').joinpath('profiles.json').read_text(encoding='utf-8')
    return _parse_table(json.loads(text))


# ─── option matching ────────────────────────────────────────────────────────


@dataclass
class BrowserSpec:
    name: str
    min_version: Optional[int] = None
    max_version: Optional[int] = None

    def matches(self, profile: BrowserProfile) -> bool:
        if profile.family != self.name:
            return False
        if self.min_version is not None and profile.version < self.min_version:
            return False
        if self.max_version is not None and profile.version > self.max_version:
            return False
        return True


def _filter(
    table: ProfileTable,
    browsers: Sequence[BrowserSpec],
    operating_systems: Sequence[str],
    devices: Sequence[str],
    verified_only: bool,
) -> List[BrowserProfile]:
    out = []
    for p in table.profiles:
        if verified_only and not p.verified:
            continue
        if browsers and not any(b.matches(p) for b in browsers):
            continue
        if operating_systems and p.os not in operating_systems:
            continue
        if devices and p.device not in devices:
            continue
        out.append(p)
    return out


class NoMatchingProfile(ValueError):
    """No profile in the table matches the fingerprint options (``strict=True``)."""


def select_candidates(
    browsers: Sequence[BrowserSpec],
    operating_systems: Sequence[str],
    devices: Sequence[str],
    strict: bool,
    table: Optional[ProfileTable] = None,
) -> Tuple[List[BrowserProfile], List[str]]:
    """Profiles that may be used for the given options, plus notes on any relaxation.

    ``strict=True``: only verified profiles, nothing relaxed; raises
    ``NoMatchingProfile`` when nothing matches.

    ``strict=False``: unverified profiles of explicitly requested families are
    allowed. When nothing matches, the options are relaxed in this order:
    version range, devices, operating systems, browsers. The result is always a
    profile from the table, so the backend can impersonate it end to end.
    """
    table = table or load_table()
    notes: List[str] = []
    default_browsers = [BrowserSpec('chrome')]
    explicit = bool(browsers)
    specs = list(browsers) if explicit else default_browsers
    devs = list(devices) if devices else ['desktop']
    oses = list(operating_systems)

    if strict:
        found = _filter(table, specs, oses, devs, verified_only=True)
        if not found:
            raise NoMatchingProfile(
                'No verified browser profile matches the fingerprint options '
                f'(browsers={[s.name for s in specs]}, operating_systems={oses or "any"}, devices={devs}). '
                'Pass strict=False to allow unverified profiles and relaxed options.'
            )
        return found, notes

    # Unverified profiles are only used for families the caller asked for by name.
    verified_only = not explicit
    attempts: List[Tuple[str, List[BrowserSpec], List[str], List[str]]] = [
        ('', specs, oses, devs),
        ('version range', [BrowserSpec(s.name) for s in specs], oses, devs),
        ('devices', [BrowserSpec(s.name) for s in specs], oses, []),
        ('operating_systems', [BrowserSpec(s.name) for s in specs], [], []),
        ('browsers', default_browsers, [], ['desktop']),
    ]
    for relaxed, b, o, d in attempts:
        only_verified = verified_only or relaxed == 'browsers'
        found = _filter(table, b, o, d, verified_only=only_verified)
        if found:
            if relaxed:
                notes.append(f'no profile matched the fingerprint options; relaxed {relaxed}')
            return found, notes
    raise NoMatchingProfile('the profile table is empty')  # pragma: no cover - table always has chrome


def pick(
    candidates: Sequence[BrowserProfile],
    rng: random.Random,
    avoid: Optional[BrowserProfile] = None,
) -> BrowserProfile:
    """Weighted draw; avoids repeating ``avoid`` (a just-blocked profile) when there is a choice."""
    pool = [p for p in candidates if p != avoid] if avoid is not None and len(candidates) > 1 else list(candidates)
    weights = [max(p.weight, 0.001) for p in pool]
    return rng.choices(pool, weights=weights, k=1)[0]


# ─── curl_cffi-style ``impersonate=`` names ──────────────────────────────────

_IMPERSONATE_RE = re.compile(r'^(chrome|edge|firefox|safari|tor|okhttp|opera)[_-]?(ios|android)?[_-]?([\d_.]*)[_-]?(ios|android)?$')


def impersonate_to_options(name: Optional[str]) -> Tuple[List[BrowserSpec], List[str], List[str], Optional[str]]:
    """Map a curl_cffi ``impersonate`` value onto (browsers, operating_systems, devices, warning).

    The version in the name is ignored: profiles are clamped to the current
    ones in the table, because an old version is scored as a bot.
    """
    if not name:
        return [], [], [], None
    key = str(name).strip().lower()
    if key in ('realworld', 'random'):
        return [], [], [], None
    m = _IMPERSONATE_RE.match(key)
    if not m:
        return [], [], [], f'impersonate={name!r} is not a known browser; using the default Chrome profiles'
    family = m.group(1)
    platform = m.group(2) or m.group(4)
    if family in ('tor', 'okhttp', 'opera'):
        return [], [], [], f'impersonate={name!r} has no profile here; using the default Chrome profiles'
    if family == 'safari':
        if platform == 'ios':
            return [BrowserSpec('safari')], ['ios'], ['mobile'], None
        return [BrowserSpec('safari')], ['macos'], ['desktop'], None
    if platform == 'android':
        return [BrowserSpec(family)], ['android'], ['mobile'], None
    return [BrowserSpec(family)], [], ['desktop'], None
