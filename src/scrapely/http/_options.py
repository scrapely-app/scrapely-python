"""Fingerprint options: which browser profiles a client may use.

Accepts the snake_case form and the camelCase form used by fingerprint
generator options in Crawlee-style code, including the nested
``{"fingerprintGeneratorOptions": {...}}`` shape, so ported code works as is.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from ._profiles import DEVICES, FAMILIES, OPERATING_SYSTEMS, BrowserSpec

_BROWSER_ALIASES = {
    'chrome': 'chrome',
    'chromium': 'chrome',
    'google chrome': 'chrome',
    'edge': 'edge',
    'msedge': 'edge',
    'microsoft edge': 'edge',
    'firefox': 'firefox',
    'ff': 'firefox',
    'safari': 'safari',
    'webkit': 'safari',
}
_OS_ALIASES = {
    'windows': 'windows',
    'win': 'windows',
    'win32': 'windows',
    'macos': 'macos',
    'mac': 'macos',
    'osx': 'macos',
    'darwin': 'macos',
    'linux': 'linux',
    'android': 'android',
    'ios': 'ios',
    'iphone': 'ios',
}
_DEVICE_ALIASES = {'desktop': 'desktop', 'mobile': 'mobile', 'phone': 'mobile', 'tablet': 'mobile'}

# camelCase → snake_case for the keys we read.
_KEY_ALIASES = {
    'operatingSystems': 'operating_systems',
    'httpVersion': 'http_version',
    'minVersion': 'min_version',
    'maxVersion': 'max_version',
}


def _norm_key(key: str) -> str:
    return _KEY_ALIASES.get(key, key)


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (str, bytes, Mapping)):
        return [value]
    return list(value)


def _normalize(values: Any, aliases: Mapping[str, str], allowed: Sequence[str], what: str) -> List[str]:
    out: List[str] = []
    for raw in _as_list(values):
        name = aliases.get(str(raw).strip().lower())
        if name is None or name not in allowed:
            raise ValueError(f'fingerprint_options: unknown {what} {raw!r} (allowed: {", ".join(allowed)})')
        if name not in out:
            out.append(name)
    return out


def _browser_specs(values: Any) -> List[BrowserSpec]:
    specs: List[BrowserSpec] = []
    for raw in _as_list(values):
        if isinstance(raw, BrowserSpec):
            specs.append(raw)
            continue
        if isinstance(raw, Mapping):
            row = {_norm_key(k): v for k, v in raw.items()}
            name = row.get('name')
            min_v, max_v = row.get('min_version'), row.get('max_version')
        else:
            name, min_v, max_v = raw, None, None
        family = _normalize([name], _BROWSER_ALIASES, FAMILIES, 'browser')[0]
        specs.append(
            BrowserSpec(
                family,
                int(min_v) if min_v is not None else None,
                int(max_v) if max_v is not None else None,
            )
        )
    return specs


@dataclass
class FingerprintOptions:
    """Which browsers, operating systems, devices and locales a client may present.

    All fields are optional. The defaults are desktop Chrome on Windows, macOS
    and Linux (weighted toward real traffic), with Accept-Language matching
    each identity's exit country.

    Attributes:
        browsers: ``"chrome"``, ``"edge"``, ``"firefox"``, ``"safari"``, or
            ``{"name": "chrome", "min_version": 150}``.
        operating_systems: ``"windows"``, ``"macos"``, ``"linux"``, ``"android"``, ``"ios"``.
        devices: ``"desktop"`` (default) and/or ``"mobile"``.
        locales: e.g. ``["de-DE"]``. Each identity uses one of them, as a
            browser has one language setup. Wins over the proxy country.
        strict: only profiles whose fingerprint was verified against the real
            browser, and fail instead of relaxing options that match nothing.
        http_version: ``"2"`` (default) or ``"1"`` to force HTTP/1.1.
    """

    browsers: List[BrowserSpec] = field(default_factory=list)
    operating_systems: List[str] = field(default_factory=list)
    devices: List[str] = field(default_factory=list)
    locales: List[str] = field(default_factory=list)
    strict: bool = False
    http_version: str = '2'

    @classmethod
    def parse(cls, value: Union['FingerprintOptions', Mapping[str, Any], None] = None, **kwargs: Any) -> 'FingerprintOptions':
        """Build options from a ``FingerprintOptions``, a dict (snake_case or camelCase) or keywords."""
        if isinstance(value, FingerprintOptions) and not kwargs:
            return value
        raw: Dict[str, Any] = {}
        if isinstance(value, FingerprintOptions):
            raw.update(
                browsers=value.browsers,
                operating_systems=value.operating_systems,
                devices=value.devices,
                locales=value.locales,
                strict=value.strict,
                http_version=value.http_version,
            )
        elif isinstance(value, Mapping):
            src: Mapping[str, Any] = value
            # Crawlee-style nesting: {"fingerprintGeneratorOptions": {...}} (or snake_case).
            for nest in ('fingerprintGeneratorOptions', 'fingerprint_generator_options'):
                if isinstance(src.get(nest), Mapping):
                    inner = dict(src[nest])
                    rest = {k: v for k, v in src.items() if k != nest}
                    rest.update(inner)
                    src = rest
                    break
            for k, v in src.items():
                raw[_norm_key(k)] = v
        elif value is not None:
            raise TypeError(f'fingerprint_options must be a dict or FingerprintOptions, not {type(value).__name__}')
        for k, v in kwargs.items():
            raw[_norm_key(k)] = v

        locales = [str(x).strip() for x in _as_list(raw.get('locales')) if str(x).strip()]
        if len(locales) > 10:
            locales = locales[:10]
        http_version = str(raw.get('http_version') or '2')
        if http_version not in ('1', '2'):
            raise ValueError(f'fingerprint_options: http_version must be "1" or "2", got {http_version!r}')
        return cls(
            browsers=_browser_specs(raw.get('browsers')),
            operating_systems=_normalize(raw.get('operating_systems'), _OS_ALIASES, OPERATING_SYSTEMS, 'operating system'),
            devices=_normalize(raw.get('devices'), _DEVICE_ALIASES, DEVICES, 'device'),
            locales=locales,
            strict=bool(raw.get('strict', False)),
            http_version=http_version,
        )
