# Changelog

All notable changes to this project will be documented in this file.

## [0.1.38] - 2026-10-27

0.1.37 was never published to PyPI. This release includes everything listed under 0.1.37 below, plus these changes to `scrapely.http`:

- **Instant rotation.** A `429`, block page, captcha, challenge, `511`, `5xx` or network error now moves the request to a brand-new identity at once: a new proxy session, a new profile (a different one from the blocked identity's when there is a choice) and an empty cookie jar.
  - **No waiting.** By default the client no longer waits: `Retry-After` is ignored, and there is no per-host backoff or slow mode.
  - **Cap.** `max_rotations` (default 6) caps the rotations per request; `max_retries` is still accepted as its older name. With a fixed proxy URL, or no proxy, a block rotates once.
- **Polite mode (opt in).** `polite=True` or `backoff=BackoffPolicy(...)` brings back `Retry-After` (capped at 60 s), the 1, 2, 4 … s per-host backoff, the PerimeterX same-identity retry and the circuit-breaker slow mode.
- **Stats.** `stats()`, the summary line and `HTTP_STATS` count rotations: in total, per reason or vendor, and per host.

## [0.1.37] - 2026-10-27 (not published)

- Added `scrapely.http`, an HTTP client that looks like a real browser and handles blocks (`AsyncClient`, sync `Client`, `session()` for multi-step flows).
  - **Fingerprint.** TLS, HTTP/2, User-Agent, client hints and header order all come from one current browser profile, listed in `scrapely/http/profiles.json`. The default desktop Chrome profiles send the same JA4, HTTP/2 settings and header order as real Chrome 156.
  - **Identities.** An identity is a profile, a proxy session from `ProxyConfiguration.new_url(session_id)` and a cookie jar. They are pooled, concurrent requests never share one, and a blocked identity is retired as a whole.
  - **Block detection.** Cloudflare, PerimeterX, DataDome, Akamai, `429`, `511` and `403` pages are detected, and so are blocks that your own `is_blocked` check reports. Beacons that real pages also carry are not counted as blocks.
  - **Retries.** Blocked requests are retried on a fresh identity. (0.1.38 changed the waits; see above.)
  - **Accept-Language.** It follows the proxy country, unless you set `locales`.
  - **Headers you add** (referer, cookie, origin, content-type, `x-*` and so on) go where a real browser puts them. A User-Agent or client hint you pass is ignored, with one warning.
  - **Options.** `fingerprint_options` takes snake_case keys, and also the camelCase `fingerprintOptions.fingerprintGeneratorOptions` form.
  - **Stats.** When the client closes, it logs a one-line summary and writes the run's totals to the default key-value store as `HTTP_STATS`.
  - **curl_cffi migration.** `AsyncSession` / `Session` accept `impersonate=`, `proxy=`, `proxies=`, `timeout=`, `headers=` and `verify=`, so switching from curl_cffi takes one line.
- New dependency: `wreq>=0.13,<0.14` (Python 3.11+). If `wreq` can't be imported, the client falls back to `curl_cffi` when it is installed, and logs one warning.

## [0.1.36] - 2026-10-26

- Opening a named dataset, key-value store or request queue (`Actor.open_dataset('name')` and the like) retries when the API answers `503 storage-name-busy`. The API sends it when another request kept creating or renaming a storage of that name for over 15 s; nothing was changed, so the open is simply repeated. It used to fail the open.

## [0.1.35] - 2026-10-26

- Once `Actor.fail()` has run, the run stays failed. A later `Actor.exit()` keeps the failure's exit code and status message, so the usual `try: ... await Actor.fail(...) finally: await Actor.exit()` pattern now ends the run as FAILED. Before, the `exit()` in `finally` replaced the failure's exit code 1 with 0 and the run ended as SUCCEEDED with the failure message as its status. The same holds after `fail()` whose `SystemExit` was caught, after `fail(ExitOptions(exit=False))` (also inside `async with Actor(...)` and `Actor.main()`), and for a second `fail()`, which keeps the first failure's code and message.
- `Actor.fail(ExitOptions(...))` without an exit code exits with 1. `ExitOptions` defaults `exit_code` to 0, which made such a failure exit with 0. A non-zero `exit_code` in the options, or an explicit `exit_code=` keyword, still wins.

## [0.1.34] - 2026-10-23

- Debug captures (`Actor.debug_capture`, `Actor.debug_capture_sync`) store parsed XML/HTML elements as their markup instead of a repr such as `<Element 'change' at 0x...>`: `xml.etree.ElementTree` elements and trees, lxml elements and trees, BeautifulSoup tags, selectolax nodes and parsel / Scrapy selectors, passed alone or inside a dict or list. The text after an element (its tail) is not included. Other objects are still stored as their `str()`. Captures are only active on the platform's own diagnostic runs (`SCRAPELY_DEBUG_CAPTURE=1`).

## [0.1.33] - 2026-10-22

- Debug captures (`Actor.debug_capture`, `Actor.debug_capture_sync`) keep payloads over 256 KiB whole. They are stored gzip-compressed and base64-encoded as `{key, payload, encoding: 'gzip-base64', length, truncated}` instead of being cut at 262,144 characters; up to 8 Mi characters are kept. A payload that does not compress enough keeps its first 256 KiB, now marked `truncated: True` with its full `length`. Dict and list payloads are dumped as compact JSON, and `bytes` payloads are stored as UTF-8 text (they used to be stored as their `repr`). Captures are only active on the platform's own diagnostic runs (`SCRAPELY_DEBUG_CAPTURE=1`).

## [0.1.32] - 2026-10-20

- Maintenance release.

## [0.1.31] - 2026-10-20

- The storage client percent-encodes request ids and key-value store record keys as one URL path segment and refuses `''`, `.` and `..`. A request id such as `..` read from a queue used to send the call to another resource.

## [0.1.30] - 2026-10-20

- Maintenance release.

## [0.1.29] - 2026-10-19

- Initial public release.
