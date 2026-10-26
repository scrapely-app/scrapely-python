# Changelog

All notable changes to this project will be documented in this file.

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
