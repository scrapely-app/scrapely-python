# Changelog

All notable changes to this project will be documented in this file.

## [0.1.45] - 2026-11-09

- **Dataset pushes larger than the API's 10 MiB request limit are stored instead of failing.** The API refuses a request body over 10 MiB (10,485,760 bytes) with `413`. `Actor.push_data(rows)` (and `Dataset.push_data`, crawler `context.push_data`) used to send all rows in one request, so a few big rows that each fitted (for example videos with many subtitle tracks) failed the whole push, and an actor that retried it failed the same way each time. A push that fits one request is sent exactly as before. A larger one is now sent in pieces of at most 9 MiB, in order, as the JS SDK does.
- A charged push (`push_data(rows, charged_event_name=...)` or `charges={...}` per row) that big is charged and stored piece by piece, each piece as a push of its own: a piece is billed only when it was stored. If a piece fails, the earlier pieces stay stored and billed and the error is raised. A `charge_per='batch'` push is charged once; if one of its pieces fails, the charge is given back.
- An uncharged push whose later piece fails raises `DatasetPushIncompleteError` (an `ApiClientError`) with `stored_items`: the first `stored_items` rows were stored.
- A single item larger than one request (over 10 MiB as JSON) can never be stored. It now fails at once with `DatasetItemTooLargeError` (an `ApiClientError`, status `413`) naming the item's position, its size and the limit, for example "Dataset item at position 1 is 11010084 bytes as JSON, larger than the 10485760 bytes one push may hold, so it cannot be stored; nothing of this push was saved." Nothing of that push is sent or charged. Store large fields in the key-value store instead.

## [0.1.44] - 2026-11-08

- An atomic multi-event push (`Actor.push_data(rows, charges={...})`) whose charge the limit covers only in part no longer shows "Charging 5 instances of 'audio-minute' event would exceed the end of the account balance - only 3 events were charged". A row is saved only when all of its events are charged, so the SDK gives those 3 back and nothing is charged: the line was untrue. It now says what happened: "Saving this item needs 5 'audio-minute' event(s), but the account balance covers only 3: it was not saved and nothing was charged for it." When some rows still fit, it names how many were saved and charged in full, and when giving a charge back is not confirmed it says so instead of "nothing was charged". A plain `Actor.charge()` that comes back short keeps its line: those events do stay charged.
- `ChargeResult.covered_counts` (alias `coveredCounts`, from `push_data(rows, charges={...})`): for each event whose charge came back short, how many events the limit covered (given back, not charged), e.g. `{'audio-minute': 3}`. Actors can cut a row to what the limit covers without reading the log.
- Compatibility: actors that read N from the old line with a `logging.Filter` on the `scrapely.charging` logger still receive that exact line during the push. It is shown to the logger's filters only and never printed; when such a filter drops it, the SDK leaves the refusal message to the actor, as before. What is charged and saved does not change.
- A spending limit of $0 (`max_total_charge_usd = 0`) is a real limit, the same as on the platform. It used to be read as "no limit". A missing or non-numeric value still means no limit.
- `scrapely.solver`: a challenge step's answer is read as a stream and stops at the body size limit (httpx, requests and curl_cffi sessions), so a target site cannot make the step download an unbounded body.

## [0.1.43] - 2026-11-05

- **Browser patches are applied through an import hook.** `scrapely.patch_browser_pool` imports no browser library any more; it imports builtin modules only. Each patch now runs the moment its library finishes loading, before the import that loaded it returns. That covers the Crawlee BrowserPool / PlaywrightBrowserPlugin / PlaywrightCrawler, raw Playwright (`async_playwright` and `sync_playwright`, each when its module loads), Camoufox `AsyncNewBrowser` and pyppeteer `launch`. The patches behave exactly as before. The hook also:
  - survives `importlib.util.find_spec()` availability checks and a failed first import;
  - logs a warning when a patch fails instead of breaking the actor's import.

## [0.1.42] - 2026-11-02

- **Sessions that keep their identity: `http.session(keep_identity=True)`.** A pinned `scrapely.http` session used to retire its identity on the first block (a 403, a challenge page, a network error). The session then moved to a new exit, a new profile and an empty cookie jar. That breaks every challenge solve: the challenge page, the solver's script and submission requests and the page fetched again must all share one identity. With `keep_identity=True`:
  - a block comes back to the caller (`response.blocked` names it) and an error is raised, with no rotation;
  - the next request goes out on the same profile, exit and jar, with the cookies the block set;
  - `max_usage` / `max_age` do not retire it;
  - `session.rotate()` moves the session to a fresh identity when you decide to.

  The default (`keep_identity=False`) is unchanged. The option is available on `AsyncClient.session()` and `Client.session()`.
- **`scrapely.solver.AkamaiSensor`: Akamai Bot Manager on your own session, the way the solver provider's examples do it.**
  - `solve_sbsd(session, url, html)` handles a page that loads an SBSD bundle. It fetches the bundle on your session, gets the ledger from the solver (`/akamai/sbsd/generate-session`), then POSTs the ledger rows to the bundle path on your session.
  - `solve_abck(session, url, html)` runs the `_abck` sensor over the solver's WebSocket session (`/akamai/session`, which the platform relays for runs). The solver generates each sensor; it is POSTed on your session with your client's own headers, and rounds continue until `_abck` is accepted (`~0~`).
  - `solve()` does SBSD when the page carries a bundle, then `_abck`.

  Use it with a `keep_identity=True` session (or one curl_cffi or httpx client), and set `profile_id` to the browser your client claims, e.g. `chrome-154-macos` for scrapely.http Chrome 154 on macOS. Submissions outside the target site are refused, and a refused session raises the platform's code (`VendorUnavailable`, `SolverRateLimited`, ...). Also new: `find_sensor_script()`, `find_sbsd_bundle()`, `abck_state()`, `SensorResult`.
- **`AkamaiSensor.solve_pixel(session, url, html)`: Akamai's third channel, the pixel collector.** A page that loads `/akam/<n>/<hex>` (often next to an inline `bazadebezolkohpepadr=` value) expects that script to POST to `/akam/<n>/pixel_<hex>`. The collector is fetched on your session, the solver writes the POST the collector would send (`/akamai/pixel/generate`), and it is POSTed to the collector's own endpoint on your session. Also new: `find_pixel_collector()`, `pixel_target()`.

## [0.1.41] - 2026-10-29

- **A refused charge names the limit that refused it.** When a charge is refused or cut short, the platform now says why (HTTP 402 `error.reason`, and `limitedBy` on a partial acceptance): the run's item limit (`maxItems`), its maximum total charge (`maxTotalChargeUsd`) or the account balance. The SDK used to call all of them the spending limit; a run stopped by its item limit logged "The spending limit of this run (maxTotalChargeUsd = $3.0) has been reached". Now:
  - The SDK's own log lines name the real limit ("The item limit of this run (maxItems = 800) has been reached: ...").
  - `ChargingManager.get_charge_limit_reason()` returns `'max-items'`, `'max-charge'` or `'insufficient-funds'` (None while charges are still accepted), and `get_charge_limit_message()` the stop message: "Stopped: reached the item limit you set (800 items)", "Stopped: reached the maximum charge you set ($3.00)" or "Stopped: your account has no credit left for this run".
  - Once a limit was reached, a status message (`Actor.set_status_message`, `Actor.exit`, `Actor.fail`) whose clause blames the maximum charge ("Saved 800 results; stopped because the maximum charge for this run was reached") gets that clause replaced by the real stop message ("Saved 800 results. Stopped: reached the item limit you set (800 items)"). Other messages are untouched.
  - Platforms that do not send the reason yet: inferred from the 402 message and `ACTOR_MAX_PAID_DATASET_ITEMS`.
  - What is charged and saved does not change.

## [0.1.40] - 2026-10-28

0.1.39 was never published to PyPI. This release includes the 0.1.39 change below, plus these fixes to `scrapely.http` from the fingerprint test on staging (2026-10-27):

- **Accept-Language matches the exit IP.** Before, a client without a proxy country sent `en-US` on every identity: 8 of 20 residential sessions did so from Brazil, France, Spain, Mexico or Taiwan.
  - **Residential platform proxy without a country:** each identity now picks an exit country, weighted like real web traffic, asks the proxy for it and sends that country's language.
  - **Proxy with a country:** each identity gets one of the language setups common there (for example `de-DE,de;q=0.9` or `de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7`).
  - **Several `locales`:** each identity uses one of them, not all of them in one header. With a residential proxy without a country, the exit follows the locale's country.
  - **Unchanged:** no proxy, your own proxy URLs and the `DATACENTER` group (US exits only) keep `en-US,en;q=0.9`.
  - `ProxyConfiguration.new_url()` takes an optional `country_code` for one URL (residential platform proxy without a country only). Sessions expose `accept_language` and `country`.
- **No more `400` on `https://host?q`.** A URL with a query and no path is sent with the path `/`. wreq 0.13 sent `:path: ?q` over HTTP/2, which Cloudflare and nginx reject.
- **Chrome 155 and 156.** The default profiles are Chrome 156, 155, 154 and 153, weighted 55 / 30 / 10 / 5. 155 and 156 use the Chrome 154 TLS and HTTP/2 profile, which is the same as real Chrome 156's, with their own User-Agent, `sec-ch-ua` and Accept (with `image/jxl`, which real 155 and 156 send).
- **Firefox 153 ESR.** Added Firefox 153 (ESR) next to 152. Both match real Firefox 153.6 ESR and 152 builds byte for byte in the TLS ClientHello and HTTP/2 settings, and are now marked verified.

## [0.1.39] - 2026-10-27 (not published)

- A key-value store record write (`Actor.set_value`, `KeyValueStore.set_value`) is no longer lost to one failed answer. Each write now sends an `Idempotency-Key`, and when its answer is lost (a `502`/`504` page from the edge or load balancer, a dropped connection, a timeout) it is sent again with the same key; the API applies and counts it once. Before, the write was never re-sent, so one such page on the final `OUTPUT` write failed the whole run. An API that answers a keyed write without `X-Scrapely-Idempotency` (an older server) gets no re-sends, as before.

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
