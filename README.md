# Scrapely Python SDK

Build Python Actors for the [Scrapely](https://scrape.ly) platform with lifecycle helpers, Crawlee-compatible storage, platform events, remote browser CDP, run control, webhooks, and pay-per-event accounting.

## Install

Scrapely requires Python 3.11 or newer.

```bash
python -m pip install scrapely-python
```

Install the appropriate Crawlee/Playwright browser dependencies separately when your Actor uses a browser.

## Quick start

```python
import asyncio

from scrapely import Actor


async def main() -> None:
    actor_input = await Actor.get_input_or_throw()
    await Actor.push_data({
        "url": actor_input["url"],
        "status": "processed",
    })


if __name__ == "__main__":
    asyncio.run(Actor.main(main))
```

`Actor.main()` initializes the Actor, awaits your callback, closes events and storage, and exits the process. For embedded applications and tests, use `async with Actor:` instead.

## Highlights

- Local file-system storage and Scrapely API-backed datasets, key-value stores, and request queues
- Crawlee request loaders and storage objects
- Platform event WebSocket integration and state persistence
- Automatic remote Chromium CDP wiring through `CDP_URL`
- Actor start, call, abort, reboot, and metamorph operations
- Ad-hoc webhooks, proxy configuration, and charging/pricing types
- Legacy environment variable aliases and asynchronous client access

See the [complete Python SDK guide](https://scrape.ly/docs/sdk-python) for lifecycle details, option objects, storage selection, events, CDP behavior, run control, environment variables, errors, and the full public export list.

## License

MIT (see [LICENSE](./LICENSE)), except for three files that stay under the Apache License 2.0
(see [LICENSE-APACHE](./LICENSE-APACHE) and [NOTICE](./NOTICE)).
