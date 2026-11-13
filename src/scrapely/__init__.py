"""
Scrapely Python SDK - A toolkit for building Actors on the Scrapely platform.

Usage:
    from scrapely import Actor, Dataset

    async def main():
        async with Actor:
            actor_input = await Actor.get_input()
            await Actor.push_data({'url': actor_input['url']})
"""

from __future__ import annotations

# ``typing`` is not imported at run time (see below): this package loads at
# every interpreter start in actor images.
TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any

# Side-effect: patch browser launches (Crawlee BrowserPool, PlaywrightCrawler,
# raw Playwright, Camoufox, pyppeteer) to use the remote CDP browser (CDP_URL).
# Must run on package import so `from scrapely import Actor` enables the patch
# before any browser is launched (Node SDK parity). It imports none of those
# libraries: each patch is applied the moment its library finishes loading.
from .patch_browser_pool import set_cdp_url  # noqa: F401

# Nothing else is imported here. The base images' sitecustomize imports
# ``scrapely.patch_browser_pool`` (and so this package) in EVERY Python process
# of an actor image; loading the whole SDK there cost ~0.75 s per interpreter
# start at 1 CPU (5.7 s at 0.25 CPU). Every public name is resolved on first
# attribute access by ``__getattr__`` below and cached in the module namespace,
# so every ``from scrapely import X`` that worked before still works.

# Public name -> (module, attribute). Loaded on first access.
_LAZY_ATTRS = {
    'Actor': ('scrapely.actor', 'Actor'),
    'Configuration': ('scrapely.configuration', 'Configuration'),
    'ProxyConfiguration': ('scrapely.proxy_configuration', 'ProxyConfiguration'),
    'BrowserConfiguration': ('scrapely.browser_configuration', 'BrowserConfiguration'),
    # Charging client
    **{
        name: ('scrapely.charging_client', name)
        for name in ('USES_PUSH_DATA_INTERCEPTION', 'create_charging_client', 'push_data_charging_context')
    },
    # Charging
    **{
        name: ('scrapely.charging', name)
        for name in (
            'ChargingManager',
            'ChargeUncertainError',
            'ChargeRejectedError',
            'has_default_dataset_item_event_in_pricing',
            'is_default_dataset_item_event',
            'merge_charge_results',
            'push_data_and_charge',
            'resolve_default_dataset_item_event_name',
            'APIFY_DEFAULT_DATASET_ITEM_EVENT',
            'SCRAPELY_DEFAULT_DATASET_ITEM_EVENT',
            'DEFAULT_DATASET_ITEM_EVENT',
        )
    },
    # Types
    **{
        name: ('scrapely.types', name)
        for name in (
            'ActorInput',
            'ActorPermissionLevel',
            'ActorRun',
            'ApifyEnv',
            'AbortOptions',
            'CallOptions',
            'ExitOptions',
            'InitOptions',
            'MetamorphOptions',
            'OpenStorageOptions',
            'ProxyInfo',
            'RebootOptions',
            'RecordOptions',
            'SetStatusMessageOptions',
            'StartOptions',
            'WebhookOptions',
        )
    },
    # Webhook
    'Webhook': ('scrapely.webhook', 'Webhook'),
    'WebhookEventType': ('scrapely.webhook', 'WebhookEventType'),
    # Pricing
    **{
        name: ('scrapely.pricing_types', name)
        for name in (
            'PricingModel',
            'FreeActorPricingInfo',
            'FlatPricePerMonthActorPricingInfo',
            'PricePerDatasetItemActorPricingInfo',
            'PayPerEventActorPricingInfo',
            'PricingPerEvent',
            'ActorPricingInfo',
            'ActorChargeEvent',
            'ChargeResult',
            'ActorPricingInfoResult',
        )
    },
    # Errors
    **{
        name: ('scrapely.errors', name)
        for name in (
            'ScrapelyClientError',
            'ScrapelyApiError',
            'InvalidResponseBodyError',
            'InvalidRequestError',
            'UnauthorizedError',
            'ForbiddenError',
            'NotFoundError',
            'ConflictError',
            'RateLimitError',
            'ServerError',
            'raise_for_status',
        )
    },
    # Crawlee re-exports
    'Request': ('crawlee', 'Request'),
    'RequestLoader': ('crawlee.request_loaders', 'RequestLoader'),
    'RequestManager': ('crawlee.request_loaders', 'RequestManager'),
    'RequestManagerTandem': ('crawlee.request_loaders', 'RequestManagerTandem'),
    'SitemapRequestLoader': ('crawlee.request_loaders', 'SitemapRequestLoader'),
    'Dataset': ('crawlee.storages', 'Dataset'),
    'KeyValueStore': ('crawlee.storages', 'KeyValueStore'),
    'RequestQueue': ('crawlee.storages', 'RequestQueue'),
    'DatasetItemsListPage': ('crawlee.storage_clients.models', 'DatasetItemsListPage'),
    'EventListener': ('crawlee.events', 'EventListener'),
    # Storage clients
    'ApiStorageClient': ('scrapely.storage', 'ApiStorageClient'),
    'FileSystemStorageClient': ('scrapely.storage', 'FileSystemStorageClient'),
    'MemoryStorageClient': ('scrapely.storage', 'MemoryStorageClient'),
    'ScrapelyStorageClient': ('scrapely.storage', 'ScrapelyStorageClient'),
    'SmartScrapelyStorageClient': ('scrapely.storage', 'SmartScrapelyStorageClient'),
    'StorageClient': ('scrapely.storage', 'StorageClient'),
    'set_api_storage': ('scrapely.storage', 'set_api_storage'),
    # Request loaders
    'ScrapelyRequestList': ('scrapely.request_loaders', 'ScrapelyRequestList'),
    # Events
    **{
        name: ('scrapely.events', name)
        for name in (
            'AbortingEvent',
            'ActorEventTypes',
            'CrawleeEventManager',
            'DeprecatedEvent',
            'Event',
            'EventAbortingData',
            'EventExitData',
            'EventManager',
            'EventMessage',
            'EventMigratingData',
            'EventPersistStateData',
            'EventSystemInfoData',
            'EventWithoutData',
            'ExitEvent',
            'LocalEventManager',
            'MigratingEvent',
            'PersistStateEvent',
            'ScrapelyEventManager',
            'SystemInfoEvent',
            'SystemInfoEventData',
            'UnknownEvent',
        )
    },
}

# Submodules that used to be loaded (and so bound as attributes) by this
# package's eager imports; ``scrapely.actor``, ``scrapely.events`` etc. keep
# working without an explicit ``import scrapely.<name>``.
_LAZY_SUBMODULES = frozenset({
    '_browser_locations',
    'actor',
    'browser_configuration',
    'cdp_retry',
    'charging',
    'charging_client',
    'configuration',
    'crypto',
    'errors',
    'events',
    'log',
    'pricing_types',
    'proxy_configuration',
    'request_loaders',
    'storage',
    'storage_clients',
    'storages',
    'types',
    'webhook',
})


def __getattr__(name: str) -> Any:
    target = _LAZY_ATTRS.get(name)
    if target is not None:
        import importlib

        value = getattr(importlib.import_module(target[0]), target[1])
        globals()[name] = value
        return value
    if name in _LAZY_SUBMODULES:
        import importlib

        return importlib.import_module(f'{__name__}.{name}')
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


def __dir__() -> list:
    return sorted(set(globals()) | set(_LAZY_ATTRS) | _LAZY_SUBMODULES)


if TYPE_CHECKING:  # Static analysers and IDEs see the lazy names as plain imports.
    from crawlee import Request
    from crawlee.events import EventListener
    from crawlee.request_loaders import (
        RequestLoader,
        RequestManager,
        RequestManagerTandem,
        SitemapRequestLoader,
    )
    from crawlee.storage_clients.models import DatasetItemsListPage
    from crawlee.storages import Dataset, KeyValueStore, RequestQueue

    from .actor import Actor
    from .browser_configuration import BrowserConfiguration
    from .charging import (
        APIFY_DEFAULT_DATASET_ITEM_EVENT,
        DEFAULT_DATASET_ITEM_EVENT,
        SCRAPELY_DEFAULT_DATASET_ITEM_EVENT,
        ChargeRejectedError,
        ChargeUncertainError,
        ChargingManager,
        has_default_dataset_item_event_in_pricing,
        is_default_dataset_item_event,
        merge_charge_results,
        push_data_and_charge,
        resolve_default_dataset_item_event_name,
    )
    from .charging_client import (
        USES_PUSH_DATA_INTERCEPTION,
        create_charging_client,
        push_data_charging_context,
    )
    from .configuration import Configuration
    from .errors import (
        ConflictError,
        ForbiddenError,
        InvalidRequestError,
        InvalidResponseBodyError,
        NotFoundError,
        RateLimitError,
        ScrapelyApiError,
        ScrapelyClientError,
        ServerError,
        UnauthorizedError,
        raise_for_status,
    )
    from .events import (
        AbortingEvent,
        ActorEventTypes,
        CrawleeEventManager,
        DeprecatedEvent,
        Event,
        EventAbortingData,
        EventExitData,
        EventManager,
        EventMessage,
        EventMigratingData,
        EventPersistStateData,
        EventSystemInfoData,
        EventWithoutData,
        ExitEvent,
        LocalEventManager,
        MigratingEvent,
        PersistStateEvent,
        ScrapelyEventManager,
        SystemInfoEvent,
        SystemInfoEventData,
        UnknownEvent,
    )
    from .pricing_types import (
        ActorChargeEvent,
        ActorPricingInfo,
        ActorPricingInfoResult,
        ChargeResult,
        FlatPricePerMonthActorPricingInfo,
        FreeActorPricingInfo,
        PayPerEventActorPricingInfo,
        PricePerDatasetItemActorPricingInfo,
        PricingModel,
        PricingPerEvent,
    )
    from .proxy_configuration import ProxyConfiguration
    from .request_loaders import ScrapelyRequestList
    from .storage import (
        ApiStorageClient,
        FileSystemStorageClient,
        MemoryStorageClient,
        ScrapelyStorageClient,
        SmartScrapelyStorageClient,
        StorageClient,
        set_api_storage,
    )
    from .types import (
        AbortOptions,
        ActorInput,
        ActorPermissionLevel,
        ActorRun,
        ApifyEnv,
        CallOptions,
        ExitOptions,
        InitOptions,
        MetamorphOptions,
        OpenStorageOptions,
        ProxyInfo,
        RebootOptions,
        RecordOptions,
        SetStatusMessageOptions,
        StartOptions,
        WebhookOptions,
    )
    from .webhook import Webhook, WebhookEventType

__version__ = '0.1.49'
__all__ = [
    # Core
    'Actor',
    'Configuration',
    'set_cdp_url',
    # Charging client
    'create_charging_client',
    'USES_PUSH_DATA_INTERCEPTION',
    'push_data_charging_context',

    # Storage
    'Dataset',
    'KeyValueStore',
    'RequestQueue',
    'Request',
    'DatasetItemsListPage',
    # Storage clients
    'ApiStorageClient',
    'ScrapelyStorageClient',
    'SmartScrapelyStorageClient',
    'FileSystemStorageClient',
    'MemoryStorageClient',
    'StorageClient',
    'set_api_storage',
    # Request loaders
    'ScrapelyRequestList',
    'RequestLoader',
    'RequestManager',
    'RequestManagerTandem',
    'SitemapRequestLoader',
    # Events
    'Event',
    'EventManager',
    'ScrapelyEventManager',
    'CrawleeEventManager',
    'LocalEventManager',
    # Event data types
    'SystemInfoEventData',
    'EventAbortingData',
    'EventExitData',
    'EventListener',
    'EventMigratingData',
    'EventPersistStateData',
    'EventSystemInfoData',
    # Event types
    'PersistStateEvent',
    'SystemInfoEvent',
    'MigratingEvent',
    'AbortingEvent',
    'ExitEvent',
    'EventWithoutData',
    'DeprecatedEvent',
    'UnknownEvent',
    'EventMessage',
    'ActorEventTypes',
    # Proxy
    'ProxyConfiguration',
    'ProxyInfo',
    # Remote browser
    'BrowserConfiguration',
    # Charging
    'ChargingManager',
    'ChargeResult',
    'ChargeUncertainError',
    'ChargeRejectedError',
    'merge_charge_results',
    'push_data_and_charge',

    # Types
    'ActorInput',
    'ActorPermissionLevel',
    'ActorRun',
    'ApifyEnv',
    'AbortOptions',
    'CallOptions',
    'ExitOptions',
    'InitOptions',
    'MetamorphOptions',
    'OpenStorageOptions',
    'RebootOptions',
    'RecordOptions',
    'SetStatusMessageOptions',
    'StartOptions',
    'WebhookOptions',

    # Webhook
    'Webhook',
    'WebhookEventType',
    # Pricing
    'PricingModel',
    'FreeActorPricingInfo',
    'FlatPricePerMonthActorPricingInfo',
    'PricePerDatasetItemActorPricingInfo',
    'PayPerEventActorPricingInfo',
    'PricingPerEvent',
    'ActorPricingInfo',
    'ActorChargeEvent',
    'ActorPricingInfoResult',
    # Errors
    'ScrapelyClientError',
    'ScrapelyApiError',
    'InvalidResponseBodyError',
    'InvalidRequestError',
    'UnauthorizedError',
    'ForbiddenError',
    'NotFoundError',
    'ConflictError',
    'RateLimitError',
    'ServerError',
    'raise_for_status',
]
