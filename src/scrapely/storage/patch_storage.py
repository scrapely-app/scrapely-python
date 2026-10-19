"""Register Scrapely storage with Crawlee's service locator (TS setApiStorage equivalent)."""

from __future__ import annotations

import logging
from typing import Any, Dict, MutableMapping, Optional, TypedDict, cast

from crawlee import service_locator
from crawlee.storage_clients._base import StorageClient

from ._file_system import ScrapelyFileSystemStorageClient
from .scrapely_storage_client import ScrapelyStorageClient
from .smart_scrapely_storage_client import SmartScrapelyStorageClient

logger = logging.getLogger(__name__)

_api_factory: Optional[ScrapelyStorageClient] = None
_smart_client: Optional[SmartScrapelyStorageClient] = None

# Cache for auto-saved values: store_id -> key -> value
# This allows get_auto_saved_value to return the same object reference,
# and persist_state listeners can iterate and save all cached values.
_auto_save_cache: Dict[str, Dict[str, Any]] = {}

# Track which stores have registered persistState listeners
_persist_state_registered: set = set()


class ApiStorageConfig(TypedDict, total=False):
    api_url: str
    token: str
    run_id: str
    transport: Any


def set_api_storage(
    api_config: Optional[ApiStorageConfig] = None,
    *,
    cloud_storage_client: Optional[ScrapelyStorageClient] = None,
    local_storage_client: Optional[StorageClient] = None,
) -> SmartScrapelyStorageClient:
    """Configure Crawlee to use Scrapely storage (Smart wrapper + ``/v2`` cloud).

    Equivalent to TypeScript ``setApiStorage()`` plus registering a ``SmartScrapelyStorageClient``
    on the service locator.
    """
    global _api_factory, _smart_client

    if cloud_storage_client is not None:
        cloud = cloud_storage_client
    elif api_config is not None:
        cloud = ScrapelyStorageClient(
            api_url=api_config.get('api_url'),
            token=api_config.get('token'),
            run_id=api_config.get('run_id'),
            transport=api_config.get('transport'),
        )
    else:
        cloud = ScrapelyStorageClient()

    _api_factory = cloud
    local = local_storage_client or ScrapelyFileSystemStorageClient()
    smart = SmartScrapelyStorageClient(
        cloud_storage_client=cloud,
        local_storage_client=local,
    )
    _smart_client = smart
    service_locator.set_storage_client(smart)
    return smart


def set_storage_client(client: StorageClient) -> SmartScrapelyStorageClient:
    """Register a storage client; wrap non-Smart clients in the Smart client."""
    global _api_factory, _smart_client

    if isinstance(client, SmartScrapelyStorageClient):
        smart = client
    elif isinstance(client, ScrapelyStorageClient):
        _api_factory = client
        smart = SmartScrapelyStorageClient(
            cloud_storage_client=client,
            local_storage_client=ScrapelyFileSystemStorageClient(),
        )
    else:
        smart = SmartScrapelyStorageClient(
            cloud_storage_client=_api_factory or ScrapelyStorageClient(),
            local_storage_client=client,
        )

    _smart_client = smart
    service_locator.set_storage_client(smart)
    return smart


def get_api_storage_factory() -> Optional[ScrapelyStorageClient]:
    return _api_factory


def get_smart_storage_client() -> Optional[SmartScrapelyStorageClient]:
    return _smart_client


def is_api_storage_configured() -> bool:
    return _api_factory is not None


def disconnect_storage() -> None:
    global _api_factory, _smart_client, _auto_save_cache, _persist_state_registered
    _api_factory = None
    _smart_client = None
    _auto_save_cache.clear()
    _persist_state_registered.clear()


def _ensure_persist_state_listener(store_id: str, config: Any) -> None:
    """Register a persistState listener for a store if not already registered.
    
    This listener saves all cached values for that store when persistState is emitted.
    """
    if store_id in _persist_state_registered:
        return
    
    event_manager = config.get_event_manager() if hasattr(config, 'get_event_manager') else None
    if event_manager is None:
        logger.warning('No event manager available for persistState listener')
        return
    
    async def on_persist_state(_: Any) -> None:
        store_cache = _auto_save_cache.get(store_id)
        if not store_cache:
            return
        
        factory = _api_factory
        if not factory:
            return
        
        try:
            api = factory._ensure_api()
            for key, value in store_cache.items():
                try:
                    await api.kvs_set_record(store_id, key, value)
                except Exception as e:
                    logger.warning('Failed to persist state value to %s: %s', key, e)
        except Exception as e:
            logger.warning('Failed to persist auto-saved values: %s', e)
    
    event_manager.on('persistState', on_persist_state)
    _persist_state_registered.add(store_id)


async def get_auto_saved_value(
    key: str,
    default_value: MutableMapping[str, Any],
    *,
    store_id: Optional[str] = None,
) -> MutableMapping[str, Any]:
    """Get a value from KVS that will be automatically saved on changes.
    
    This implementation matches Crawlee's approach:
    1. Check cache first (return same object reference for subsequent calls)
    2. Fetch value from storage if not cached
    3. Cache the value (so mutations are tracked)
    4. Register persistState listener to save cached values
    
    Args:
        key: The key of the record to get
        default_value: The default value to return and store if the key doesn't exist
        store_id: Optional store ID (uses default KVS if not provided)
        
    Returns:
        The stored value or the default_value
    """
    from scrapely.configuration import Configuration as ScrapelyConfiguration
    
    config = ScrapelyConfiguration.get_global_config()
    
    # Resolve store ID from environment if not provided
    if store_id is None:
        store_id = (
            config.get('default_key_value_store_id')
            or config.actor_run_id
        )
    
    if store_id is None:
        raise ValueError('No key-value store ID available for get_auto_saved_value')
    
    # Check if we have a cached value for this store/key
    store_cache = _auto_save_cache.get(store_id)
    if store_cache is not None and key in store_cache:
        return cast(MutableMapping[str, Any], store_cache[key])
    
    # Fetch value from storage
    factory = _api_factory
    if factory is None:
        raise ValueError('API storage client not initialized. Call set_api_storage() first.')
    
    api = factory._ensure_api()
    
    # Try to get existing value
    value, content_type = await api.kvs_get_record(store_id, key)
    
    if value is None:
        # Store the default value and return it
        await api.kvs_set_record(store_id, key, default_value)
        value = default_value
    elif not isinstance(value, MutableMapping):
        # If the stored value is not a mutable mapping, we can't track mutations
        # Return it as-is but don't cache for auto-save
        logger.warning(
            'get_auto_saved_value: stored value for key "%s" is not a mutable mapping, '
            'auto-save will not work',
            key,
        )
        return cast(MutableMapping[str, Any], value)
    
    # Initialize store cache if needed
    if store_cache is None:
        store_cache = {}
        _auto_save_cache[store_id] = store_cache
    
    # Check cache again (race condition: another call might have cached it)
    if key in store_cache:
        return cast(MutableMapping[str, Any], store_cache[key])
    
    # Cache the value - mutations to this object will be persisted
    store_cache[key] = value
    
    # Register persistState listener for this store
    _ensure_persist_state_listener(store_id, config)
    
    return cast(MutableMapping[str, Any], value)


async def persist_autosaved_values(*, store_id: Optional[str] = None) -> None:
    """Force autosaved values to be saved without waiting for an event.
    
    Args:
        store_id: Optional store ID to persist. If None, persists all stores.
    """
    factory = _api_factory
    if factory is None:
        return
    
    api = factory._ensure_api()
    
    stores_to_persist = [store_id] if store_id else list(_auto_save_cache.keys())
    
    for sid in stores_to_persist:
        store_cache = _auto_save_cache.get(sid)
        if not store_cache:
            continue
        
        for key, value in store_cache.items():
            try:
                await api.kvs_set_record(sid, key, value)
            except Exception as e:
                logger.warning('Failed to persist state value to %s: %s', key, e)
