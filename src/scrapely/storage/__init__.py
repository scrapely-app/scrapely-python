"""Scrapely storage: Crawlee StorageClient backends + HTTP helpers."""

from typing import TYPE_CHECKING, Any

# Only the Crawlee-free HTTP helpers load eagerly, so ``Actor`` can use
# ``scrapely.storage.api_client`` / ``.client`` without importing Crawlee.
# The Crawlee StorageClient backends load on first attribute access.
from .api_client import ApiClient, ApiClientError
from .client import ApiStorageClient

_LAZY_ATTRS = {
    'Request': ('crawlee', 'Request'),
    'MemoryStorageClient': ('crawlee.storage_clients', 'MemoryStorageClient'),
    'StorageClient': ('crawlee.storage_clients', 'StorageClient'),
    'Dataset': ('crawlee.storages', 'Dataset'),
    'KeyValueStore': ('crawlee.storages', 'KeyValueStore'),
    'RequestQueue': ('crawlee.storages', 'RequestQueue'),
    'ScrapelyFileSystemStorageClient': ('scrapely.storage._file_system', 'ScrapelyFileSystemStorageClient'),
    # Public alias
    'FileSystemStorageClient': ('scrapely.storage._file_system', 'ScrapelyFileSystemStorageClient'),
    'ScrapelyStorageClient': ('scrapely.storage.scrapely_storage_client', 'ScrapelyStorageClient'),
    'SmartScrapelyStorageClient': ('scrapely.storage.smart_scrapely_storage_client', 'SmartScrapelyStorageClient'),
    **{
        name: ('scrapely.storage.patch_storage', name)
        for name in (
            'disconnect_storage',
            'get_api_storage_factory',
            'get_auto_saved_value',
            'get_smart_storage_client',
            'is_api_storage_configured',
            'persist_autosaved_values',
            'set_api_storage',
            'set_storage_client',
        )
    },
}

# Submodules that used to be bound as attributes by this package's eager imports.
_LAZY_SUBMODULES = frozenset({
    '_file_system',
    '_metadata',
    'patch_storage',
    'scrapely_dataset_client',
    'scrapely_kvs_client',
    'scrapely_rq_client',
    'scrapely_storage_client',
    'smart_scrapely_storage_client',
    'utils',
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


if TYPE_CHECKING:
    from crawlee import Request
    from crawlee.storage_clients import MemoryStorageClient, StorageClient
    from crawlee.storages import Dataset, KeyValueStore, RequestQueue

    from ._file_system import ScrapelyFileSystemStorageClient
    from ._file_system import ScrapelyFileSystemStorageClient as FileSystemStorageClient
    from .patch_storage import (
        disconnect_storage,
        get_api_storage_factory,
        get_auto_saved_value,
        get_smart_storage_client,
        is_api_storage_configured,
        persist_autosaved_values,
        set_api_storage,
        set_storage_client,
    )
    from .scrapely_storage_client import ScrapelyStorageClient
    from .smart_scrapely_storage_client import SmartScrapelyStorageClient

__all__ = [
    'ApiClient',
    'ApiClientError',
    'ApiStorageClient',
    'ScrapelyStorageClient',
    'SmartScrapelyStorageClient',
    'FileSystemStorageClient',
    'ScrapelyFileSystemStorageClient',
    'MemoryStorageClient',
    'StorageClient',
    'set_api_storage',
    'set_storage_client',
    'get_api_storage_factory',
    'get_smart_storage_client',
    'is_api_storage_configured',
    'disconnect_storage',
    'get_auto_saved_value',
    'persist_autosaved_values',
    'Dataset',
    'KeyValueStore',
    'RequestQueue',
    'Request',
]
