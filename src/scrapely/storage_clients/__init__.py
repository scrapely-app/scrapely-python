"""Storage clients.

The source transform rewrites module paths only, so the legacy class names are
provided as aliases of the Scrapely clients.
"""

from crawlee.storage_clients import MemoryStorageClient

from scrapely.storage import (
    FileSystemStorageClient,
    ScrapelyStorageClient,
    SmartScrapelyStorageClient,
)

ApifyStorageClient = ScrapelyStorageClient
SmartApifyStorageClient = SmartScrapelyStorageClient

__all__ = [
    'ApifyStorageClient',
    'FileSystemStorageClient',
    'MemoryStorageClient',
    'ScrapelyStorageClient',
    'SmartApifyStorageClient',
    'SmartScrapelyStorageClient',
]
