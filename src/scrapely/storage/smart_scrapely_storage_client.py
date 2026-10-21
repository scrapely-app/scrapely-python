# SPDX-License-Identifier: Apache-2.0
# Derived from apify-sdk-python (src/apify/storage_clients/_smart_apify/_storage_client.py),
# Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
# Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
"""Smart storage client that selects cloud or local based on environment."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from typing_extensions import override

from crawlee.storage_clients._base import DatasetClient, KeyValueStoreClient, RequestQueueClient, StorageClient

from scrapely.configuration import Configuration as ScrapelyConfiguration

from ._file_system import ScrapelyFileSystemStorageClient
from .scrapely_storage_client import ScrapelyStorageClient

if TYPE_CHECKING:
    from collections.abc import Hashable

    from crawlee.configuration import Configuration as CrawleeConfiguration


class SmartScrapelyStorageClient(StorageClient):
    """Selects Scrapely cloud or local FS storage based on environment.

    When running on the Scrapely platform
    (``is_at_home``), uses ``cloud_storage_client``. Locally, uses ``local_storage_client``.
    Pass ``force_cloud=True`` to use cloud storage from a local machine.
    """

    def __init__(
        self,
        *,
        cloud_storage_client: Optional[StorageClient] = None,
        local_storage_client: Optional[StorageClient] = None,
    ) -> None:
        self._cloud_storage_client = cloud_storage_client or ScrapelyStorageClient()
        self._local_storage_client = local_storage_client or ScrapelyFileSystemStorageClient()

    def __str__(self) -> str:
        return (
            f'{self.__class__.__name__}(cloud_storage_client={self._cloud_storage_client.__class__.__name__},'
            f' local_storage_client={self._local_storage_client.__class__.__name__})'
        )

    @override
    def get_storage_client_cache_key(self, configuration: CrawleeConfiguration) -> Hashable:
        if ScrapelyConfiguration.get_global_config().is_at_home:
            return self._cloud_storage_client.get_storage_client_cache_key(configuration)
        return self._local_storage_client.get_storage_client_cache_key(configuration)

    @override
    async def create_dataset_client(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        configuration: Optional[CrawleeConfiguration] = None,
    ) -> DatasetClient:
        return await self.get_suitable_storage_client().create_dataset_client(
            id=id, name=name, alias=alias, configuration=configuration,
        )

    @override
    async def create_kvs_client(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        configuration: Optional[CrawleeConfiguration] = None,
    ) -> KeyValueStoreClient:
        return await self.get_suitable_storage_client().create_kvs_client(
            id=id, name=name, alias=alias, configuration=configuration,
        )

    @override
    async def create_rq_client(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        configuration: Optional[CrawleeConfiguration] = None,
    ) -> RequestQueueClient:
        return await self.get_suitable_storage_client().create_rq_client(
            id=id, name=name, alias=alias, configuration=configuration,
        )

    def get_suitable_storage_client(self, *, force_cloud: bool = False) -> StorageClient:
        """Return cloud client when at home or ``force_cloud``, else local FS."""
        scrapely_config = ScrapelyConfiguration.get_global_config()
        if scrapely_config.is_at_home:
            return self._cloud_storage_client

        if force_cloud:
            cloud = self._cloud_storage_client
            has_token = scrapely_config.token is not None
            if isinstance(cloud, ScrapelyStorageClient):
                has_token = has_token or cloud.has_credentials()
            if not has_token:
                raise RuntimeError(
                    'In order to use Scrapely cloud storage from your computer, '
                    'you need to provide a token using the SCRAPELY_TOKEN / APIFY_TOKEN '
                    'environment variable.'
                )
            return self._cloud_storage_client

        return self._local_storage_client
