# SPDX-License-Identifier: Apache-2.0
# Derived from apify-sdk-python (src/apify/storage_clients/_file_system/_storage_client.py),
# Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
# Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
"""Scrapely file-system StorageClient."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from typing_extensions import override

from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient

from ._key_value_store_client import ScrapelyFileSystemKeyValueStoreClient

if TYPE_CHECKING:
    from collections.abc import Hashable

    from crawlee.storage_clients._file_system import FileSystemKeyValueStoreClient


class ScrapelyFileSystemStorageClient(FileSystemStorageClient):
    """File-system storage client with Scrapely KVS purge that preserves INPUT.

    The only difference from Crawlee's ``FileSystemStorageClient`` is that it uses
    ``ScrapelyFileSystemKeyValueStoreClient`` for key-value stores, which overrides
    ``purge`` to keep the metadata file and the INPUT file.
    """

    @override
    def get_storage_client_cache_key(self, configuration: Configuration) -> Hashable:
        # Same cache key as FileSystemStorageClient to avoid double-purge.
        return FileSystemStorageClient().get_storage_client_cache_key(configuration)

    @override
    async def create_kvs_client(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        configuration: Optional[Configuration] = None,
    ) -> FileSystemKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await ScrapelyFileSystemKeyValueStoreClient.open(
            id=id,
            name=name,
            alias=alias,
            configuration=configuration,
        )
        await self._purge_if_needed(client, configuration)
        return client
