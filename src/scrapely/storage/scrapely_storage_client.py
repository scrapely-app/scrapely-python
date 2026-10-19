"""Scrapely cloud StorageClient for Crawlee (like the TS ApiStorageClientFactory)."""

from __future__ import annotations

import os
from typing import Any, Literal, Optional

from typing_extensions import override

from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from scrapely.configuration import Configuration as ScrapelyConfiguration

from .api_client import ApiClient
from .client import ApiStorageClient
from .scrapely_dataset_client import ScrapelyDatasetClient
from .scrapely_kvs_client import ScrapelyKeyValueStoreClient
from .scrapely_rq_client import ScrapelyRequestQueueClient


class ScrapelyStorageClient(StorageClient):
    """Cloud storage client that persists datasets, KVS, and RQs via Scrapely ``/v2``.

    Mirrors the TypeScript ``ApiStorageClientFactory``.
    The Scrapely server backs these endpoints with Dragonfly; actors never open Dragonfly directly.

    ``request_queue_access`` is accepted for API compatibility. Scrapely ``/v2`` uses a single
    head-lock RQ client (same as TypeScript); ``shared`` currently behaves like ``single``.
    """

    def __init__(
        self,
        *,
        request_queue_access: Literal['single', 'shared'] = 'single',
        api_url: Optional[str] = None,
        token: Optional[str] = None,
        run_id: Optional[str] = None,
        api: Optional[ApiStorageClient] = None,
        transport: Any = None,
    ) -> None:
        self._request_queue_access = request_queue_access
        self._api_url = api_url
        self._token = token
        self._run_id = run_id
        self._transport = transport
        self._api = api

    def has_credentials(self) -> bool:
        if self._api is not None:
            return True
        url, token, run_id = self._resolve_credentials()
        return bool(url and token and run_id)

    def _resolve_credentials(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        scrapely = ScrapelyConfiguration.get_global_config()
        url = (
            self._api_url
            or scrapely.get('api_base_url')
            or os.environ.get('SCRAPELY_API_URL')
            or os.environ.get('APIFY_API_BASE_URL')
        )
        token = (
            self._token
            or scrapely.token
            or os.environ.get('SCRAPELY_TOKEN')
            or os.environ.get('APIFY_TOKEN')
        )
        run_id = (
            self._run_id
            or scrapely.actor_run_id
            or os.environ.get('ACTOR_RUN_ID')
            or os.environ.get('APIFY_ACTOR_RUN_ID')
        )
        return url, token, run_id

    def _ensure_api(self) -> ApiStorageClient:
        if self._api is not None:
            return self._api
        url, token, run_id = self._resolve_credentials()
        if not url or not token or not run_id:
            raise ValueError(
                'ScrapelyStorageClient requires api_url, token, and run_id '
                '(or SCRAPELY_API_URL / SCRAPELY_TOKEN / ACTOR_RUN_ID env vars).'
            )
        self._api = ApiStorageClient(
            api_url=url,
            token=token,
            run_id=run_id,
            transport=self._transport,
        )
        return self._api

    @property
    def api(self) -> ApiStorageClient:
        return self._ensure_api()

    @property
    def api_client(self) -> ApiClient:
        return self._ensure_api().api_client

    async def close(self) -> None:
        if self._api is not None:
            await self._api.close()

    def get_storage_client_cache_key(self, configuration: Configuration) -> Any:
        run_id = self._run_id or self._resolve_credentials()[2] or 'unknown'
        return (
            f'{self.__class__.__module__}.{self.__class__.__name__}',
            run_id,
            self._request_queue_access,
        )

    def _default_dataset_id(self) -> Optional[str]:
        return (
            os.environ.get('ACTOR_DEFAULT_DATASET_ID')
            or os.environ.get('APIFY_DEFAULT_DATASET_ID')
        )

    def _default_kvs_id(self) -> Optional[str]:
        return (
            os.environ.get('ACTOR_DEFAULT_KEY_VALUE_STORE_ID')
            or os.environ.get('APIFY_DEFAULT_KEY_VALUE_STORE_ID')
        )

    def _default_rq_id(self) -> Optional[str]:
        return (
            os.environ.get('ACTOR_DEFAULT_REQUEST_QUEUE_ID')
            or os.environ.get('APIFY_DEFAULT_REQUEST_QUEUE_ID')
        )

    @override
    async def create_dataset_client(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        configuration: Optional[Configuration] = None,
    ) -> ScrapelyDatasetClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await ScrapelyDatasetClient.open(
            api=self._ensure_api(),
            id=id,
            name=name,
            alias=alias,
            default_id=self._default_dataset_id(),
        )
        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_kvs_client(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        configuration: Optional[Configuration] = None,
    ) -> ScrapelyKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()
        client = await ScrapelyKeyValueStoreClient.open(
            api=self._ensure_api(),
            id=id,
            name=name,
            alias=alias,
            default_id=self._default_kvs_id(),
        )
        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_rq_client(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        configuration: Optional[Configuration] = None,
    ) -> ScrapelyRequestQueueClient:
        configuration = configuration or Configuration.get_global_configuration()
        # Scrapely /v2 uses one head-lock client (TS parity); access mode is API-compat only.
        _ = self._request_queue_access
        client = await ScrapelyRequestQueueClient.open(
            api=self._ensure_api(),
            id=id,
            name=name,
            alias=alias,
            default_id=self._default_rq_id(),
        )
        await self._purge_if_needed(client, configuration)
        return client
