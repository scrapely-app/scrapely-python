"""Scrapely cloud DatasetClient for Crawlee."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Mapping, Optional, Sequence, Union

from typing_extensions import Self, override

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from .client import ApiStorageClient
from ._metadata import resolve_storage_id, to_dataset_metadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee._types import JsonSerializable
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import StorageClient


def _old_dataset_to_crawlee(meta: Any) -> DatasetMetadata:
    return to_dataset_metadata(
        {
            'id': meta.id,
            'name': meta.name,
            'createdAt': meta.created_at.isoformat() if getattr(meta, 'created_at', None) else None,
            'accessedAt': meta.accessed_at.isoformat() if getattr(meta, 'accessed_at', None) else None,
            'modifiedAt': meta.modified_at.isoformat() if getattr(meta, 'modified_at', None) else None,
            'itemCount': getattr(meta, 'item_count', 0),
        },
        meta.id,
    )


class ScrapelyDatasetClient(DatasetClient):
    """Dataset client backed by Scrapely `/v2/datasets`."""

    def __init__(self, *, api: ApiStorageClient, metadata: DatasetMetadata) -> None:
        self._api = api
        self._metadata = metadata

    @property
    def id(self) -> str:
        """Get the storage ID."""
        return self._metadata.id

    @property
    def name(self) -> Optional[str]:
        """Get the storage name."""
        return self._metadata.name

    @classmethod
    async def open(
        cls,
        *,
        api: ApiStorageClient,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        default_id: Optional[str] = None,
    ) -> Self:
        open_id, open_name = resolve_storage_id(
            id=id, name=name, alias=alias, default_id=default_id,
        )
        meta = await api.get_or_create_dataset(id=open_id, name=open_name)
        return cls(api=api, metadata=_old_dataset_to_crawlee(meta))

    @override
    async def get_metadata(self) -> DatasetMetadata:
        meta = await self._api.dataset_get(self._metadata.id)
        self._metadata = _old_dataset_to_crawlee(meta)
        return self._metadata

    @override
    async def drop(self) -> None:
        await self._api.dataset_delete(self._metadata.id)

    @override
    async def purge(self) -> None:
        if not self._metadata.name:
            raise NotImplementedError(
                'Dataset.purge() is not supported for unnamed/default datasets on Scrapely '
                '(no clear endpoint). Use a named dataset or drop() and reopen.'
            )
        name = self._metadata.name
        await self.drop()
        meta = await self._api.get_or_create_dataset(name=name)
        self._metadata = _old_dataset_to_crawlee(meta)

    @override
    async def push_data(
        self,
        data: Union[Sequence[Mapping[str, 'JsonSerializable']], Mapping[str, 'JsonSerializable']],
    ) -> None:
        items: List[Mapping[str, Any]]
        if isinstance(data, Mapping):
            items = [data]
        else:
            items = list(data)
        # Charge-limit safety net: a pay-per-event run past its spending limit
        # must not store unbilled rows in its default dataset.
        from ..charging import default_dataset_id_from_env, refuse_default_dataset_push

        if self._metadata.id == default_dataset_id_from_env() and refuse_default_dataset_push(len(items)):
            return
        await self._api.dataset_push_items(self._metadata.id, items)

    @override
    async def get_data(
        self,
        *,
        offset: int = 0,
        limit: Optional[int] = 999_999_999_999,
        clean: bool = False,
        desc: bool = False,
        fields: Optional[List[str]] = None,
        omit: Optional[List[str]] = None,
        unwind: Optional[List[str]] = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
        flatten: Optional[List[str]] = None,
        view: Optional[str] = None,
    ) -> DatasetItemsListPage:
        _, page = await self._api.dataset_list_items(
            self._metadata.id,
            offset=offset,
            limit=None if limit == 999_999_999_999 else limit,
            clean=clean,
            desc=desc,
            fields=fields,
            omit=omit,
            unwind=unwind,
            skip_empty=skip_empty,
            skip_hidden=skip_hidden,
            flatten=flatten,
            view=view,
        )
        return DatasetItemsListPage(
            count=page.count,
            offset=page.offset,
            limit=page.limit,
            total=page.total,
            desc=page.desc,
            items=list(page.items),
        )

    @override
    async def iterate_items(
        self,
        *,
        offset: int = 0,
        limit: Optional[int] = None,
        clean: bool = False,
        desc: bool = False,
        fields: Optional[List[str]] = None,
        omit: Optional[List[str]] = None,
        unwind: Optional[List[str]] = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> AsyncIterator[Mapping[str, 'JsonSerializable']]:
        current = offset
        remaining = limit
        chunk = 1000
        while True:
            fetch = chunk if remaining is None else min(chunk, remaining)
            if fetch <= 0:
                break
            page = await self.get_data(
                offset=current,
                limit=fetch,
                clean=clean,
                desc=desc,
                fields=fields,
                omit=omit,
                unwind=unwind,
                skip_empty=skip_empty,
                skip_hidden=skip_hidden,
            )
            if not page.items:
                break
            for item in page.items:
                yield item
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return
            current += len(page.items)
            if len(page.items) < fetch:
                break

    async def list_items(
        self,
        *,
        offset: int = 0,
        limit: Optional[int] = None,
        clean: bool = False,
        desc: bool = False,
        fields: Optional[List[str]] = None,
        omit: Optional[List[str]] = None,
        unwind: Optional[List[str]] = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> list[Mapping[str, 'JsonSerializable']]:
        """List all items in the dataset.

        Args:
            offset: Number of items to skip at the start.
            limit: Maximum number of items to return.
            clean: Return only non-empty items, exclude hidden fields.
            desc: Sort in descending order.
            fields: Fields to include in each item.
            omit: Fields to exclude from each item.
            unwind: Unwind items by a specified array field.
            skip_empty: Exclude empty items.
            skip_hidden: Exclude fields starting with '#'.
            flatten: Fields to flatten in returned items.

        Returns:
            List of items as dictionaries.
        """
        result: list[Mapping[str, 'JsonSerializable']] = []
        async for item in self.iterate_items(
            offset=offset,
            limit=limit,
            clean=clean,
            desc=desc,
            fields=fields,
            omit=omit,
            unwind=unwind,
            skip_empty=skip_empty,
            skip_hidden=skip_hidden,
        ):
            result.append(item)
        return result

    async def export_to(
        self,
        key: str,
        *,
        content_type: str = 'json',
        to_kvs_id: Optional[str] = None,
        to_kvs_name: Optional[str] = None,
        to_kvs_storage_client: Optional['StorageClient'] = None,
        to_kvs_configuration: Optional['Configuration'] = None,
        **kwargs: Any,
    ) -> None:
        """Export the entire dataset to a key-value store.

        Args:
            key: Key under which to save the data in the KVS.
            content_type: Format for export ('json' or 'csv').
            to_kvs_id: ID of the target key-value store.
            to_kvs_name: Name of the target key-value store.
            to_kvs_storage_client: Storage client for the target KVS.
            to_kvs_configuration: Configuration for the target KVS.
            **kwargs: Additional parameters for the export (e.g., JSON/CSV options).
        """
        import json
        import csv
        import io

        # Get all items from the dataset
        items = await self.list_items(
            clean=kwargs.get('clean', False),
            skip_empty=kwargs.get('skip_empty', False),
            skip_hidden=kwargs.get('skip_hidden', False),
        )

        # Determine target KVS - use this API client's KVS by default
        if to_kvs_id or to_kvs_name:
            target_kvs_meta = await self._api.get_or_create_key_value_store(
                id=to_kvs_id,
                name=to_kvs_name,
            )
            target_kvs_id = target_kvs_meta.id
        else:
            # Use default KVS
            target_kvs_meta = await self._api.get_or_create_key_value_store()
            target_kvs_id = target_kvs_meta.id

        # Format the data
        if content_type == 'csv':
            output = io.StringIO()
            if items:
                writer = csv.DictWriter(
                    output,
                    fieldnames=items[0].keys() if items else [],
                    **{k: v for k, v in kwargs.items() if k in [
                        'dialect', 'delimiter', 'doublequote', 'escapechar',
                        'lineterminator', 'quotechar', 'quoting', 'skipinitialspace', 'strict'
                    ]},
                )
                writer.writeheader()
                writer.writerows(items)
            value = output.getvalue()
            ct = 'text/csv'
        else:
            # JSON format
            value = json.dumps(
                items,
                **{k: v for k, v in kwargs.items() if k in [
                    'skipkeys', 'ensure_ascii', 'check_circular', 'allow_nan',
                    'cls', 'indent', 'separators', 'default', 'sort_keys'
                ]},
            )
            ct = 'application/json'

        # Store in KVS
        await self._api.kvs_set_record(target_kvs_id, key, value, ct)
