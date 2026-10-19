"""Scrapely cloud KeyValueStoreClient for Crawlee."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, MutableMapping, Optional, TypeVar

from typing_extensions import Self, override

from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import (
    KeyValueStoreMetadata,
    KeyValueStoreRecord,
    KeyValueStoreRecordMetadata,
)

from .client import ApiStorageClient
from ._metadata import resolve_storage_id, to_kvs_metadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from crawlee._types import JsonSerializable

T = TypeVar('T')


def _old_kvs_to_crawlee(meta: Any) -> KeyValueStoreMetadata:
    return to_kvs_metadata(
        {
            'id': meta.id,
            'name': meta.name,
            'createdAt': meta.created_at.isoformat() if getattr(meta, 'created_at', None) else None,
            'accessedAt': meta.accessed_at.isoformat() if getattr(meta, 'accessed_at', None) else None,
            'modifiedAt': meta.modified_at.isoformat() if getattr(meta, 'modified_at', None) else None,
        },
        meta.id,
    )


class ScrapelyKeyValueStoreClient(KeyValueStoreClient):
    """Key-value store client backed by Scrapely `/v2/key-value-stores`."""

    def __init__(self, *, api: ApiStorageClient, metadata: KeyValueStoreMetadata) -> None:
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
        meta = await api.get_or_create_key_value_store(id=open_id, name=open_name)
        return cls(api=api, metadata=_old_kvs_to_crawlee(meta))

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        meta = await self._api.kvs_get(self._metadata.id)
        self._metadata = _old_kvs_to_crawlee(meta)
        return self._metadata

    @override
    async def drop(self) -> None:
        await self._api.kvs_delete(self._metadata.id)

    @override
    async def purge(self) -> None:
        async for record in self.iterate_keys():
            await self.delete_value(key=record.key)

    @override
    async def get_value(self, *, key: str) -> Optional[KeyValueStoreRecord]:
        value, content_type = await self._api.kvs_get_record(self._metadata.id, key)
        if value is None and content_type is None:
            return None
        return KeyValueStoreRecord(
            key=key,
            value=value,
            content_type=content_type or 'application/octet-stream',
        )

    @override
    async def set_value(self, *, key: str, value: Any, content_type: Optional[str] = None) -> None:
        if value is None:
            await self.delete_value(key=key)
            return
        await self._api.kvs_set_record(self._metadata.id, key, value, content_type)

    @override
    async def delete_value(self, *, key: str) -> None:
        await self._api.kvs_delete_record(self._metadata.id, key)

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        start = exclusive_start_key
        yielded = 0
        while True:
            page_limit = 1000
            if limit is not None:
                remaining = limit - yielded
                if remaining <= 0:
                    break
                page_limit = min(1000, remaining)

            data = await self._api.kvs_list_keys(
                self._metadata.id,
                exclusive_start_key=start,
                limit=page_limit,
            )
            items = data.get('items') or []
            if not items:
                break
            for item in items:
                key = item.get('key')
                if key is None:
                    continue
                yield KeyValueStoreRecordMetadata(
                    key=str(key),
                    content_type=str(item.get('contentType') or 'application/octet-stream'),
                    size=int(item.get('size') or 0) if item.get('size') is not None else None,
                )
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            if not data.get('isTruncated'):
                break
            start = data.get('nextExclusiveStartKey')
            if not start:
                break

    @override
    async def get_public_url(self, *, key: str) -> str:
        exists = await self.record_exists(key=key)
        if not exists:
            raise ValueError(f'Record with key "{key}" does not exist')
        return self._api.kvs_public_url(self._metadata.id, key)

    @override
    async def record_exists(self, *, key: str) -> bool:
        return await self._api.kvs_record_exists(self._metadata.id, key)

    async def get_auto_saved_value(
        self,
        key: str,
        default_value: Optional[MutableMapping[str, 'JsonSerializable']] = None,
    ) -> MutableMapping[str, 'JsonSerializable']:
        """Get a value that will be automatically saved on changes.

        Args:
            key: Key of the record to store the value.
            default_value: Default value if record doesn't exist.

        Returns:
            The stored value or the default value.
        """
        record = await self.get_value(key=key)
        if record is None:
            # Store and return the default value
            value = default_value if default_value is not None else {}
            await self.set_value(key=key, value=value)
            return value
        value = record.value
        if not isinstance(value, dict):
            # If stored value isn't a dict, return it wrapped or default
            return default_value if default_value is not None else {}
        return value

    async def persist_autosaved_values(self) -> None:
        """Force autosaved values to be saved.

        Note: In this implementation, values are saved immediately on change,
        so this method is a no-op. It exists for API compatibility.
        """
        # Values are saved immediately in set_value, so nothing to do here
        pass

    async def list_keys(
        self,
        *,
        exclusive_start_key: Optional[str] = None,
        limit: int = 1000,
    ) -> list[KeyValueStoreRecordMetadata]:
        """List all keys in the store.

        Args:
            exclusive_start_key: Key to start iteration from.
            limit: Maximum number of keys to return.

        Returns:
            List of KeyValueStoreRecordMetadata objects.
        """
        result: list[KeyValueStoreRecordMetadata] = []
        async for record in self.iterate_keys(
            exclusive_start_key=exclusive_start_key,
            limit=limit,
        ):
            result.append(record)
        return result
