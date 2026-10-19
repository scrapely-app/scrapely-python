"""File-system KVS client that preserves INPUT on purge."""

from __future__ import annotations

import asyncio
import json
import logging
from itertools import chain
from pathlib import Path
from typing import Any, Optional

from typing_extensions import Self, override

from crawlee._consts import METADATA_FILENAME
from crawlee._utils.file import atomic_write, infer_mime_type, json_dumps
from crawlee.configuration import Configuration as CrawleeConfiguration
from crawlee.storage_clients._file_system import FileSystemKeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

from scrapely.configuration import Configuration as ScrapelyConfiguration

logger = logging.getLogger(__name__)


class ScrapelyFileSystemKeyValueStoreClient(FileSystemKeyValueStoreClient):
    """File-system KVS that keeps metadata and INPUT when purged."""

    def __init__(
        self,
        *,
        metadata: KeyValueStoreMetadata,
        path_to_kvs: Path,
        lock: asyncio.Lock,
    ) -> None:
        super().__init__(metadata=metadata, path_to_kvs=path_to_kvs, lock=lock)
        global_configuration = ScrapelyConfiguration.get_global_config()
        self._input_key = global_configuration.input_key
        self._input_key_filename = global_configuration.input_key

    @override
    @classmethod
    async def open(
        cls,
        *,
        id: Optional[str],
        name: Optional[str],
        alias: Optional[str],
        configuration: CrawleeConfiguration,
    ) -> Self:
        client = await super().open(id=id, name=name, alias=alias, configuration=configuration)

        scrapely_config = ScrapelyConfiguration.get_global_config()
        client._input_key = scrapely_config.input_key  # noqa: SLF001
        input_key_filename = cls._get_input_key_file_name(
            path_to_kvs=client.path_to_kvs,
            input_key=scrapely_config.input_key,
        )
        client._input_key_filename = input_key_filename  # noqa: SLF001
        input_file_path = client.path_to_kvs / input_key_filename
        input_file_metadata_path = client.path_to_kvs / f'{input_file_path.name}.{METADATA_FILENAME}'
        if input_file_path.exists() and not input_file_metadata_path.exists():
            await cls._create_missing_metadata_for_input_file(
                key=scrapely_config.input_key,
                record_path=input_file_path,
            )

        return client

    @override
    async def purge(self) -> None:
        """Delete all files except store metadata and INPUT (+ its metadata sidecar)."""
        async with self._lock:
            files_to_keep = {
                self._input_key_filename,
                f'{self._input_key_filename}.{METADATA_FILENAME}',
                METADATA_FILENAME,
            }

            async with asyncio.TaskGroup() as tg:
                for file_path in self.path_to_kvs.glob('*'):
                    if file_path.name in files_to_keep:
                        continue
                    if file_path.is_file():
                        tg.create_task(asyncio.to_thread(file_path.unlink, missing_ok=True))

            await self._update_metadata(
                update_accessed_at=True,
                update_modified_at=True,
            )

    @override
    async def get_value(self, *, key: str) -> Optional[KeyValueStoreRecord]:
        return await super().get_value(key=self._resolve_input_key(key))

    @override
    async def set_value(self, *, key: str, value: Any, content_type: Optional[str] = None) -> None:
        await super().set_value(key=self._resolve_input_key(key), value=value, content_type=content_type)

    @override
    async def record_exists(self, *, key: str) -> bool:
        return await super().record_exists(key=self._resolve_input_key(key))

    @override
    async def delete_value(self, *, key: str) -> None:
        await super().delete_value(key=self._resolve_input_key(key))

    @override
    async def get_public_url(self, *, key: str) -> str:
        return await super().get_public_url(key=self._resolve_input_key(key))

    def _resolve_input_key(self, key: str) -> str:
        return self._input_key_filename if key == self._input_key else key

    @staticmethod
    async def _create_missing_metadata_for_input_file(key: str, record_path: Path) -> None:
        try:
            content = await asyncio.to_thread(record_path.read_bytes)
        except FileNotFoundError:
            logger.warning('Input file disappeared from path: "%s"', record_path)
            return

        size = len(content)
        if record_path.suffix == '.json':
            value: Any = json.loads(content.decode('utf-8'))
        elif record_path.suffix == '.txt':
            value = content.decode('utf-8')
        elif record_path.suffix == '':
            try:
                value = json.loads(content.decode('utf-8'))
            except json.JSONDecodeError:
                value = content
        else:
            value = content

        content_type = infer_mime_type(value)
        record_metadata = KeyValueStoreRecordMetadata(key=key, content_type=content_type, size=size)
        record_metadata_filepath = record_path.with_name(f'{record_path.name}.{METADATA_FILENAME}')
        record_metadata_content = await json_dumps(record_metadata.model_dump())
        await atomic_write(record_metadata_filepath, record_metadata_content)

    @staticmethod
    def _get_input_key_file_name(path_to_kvs: Path, input_key: str) -> str:
        found_input_files = set()
        for file_path in chain(path_to_kvs.glob(f'{input_key}.*'), path_to_kvs.glob(f'{input_key}')):
            if str(file_path).endswith(METADATA_FILENAME):
                continue
            found_input_files.add(file_path.name)

        if len(found_input_files) > 1:
            raise RuntimeError(f'Only one input file is allowed. Following input files found: {found_input_files}')

        if len(found_input_files) == 1:
            return found_input_files.pop()

        return input_key
