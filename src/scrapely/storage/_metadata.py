"""HTTP helpers shared by Scrapely Crawlee storage clients."""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Optional, cast

from crawlee.storage_clients.models import (
    DatasetMetadata,
    KeyValueStoreMetadata,
    RequestQueueMetadata,
)

logger = logging.getLogger(__name__)

StorageType = Literal['datasets', 'keyValueStores', 'requestQueues']


def parse_api_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def to_dataset_metadata(data: Mapping[str, Any], fallback_id: str = '') -> DatasetMetadata:
    return DatasetMetadata(
        id=str(data.get('id') or fallback_id),
        name=data.get('name'),
        created_at=parse_api_datetime(data.get('createdAt')),
        accessed_at=parse_api_datetime(data.get('accessedAt')),
        modified_at=parse_api_datetime(data.get('modifiedAt')),
        item_count=int(data.get('itemCount') or 0),
    )


def to_kvs_metadata(data: Mapping[str, Any], fallback_id: str = '') -> KeyValueStoreMetadata:
    return KeyValueStoreMetadata(
        id=str(data.get('id') or fallback_id),
        name=data.get('name'),
        created_at=parse_api_datetime(data.get('createdAt')),
        accessed_at=parse_api_datetime(data.get('accessedAt')),
        modified_at=parse_api_datetime(data.get('modifiedAt')),
    )


def to_rq_metadata(data: Mapping[str, Any], fallback_id: str = '') -> RequestQueueMetadata:
    return RequestQueueMetadata(
        id=str(data.get('id') or fallback_id),
        name=data.get('name'),
        created_at=parse_api_datetime(data.get('createdAt')),
        accessed_at=parse_api_datetime(data.get('accessedAt')),
        modified_at=parse_api_datetime(data.get('modifiedAt')),
        had_multiple_clients=bool(data.get('hadMultipleClients') or False),
        handled_request_count=int(data.get('handledRequestCount') or 0),
        pending_request_count=int(data.get('pendingRequestCount') or 0),
        total_request_count=int(data.get('totalRequestCount') or 0),
    )


def parse_actor_storages_json(raw: Optional[str]) -> Optional[dict[str, Any]]:
    """Parse ``ACTOR_STORAGES_JSON`` (plain JSON or base64-encoded JSON)."""
    if not raw:
        return None
    try:
        return cast(dict[str, Any], json.loads(raw))
    except json.JSONDecodeError:
        pass
    try:
        decoded = base64.b64decode(raw).decode('utf-8')
        return cast(dict[str, Any], json.loads(decoded))
    except Exception:
        logger.debug('Failed to parse ACTOR_STORAGES_JSON', exc_info=True)
        return None


def resolve_alias_from_storages_json(
    alias: str,
    storage_type: StorageType,
    actor_storages_json: Optional[str] = None,
) -> Optional[str]:
    """Resolve an alias to a storage id via ``ACTOR_STORAGES_JSON`` (TS parity).

    Returns the resolved id, or ``None`` if not found (caller should treat alias as name).
    """
    parsed = parse_actor_storages_json(actor_storages_json)
    if not parsed:
        return None
    bucket = parsed.get(storage_type)
    if isinstance(bucket, dict):
        value = bucket.get(alias)
        if isinstance(value, str) and value:
            return value
    return None


def resolve_storage_id(
    *,
    id: Optional[str],
    name: Optional[str],
    alias: Optional[str],
    default_id: Optional[str],
    storage_type: Optional[StorageType] = None,
    actor_storages_json: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Return (id, name) for get-or-create.

    Alias is resolved via ``ACTOR_STORAGES_JSON`` when possible; otherwise treated as a name
    (same as TypeScript Actor outside the platform).
    """
    provided = [v for v in (id, name, alias) if v is not None]
    if len(provided) > 1:
        raise ValueError('Only one of id, name, or alias may be provided')
    if alias is not None:
        if storage_type is not None:
            resolved = resolve_alias_from_storages_json(
                alias,
                storage_type,
                actor_storages_json,
            )
            if resolved is not None:
                return resolved, None
        return None, alias
    if id is not None or name is not None:
        return id, name
    return default_id, None
