"""
API-backed storage client for Scrapely /v2 storage endpoints.

Ports the patterns from the TypeScript ApiStorageClient.
"""

from __future__ import annotations

import os
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, cast
from urllib.parse import quote

from .api_client import ApiClient, ApiClientError
from .types import (
    DatasetItemsListPage,
    DatasetMetadata,
    KeyValueStoreMetadata,
    ProcessedRequest,
    Request,
    RequestQueueHead,
    RequestQueueMetadata,
    dataset_metadata_from_dict,
    kvs_metadata_from_dict,
    rq_metadata_from_dict,
)

logger = logging.getLogger(__name__)

STORAGE_UUID_REGEX = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)
RQ_BATCH_SIZE = 25
# Must comfortably exceed Crawlee's default 60s request-handler timeout: if the
# lock expires first, the same request is handed to a second worker and both
# later try to reclaim it. Defaults to 3 minutes.
DEFAULT_LOCK_SECS = 180
# Only needs to cover the update and unlock calls that follow the renewal.
_RECLAIM_LOCK_SECS = 60


def is_storage_uuid(value: str) -> bool:
    return bool(STORAGE_UUID_REGEX.match(value))


def path_segment(value: str) -> str:
    """Percent-encode a value so it stays one URL path segment.

    `''`, `.` and `..` are rejected: a URL parser resolves dot segments even when
    percent-encoded, so the call would land on the parent resource.
    """
    if not isinstance(value, str):
        raise TypeError(f'Expected a string for a URL path segment, got {type(value).__name__}')
    if value in ('', '.', '..'):
        raise ValueError(f'"{value}" cannot be used as a URL path segment.')
    return quote(value, safe='')


def kvs_record_path(store_id: str, key: str) -> str:
    """Build a KVS record path with a URL-encoded key."""
    return f'/v2/key-value-stores/{store_id}/records/{path_segment(key)}'


def rq_request_path(queue_id: str, request_id: str, suffix: str = '') -> str:
    """Build the path of one queued request with the id URL-encoded.

    Request ids come from queue contents, which another client may have written:
    unencoded, an id such as `..` or `../../key-value-stores/x` would send the
    call to another resource.
    """
    return f'/v2/request-queues/{queue_id}/requests/{path_segment(request_id)}{suffix}'


def build_rq_params(
    client_key: str,
    *,
    forefront: bool = False,
    lock_secs: Optional[int] = None,
    limit: Optional[int] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {'clientKey': client_key}
    if forefront:
        params['forefront'] = 'true'
    if lock_secs is not None:
        params['lockSecs'] = lock_secs
    if limit is not None:
        params['limit'] = limit
    if run_id:
        params['runId'] = run_id
    return params


class ApiStorageClient:
    """Factory/client for Scrapely cloud storages over HTTP."""

    def __init__(
        self,
        api_url: str,
        token: str,
        run_id: str,
        *,
        api_client: Optional[ApiClient] = None,
        timeout: float = 30.0,
        transport: Any = None,
    ):
        self.api_client = api_client or ApiClient(
            api_url=api_url,
            token=token,
            run_id=run_id,
            timeout=timeout,
            transport=transport,
        )

    async def close(self) -> None:
        await self.api_client.close()

    # --- Collections / get-or-create ---

    async def get_or_create_dataset(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> DatasetMetadata:
        if id:
            if is_storage_uuid(id):
                try:
                    data = await self.api_client.get_json(f'/v2/datasets/{id}')
                    return dataset_metadata_from_dict(data, id)
                except ApiClientError as err:
                    if err.status_code == 404:
                        raise ValueError(f'Dataset with id "{id}" was not found') from err
                    raise
            # Treat non-UUID id as a name for convenience
            name = name or id

        data = await self.api_client.post_json(
            '/v2/datasets',
            params={'name': name} if name else None,
        )
        return dataset_metadata_from_dict(data)

    async def get_or_create_key_value_store(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> KeyValueStoreMetadata:
        if id:
            if is_storage_uuid(id):
                try:
                    data = await self.api_client.get_json(f'/v2/key-value-stores/{id}')
                    return kvs_metadata_from_dict(data, id)
                except ApiClientError as err:
                    if err.status_code == 404:
                        raise ValueError(f'Key-value store with id "{id}" was not found') from err
                    raise
            name = name or id

        data = await self.api_client.post_json(
            '/v2/key-value-stores',
            params={'name': name} if name else None,
        )
        return kvs_metadata_from_dict(data)

    async def get_or_create_request_queue(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> RequestQueueMetadata:
        if id:
            if is_storage_uuid(id):
                try:
                    data = await self.api_client.get_json(f'/v2/request-queues/{id}')
                    return rq_metadata_from_dict(data, id)
                except ApiClientError as err:
                    if err.status_code == 404:
                        raise ValueError(f'Request queue with id "{id}" was not found') from err
                    raise
            name = name or id

        data = await self.api_client.post_json(
            '/v2/request-queues',
            params={'name': name} if name else None,
        )
        return rq_metadata_from_dict(data)

    # --- Dataset resource ops ---

    async def dataset_get(self, dataset_id: str) -> DatasetMetadata:
        data = await self.api_client.get_json(f'/v2/datasets/{dataset_id}')
        return dataset_metadata_from_dict(data, dataset_id)

    async def dataset_delete(self, dataset_id: str) -> None:
        await self.api_client.delete(f'/v2/datasets/{dataset_id}')

    async def dataset_push_items(
        self,
        dataset_id: str,
        items: Sequence[Mapping[str, Any]],
    ) -> None:
        # One key per push, the same on every attempt: the API stores a re-sent
        # push once, so a push whose connection dropped can be retried.
        await self.api_client.post_json(
            f'/v2/datasets/{dataset_id}/items', list(items), idempotency_key=str(uuid.uuid4())
        )

    async def dataset_list_items(
        self,
        dataset_id: str,
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
        flatten: Optional[List[str]] = None,
        view: Optional[str] = None,
        format: Optional[str] = None,
    ) -> Tuple[Any, DatasetItemsListPage]:
        params: Dict[str, Any] = {'offset': offset}
        if limit is not None:
            params['limit'] = limit
        if clean:
            params['clean'] = 'true'
        if desc:
            params['desc'] = 'true'
        if fields:
            params['fields'] = ','.join(fields)
        if omit:
            params['omit'] = ','.join(omit)
        if unwind:
            params['unwind'] = ','.join(unwind)
        if skip_empty:
            params['skipEmpty'] = 'true'
        if skip_hidden:
            params['skipHidden'] = 'true'
        if flatten:
            params['flatten'] = ','.join(flatten)
        if view:
            params['view'] = view
        if format:
            params['format'] = format

        response = await self.api_client.get(f'/v2/datasets/{dataset_id}/items', params=params)
        headers = response.headers

        def _header_int(name: str, default: int = 0) -> int:
            raw = headers.get(name)
            if raw is None:
                return default
            try:
                return int(raw)
            except ValueError:
                return default

        content_type = headers.get('content-type', '')
        if format and format != 'json':
            body: Any = response.content if 'text/' not in content_type else response.text
            items: List[Any] = []
        else:
            body = response.json()
            if isinstance(body, list):
                items = body
            elif isinstance(body, dict) and 'items' in body:
                items = list(body['items'])
            else:
                items = []

        page = DatasetItemsListPage(
            items=items if isinstance(items, list) else [],
            total=_header_int('X-Apify-Pagination-Total', len(items) if isinstance(items, list) else 0),
            offset=_header_int('X-Apify-Pagination-Offset', offset),
            count=_header_int('X-Apify-Pagination-Count', len(items) if isinstance(items, list) else 0),
            limit=_header_int('X-Apify-Pagination-Limit', limit or 0),
            desc=desc,
        )
        return body, page

    async def dataset_get_statistics(self, dataset_id: str) -> Dict[str, Any]:
        return cast(Dict[str, Any], await self.api_client.get_json(f'/v2/datasets/{dataset_id}/statistics'))

    # --- KVS resource ops ---

    async def kvs_get(self, store_id: str) -> KeyValueStoreMetadata:
        data = await self.api_client.get_json(f'/v2/key-value-stores/{store_id}')
        return kvs_metadata_from_dict(data, store_id)

    async def kvs_delete(self, store_id: str) -> None:
        await self.api_client.delete(f'/v2/key-value-stores/{store_id}')

    async def kvs_get_record(self, store_id: str, key: str) -> Tuple[Any, Optional[str]]:
        response = await self.api_client.get(
            kvs_record_path(store_id, key),
            raise_for_status=False,
        )
        if response.status_code == 404:
            return None, None
        await self.api_client._raise_for_status(response)
        content_type = response.headers.get('content-type', '')
        if 'application/json' in content_type:
            return response.json(), content_type
        if content_type.startswith('text/'):
            return response.text, content_type
        return response.content, content_type

    async def kvs_set_record(
        self,
        store_id: str,
        key: str,
        value: Any,
        content_type: Optional[str] = None,
    ) -> None:
        if isinstance(value, (bytearray, memoryview)):
            value = bytes(value)
        if content_type is None:
            # Defaults: bytes are binary, str is text, anything else JSON.
            if isinstance(value, bytes):
                content_type = 'application/octet-stream'
            elif isinstance(value, str):
                content_type = 'text/plain'
            else:
                content_type = 'application/json'

        path = kvs_record_path(store_id, key)
        headers = {'Content-Type': content_type}
        # One key for every attempt of this write: a re-send after a lost answer
        # (edge 502/504, dropped connection) is applied and counted once.
        idempotency_key = uuid.uuid4().hex
        if isinstance(value, bytes):
            await self.api_client.put(
                path,
                content=value,
                headers=headers,
                idempotency_key=idempotency_key,
            )
        elif isinstance(value, str) and content_type != 'application/json':
            await self.api_client.put(
                path,
                content=value.encode('utf-8'),
                headers=headers,
                idempotency_key=idempotency_key,
            )
        else:
            await self.api_client.put(
                path,
                json_data=value,
                headers=headers,
                idempotency_key=idempotency_key,
            )

    async def kvs_delete_record(self, store_id: str, key: str) -> None:
        await self.api_client.delete(kvs_record_path(store_id, key))

    async def kvs_record_exists(self, store_id: str, key: str) -> bool:
        response = await self.api_client.head(
            kvs_record_path(store_id, key),
            raise_for_status=False,
        )
        if response.status_code == 404:
            return False
        if response.is_success:
            return True
        await self.api_client._raise_for_status(response)
        return False

    async def kvs_list_keys(
        self,
        store_id: str,
        *,
        exclusive_start_key: Optional[str] = None,
        limit: Optional[int] = None,
        prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if exclusive_start_key is not None:
            params['exclusiveStartKey'] = exclusive_start_key
        if limit is not None:
            params['limit'] = limit
        if prefix is not None:
            params['prefix'] = prefix
        return cast(Dict[str, Any], await self.api_client.get_json(f'/v2/key-value-stores/{store_id}/keys', params=params))

    def kvs_public_url(self, store_id: str, key: str) -> str:
        # api_url is the container-internal API address; links handed to users
        # must use the public base URL the platform injects.
        public_base = (
            os.environ.get('APIFY_API_PUBLIC_BASE_URL')
            or os.environ.get('ACTOR_API_PUBLIC_BASE_URL')
            or self.api_client.api_url
        ).rstrip('/')
        return f'{public_base}{kvs_record_path(store_id, key)}'

    # --- Request queue resource ops ---

    def new_client_key(self) -> str:
        return uuid.uuid4().hex[:16]

    async def rq_get(self, queue_id: str) -> RequestQueueMetadata:
        data = await self.api_client.get_json(f'/v2/request-queues/{queue_id}')
        return rq_metadata_from_dict(data, queue_id)

    async def rq_delete(self, queue_id: str) -> None:
        await self.api_client.delete(f'/v2/request-queues/{queue_id}')

    async def rq_add_request(
        self,
        queue_id: str,
        request: Request,
        *,
        client_key: str,
        forefront: bool = False,
    ) -> ProcessedRequest:
        data = await self.api_client.post_json(
            f'/v2/request-queues/{queue_id}/requests',
            request.to_dict(),
            params=build_rq_params(client_key, forefront=forefront),
        )
        return ProcessedRequest(
            id=str(data.get('requestId') or ''),
            unique_key=request.unique_key,
            was_already_present=bool(data.get('wasAlreadyPresent')),
            was_already_handled=bool(data.get('wasAlreadyHandled')),
        )

    async def rq_batch_add_requests(
        self,
        queue_id: str,
        requests: Sequence[Request],
        *,
        client_key: str,
        forefront: bool = False,
    ) -> Dict[str, List[Any]]:
        processed: List[Dict[str, Any]] = []
        unprocessed: List[Dict[str, Any]] = []

        for i in range(0, len(requests), RQ_BATCH_SIZE):
            chunk = list(requests[i:i + RQ_BATCH_SIZE])
            try:
                data = await self.api_client.post_json(
                    f'/v2/request-queues/{queue_id}/requests/batch',
                    [req.to_dict() for req in chunk],
                    params=build_rq_params(client_key, forefront=forefront),
                )
            except Exception:
                logger.exception(
                    'Failed to batch-add %d request(s) to queue %s',
                    len(chunk),
                    queue_id,
                )
                raise
            processed.extend(data.get('processedRequests') or [])
            unprocessed.extend(data.get('unprocessedRequests') or [])

        return {'processedRequests': processed, 'unprocessedRequests': unprocessed}

    async def rq_list_head(
        self,
        queue_id: str,
        *,
        client_key: str,
        limit: int = 1,
    ) -> RequestQueueHead:
        """Peek at the queue head without locking requests."""
        data = await self.api_client.get_json(
            f'/v2/request-queues/{queue_id}/head',
            params=build_rq_params(client_key, limit=limit),
        )
        items = [Request.from_dict(item) for item in (data.get('items') or [])]
        modified = data.get('queueModifiedAt')
        return RequestQueueHead(
            items=items,
            limit=int(data.get('limit') or limit),
            queue_modified_at=(
                datetime.fromisoformat(str(modified).replace('Z', '+00:00'))
                if modified else None
            ),
            had_multiple_clients=bool(data.get('hadMultipleClients')),
            lock_secs=data.get('lockSecs'),
            queue_has_locked_requests=data.get('queueHasLockedRequests'),
        )

    async def rq_batch_delete_requests(
        self,
        queue_id: str,
        requests: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        from .api_client import unwrap_data

        response = await self.api_client.delete(
            f'/v2/request-queues/{queue_id}/requests/batch',
            json_data=list(requests),
        )
        if not response.content:
            return {}
        return cast(Dict[str, Any], unwrap_data(response.json()))

    async def rq_get_request(self, queue_id: str, request_id: str) -> Optional[Request]:
        try:
            data = await self.api_client.get_json(rq_request_path(queue_id, request_id))
            return Request.from_dict(data)
        except ApiClientError as err:
            if err.status_code == 404:
                return None
            raise

    async def rq_list_and_lock_head(
        self,
        queue_id: str,
        *,
        client_key: str,
        lock_secs: int = DEFAULT_LOCK_SECS,
        limit: int = 1,
    ) -> RequestQueueHead:
        data = await self.api_client.post_json(
            f'/v2/request-queues/{queue_id}/head/lock',
            params=build_rq_params(client_key, lock_secs=lock_secs, limit=limit),
        )
        items = [Request.from_dict(item) for item in (data.get('items') or [])]
        modified = data.get('queueModifiedAt')
        return RequestQueueHead(
            items=items,
            limit=int(data.get('limit') or limit),
            queue_modified_at=(
                datetime.fromisoformat(str(modified).replace('Z', '+00:00'))
                if modified else None
            ),
            had_multiple_clients=bool(data.get('hadMultipleClients')),
            lock_secs=data.get('lockSecs'),
            queue_has_locked_requests=data.get('queueHasLockedRequests'),
        )

    async def rq_mark_request_handled(
        self,
        queue_id: str,
        request: Request,
        *,
        client_key: str,
    ) -> ProcessedRequest:
        if not request.id:
            raise ValueError('Request must have an id to be marked as handled')
        payload = request.to_dict()
        payload['handledAt'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        data = await self.api_client.put_json(
            rq_request_path(queue_id, request.id),
            payload,
            params=build_rq_params(client_key),
        )
        return ProcessedRequest(
            id=str(data.get('requestId') or request.id),
            unique_key=request.unique_key,
            was_already_present=bool(data.get('wasAlreadyPresent')),
            was_already_handled=bool(data.get('wasAlreadyHandled')),
        )

    async def rq_reclaim_request(
        self,
        queue_id: str,
        request: Request,
        *,
        client_key: str,
        forefront: bool = False,
    ) -> ProcessedRequest:
        if not request.id:
            raise ValueError('Request must have an id to be reclaimed')
        # The update endpoint does not check lock ownership, so renew the lock
        # first: if it expired and another client took the request, this raises
        # 403 and its retry state is left alone. A 409 means the lock expired
        # but nobody holds it, so the update below cannot clobber anyone.
        try:
            await self.rq_prolong_request_lock(
                queue_id,
                request.id,
                client_key=client_key,
                lock_secs=_RECLAIM_LOCK_SECS,
            )
        except ApiClientError as err:
            if err.status_code != 409 or 'is not locked' not in str(err):
                raise
        # Persist retryCount/errorMessages before unlocking, so the next fetch
        # sees them and max_request_retries is ever reached.
        await self.api_client.put_json(
            rq_request_path(queue_id, request.id),
            request.to_dict(),
            params=build_rq_params(client_key, forefront=forefront),
        )
        await self.api_client.delete(
            rq_request_path(queue_id, request.id, '/lock'),
            params=build_rq_params(client_key, forefront=forefront),
        )
        return ProcessedRequest(
            id=request.id,
            unique_key=request.unique_key,
            was_already_present=True,
            was_already_handled=False,
        )

    async def rq_list_requests(
        self,
        queue_id: str,
        *,
        client_key: str,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        exclusive_start_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {'clientKey': client_key}
        if limit is not None:
            params['limit'] = limit
        if cursor is not None:
            params['cursor'] = cursor
        if exclusive_start_id is not None:
            params['exclusiveStartId'] = exclusive_start_id
        return cast(
            Dict[str, Any],
            await self.api_client.get_json(
                f'/v2/request-queues/{queue_id}/requests',
                params=params,
            ),
        )

    async def rq_delete_request(self, queue_id: str, request_id: str) -> None:
        await self.api_client.delete(rq_request_path(queue_id, request_id))

    async def rq_prolong_request_lock(
        self,
        queue_id: str,
        request_id: str,
        *,
        client_key: str,
        lock_secs: int,
        forefront: bool = False,
    ) -> Dict[str, Any]:
        """Prolong the lock on a request (REST API: PUT /requests/{id}/lock)."""
        data = await self.api_client.put_json(
            rq_request_path(queue_id, request_id, '/lock'),
            params=build_rq_params(client_key, lock_secs=lock_secs, forefront=forefront),
        )
        return {'lockExpiresAt': data.get('lockExpiresAt')}

    async def rq_unlock_requests(
        self,
        queue_id: str,
        *,
        client_key: str,
        forefront: bool = False,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Unlock all requests locked by a clientKey (REST API: POST /requests/unlock)."""
        data = await self.api_client.post_json(
            f'/v2/request-queues/{queue_id}/requests/unlock',
            params=build_rq_params(client_key, forefront=forefront, run_id=run_id),
        )
        return {'unlockedCount': int(data.get('unlockedCount') or 0)}
