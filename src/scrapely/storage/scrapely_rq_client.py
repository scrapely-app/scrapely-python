"""Scrapely cloud RequestQueueClient for Crawlee."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, Optional, Sequence, Union

from typing_extensions import Self, override

from crawlee import Request
from crawlee.storage_clients.models import ProcessedRequest as CrawleeProcessedRequest
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata, UnprocessedRequest

from .api_client import ApiClientError
from .client import DEFAULT_LOCK_SECS, RQ_BATCH_SIZE, ApiStorageClient
from ._metadata import resolve_storage_id, to_rq_metadata
from .utils import unique_key_to_request_id

logger = logging.getLogger(__name__)


def _is_lost_lock_error(exc: ApiClientError) -> bool:
    """True when the API rejected a lock operation only because this client no
    longer holds the lock: 403 "Request is not locked by this client" or 409
    "Request with ID ... is not locked"."""
    return exc.status_code in (403, 409) and 'not locked' in str(exc).lower()


def _old_rq_to_crawlee(meta: Any) -> RequestQueueMetadata:
    return to_rq_metadata(
        {
            'id': meta.id,
            'name': meta.name,
            'createdAt': meta.created_at.isoformat() if getattr(meta, 'created_at', None) else None,
            'accessedAt': meta.accessed_at.isoformat() if getattr(meta, 'accessed_at', None) else None,
            'modifiedAt': meta.modified_at.isoformat() if getattr(meta, 'modified_at', None) else None,
            'hadMultipleClients': getattr(meta, 'had_multiple_clients', False),
            'handledRequestCount': getattr(meta, 'handled_request_count', 0),
            'pendingRequestCount': getattr(meta, 'pending_request_count', 0),
            'totalRequestCount': getattr(meta, 'total_request_count', 0),
        },
        meta.id,
    )


def _request_to_api_dict(request: Request) -> Dict[str, Any]:
    data = request.model_dump(by_alias=True, exclude_none=True)
    # Scrapely expects plain dicts for headers/userData
    if 'headers' in data and hasattr(data['headers'], 'items'):
        data['headers'] = dict(data['headers'])
    return data


def _request_from_api(data: Dict[str, Any]) -> Request:
    # Crawlee Request accepts uniqueKey/url via model validation with aliases
    payload = {
        'uniqueKey': data.get('uniqueKey') or data.get('url') or '',
        'url': data.get('url') or '',
        'method': data.get('method') or 'GET',
        'headers': data.get('headers') or {},
        'userData': data.get('userData') or {},
        'retryCount': data.get('retryCount') or 0,
        'noRetry': data.get('noRetry') or False,
        'loadedUrl': data.get('loadedUrl'),
        'handledAt': data.get('handledAt'),
        'payload': data.get('payload'),
    }
    return Request.model_validate(payload)


class ScrapelyRequestQueueClient(RequestQueueClient):
    """Request queue client backed by Scrapely `/v2/request-queues`."""

    def __init__(
        self,
        *,
        api: ApiStorageClient,
        metadata: RequestQueueMetadata,
        client_key: Optional[str] = None,
    ) -> None:
        self._api = api
        self._metadata = metadata
        self._client_key = client_key or api.new_client_key()
        # Track requests currently being processed (fetched via fetch_next_request)
        # Maps unique_key -> request_id for in-progress requests
        self._in_progress: Dict[str, str] = {}

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
        meta = await api.get_or_create_request_queue(id=open_id, name=open_name)
        return cls(api=api, metadata=_old_rq_to_crawlee(meta))

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        meta = await self._api.rq_get(self._metadata.id)
        self._metadata = _old_rq_to_crawlee(meta)
        return self._metadata

    @override
    async def drop(self) -> None:
        await self._api.rq_delete(self._metadata.id)
        self._in_progress.clear()

    @override
    async def purge(self) -> None:
        cursor: Optional[str] = None
        while True:
            data = await self._api.rq_list_requests(
                self._metadata.id,
                client_key=self._client_key,
                limit=RQ_BATCH_SIZE,
                cursor=cursor,
            )
            items = data.get('items') or []
            if not items:
                break
            payload = []
            for item in items:
                if item.get('id'):
                    payload.append({'id': item['id']})
                elif item.get('uniqueKey'):
                    payload.append({'uniqueKey': item['uniqueKey']})
            if payload:
                await self._api.rq_batch_delete_requests(self._metadata.id, payload)
            cursor = data.get('nextCursor')
            if not cursor or len(items) < RQ_BATCH_SIZE:
                break
        self._in_progress.clear()

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        if not requests:
            return AddRequestsResponse(processed_requests=[], unprocessed_requests=[])

        # Chunk via ApiStorageClient (max 25)
        from .types import Request as ApiRequest

        api_requests = []
        for req in requests:
            d = _request_to_api_dict(req)
            api_requests.append(ApiRequest.from_dict(d))

        result = await self._api.rq_batch_add_requests(
            self._metadata.id,
            api_requests,
            client_key=self._client_key,
            forefront=forefront,
        )

        processed = []
        for item in result.get('processedRequests') or []:
            unique_key = str(item.get('uniqueKey') or '')
            # Use deterministic ID computation - same unique_key always produces the same ID
            # The server should return the same ID if it uses the same algorithm
            request_id = str(item.get('requestId') or '') or unique_key_to_request_id(unique_key)
            processed.append(
                ProcessedRequest(
                    id=request_id or None,
                    unique_key=unique_key,
                    was_already_present=bool(item.get('wasAlreadyPresent')),
                    was_already_handled=bool(item.get('wasAlreadyHandled')),
                )
            )

        unprocessed = []
        for item in result.get('unprocessedRequests') or []:
            unprocessed.append(
                UnprocessedRequest(
                    unique_key=str(item.get('uniqueKey') or item.get('url') or ''),
                    url=str(item.get('url') or 'https://invalid.invalid'),
                    method=item.get('method'),
                )
            )

        return AddRequestsResponse(
            processed_requests=processed,
            unprocessed_requests=unprocessed,
        )

    @override
    async def get_request(self, unique_key: str) -> Optional[Request]:
        # Use deterministic ID computation - same unique_key always produces the same ID
        request_id = unique_key_to_request_id(unique_key)
        api_req = await self._api.rq_get_request(self._metadata.id, request_id)
        if api_req is None:
            return None
        return _request_from_api({
            'id': api_req.id,
            'uniqueKey': api_req.unique_key,
            'url': api_req.url,
            'method': api_req.method,
            'headers': api_req.headers,
            'userData': api_req.user_data,
            'retryCount': api_req.retry_count,
            'handledAt': api_req.handled_at,
        })

    @override
    async def fetch_next_request(self) -> Optional[Request]:
        head = await self._api.rq_list_and_lock_head(
            self._metadata.id,
            client_key=self._client_key,
            lock_secs=DEFAULT_LOCK_SECS,
            limit=1,
        )
        if not head.items:
            return None
        head_req = head.items[0]
        # The head lists only id/uniqueKey/url/method; load the full
        # request so userData and headers survive. Keep the head item if it vanished.
        full_req = await self._api.rq_get_request(self._metadata.id, head_req.id) if head_req.id else None
        api_req = full_req or head_req
        request = _request_from_api({
            'id': api_req.id,
            'uniqueKey': api_req.unique_key,
            'url': api_req.url,
            'method': api_req.method,
            'headers': api_req.headers,
            'userData': api_req.user_data,
            'retryCount': api_req.retry_count,
            'handledAt': api_req.handled_at,
            'lockExpiresAt': head_req.lock_expires_at,
        })
        if head_req.id:
            self._in_progress[request.unique_key] = head_req.id
        return request

    @override
    async def mark_request_as_handled(self, request: Request) -> Optional[ProcessedRequest]:
        # Use deterministic ID computation - same unique_key always produces the same ID
        # Also check in_progress cache for requests fetched via fetch_next_request
        request_id = self._in_progress.get(request.unique_key) or unique_key_to_request_id(request.unique_key)

        from .types import Request as ApiRequest

        api_req = ApiRequest.from_dict(_request_to_api_dict(request))
        api_req.id = request_id
        result = await self._api.rq_mark_request_handled(
            self._metadata.id,
            api_req,
            client_key=self._client_key,
        )
        self._in_progress.pop(request.unique_key, None)
        return ProcessedRequest(
            id=result.id or request_id,
            unique_key=request.unique_key,
            was_already_present=result.was_already_present,
            was_already_handled=result.was_already_handled,
        )

    @override
    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> Optional[ProcessedRequest]:
        # Use deterministic ID computation - same unique_key always produces the same ID
        # Also check in_progress cache for requests fetched via fetch_next_request
        request_id = self._in_progress.get(request.unique_key) or unique_key_to_request_id(request.unique_key)

        from .types import Request as ApiRequest

        api_req = ApiRequest.from_dict(_request_to_api_dict(request))
        api_req.id = request_id
        try:
            result = await self._api.rq_reclaim_request(
                self._metadata.id,
                api_req,
                client_key=self._client_key,
                forefront=forefront,
            )
            result_id = result.id
        except ApiClientError as exc:
            # Only a lost lock is safe to ignore: this client no longer holds it
            # (it expired and the request was handed out again, or it was
            # already released). The request is still in the queue, so there is
            # nothing to undo, and raising would make Crawlee abort the crawl.
            # Any other failure, including other 403s (bad token, missing
            # permissions), is still raised.
            if not _is_lost_lock_error(exc):
                raise
            logger.warning(
                'Could not release lock on request %s (%s); it was likely already '
                'released or re-locked after its lock expired. Continuing.',
                request_id,
                exc,
            )
            result_id = None
        self._in_progress.pop(request.unique_key, None)
        return ProcessedRequest(
            id=result_id or request_id,
            unique_key=request.unique_key,
            was_already_present=True,
            was_already_handled=False,
        )

    async def prolong_request_lock(
        self,
        request: Request,
        *,
        lock_secs: int = DEFAULT_LOCK_SECS,
        forefront: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Prolong the lock on a request being processed.

        Use this when processing takes longer than the initial lock duration
        to prevent the request from being returned to other consumers.

        Args:
            request: The request whose lock should be prolonged.
            lock_secs: Number of seconds to prolong the lock for.
            forefront: If True, move the request to the front of the queue
                if the lock expires.

        Returns:
            Dict with 'lockExpiresAt' timestamp, or None if the request
            was not in progress.
        """
        request_id = self._in_progress.get(request.unique_key) or unique_key_to_request_id(request.unique_key)
        if request_id not in self._in_progress.values():
            # Request not currently locked by this client
            return None
        result = await self._api.rq_prolong_request_lock(
            self._metadata.id,
            request_id,
            client_key=self._client_key,
            lock_secs=lock_secs,
            forefront=forefront,
        )
        return result

    async def unlock_requests(
        self,
        *,
        forefront: bool = False,
    ) -> Dict[str, Any]:
        """Unlock all requests locked by this client.

        Useful for cleanup when a client is done processing or when
        recovering from errors. This releases all locks held by this
        client's clientKey.

        Args:
            forefront: If True, move unlocked requests to the front of the queue.

        Returns:
            Dict with 'unlockedCount' indicating how many requests were unlocked.
        """
        result = await self._api.rq_unlock_requests(
            self._metadata.id,
            client_key=self._client_key,
            forefront=forefront,
        )
        self._in_progress.clear()
        return result

    async def add_request(
        self,
        request: Union[str, Request],
        *,
        forefront: bool = False,
    ) -> Optional[ProcessedRequest]:
        """Add a single request to the queue.

        Args:
            request: The request object or URL string to add.
            forefront: If True, add to the front of the queue.

        Returns:
            ProcessedRequest with info about the operation, or None if not added.
        """
        if isinstance(request, str):
            # crawlee's Request requires uniqueKey — from_url computes it.
            request = Request.from_url(request)

        from .types import Request as ApiRequest

        api_req = ApiRequest.from_dict(_request_to_api_dict(request))
        result = await self._api.rq_add_request(
            self._metadata.id,
            api_req,
            client_key=self._client_key,
            forefront=forefront,
        )
        return CrawleeProcessedRequest(
            id=result.id,
            unique_key=result.unique_key,
            was_already_present=result.was_already_present,
            was_already_handled=result.was_already_handled,
        )

    async def add_requests(
        self,
        requests: Sequence[Union[str, Request]],
        *,
        forefront: bool = False,
        batch_size: int = 1000,
        wait_time_between_batches: timedelta = timedelta(seconds=1),
        wait_for_all_requests_to_be_added: bool = False,
        wait_for_all_requests_to_be_added_timeout: Optional[timedelta] = None,
    ) -> None:
        """Add multiple requests to the queue in batches.

        Args:
            requests: Sequence of request objects or URL strings.
            forefront: If True, add to the front of the queue.
            batch_size: Number of requests per batch (max 25 for API).
            wait_time_between_batches: Time to wait between batches.
            wait_for_all_requests_to_be_added: If True, wait for all to be added.
            wait_for_all_requests_to_be_added_timeout: Timeout for waiting.
        """
        import asyncio

        # Convert strings to Request objects
        converted: list[Request] = []
        for req in requests:
            if isinstance(req, str):
                converted.append(Request.from_url(req))
            else:
                converted.append(req)

        # API batch size is 25, so we use that regardless of batch_size param
        api_batch_size = min(batch_size, RQ_BATCH_SIZE)

        for i in range(0, len(converted), api_batch_size):
            chunk = converted[i:i + api_batch_size]
            await self.add_batch_of_requests(chunk, forefront=forefront)

            # Wait between batches if there are more to process
            if i + api_batch_size < len(converted):
                await asyncio.sleep(wait_time_between_batches.total_seconds())

    async def get_handled_count(self) -> int:
        """Get the number of handled requests in the queue."""
        meta = await self.get_metadata()
        return meta.handled_request_count

    async def get_total_count(self) -> int:
        """Get the total number of requests in the queue (pending + handled)."""
        meta = await self.get_metadata()
        return meta.total_request_count

    async def to_tandem(
        self,
        request_manager: Optional['RequestQueueClient'] = None,
    ) -> Any:
        """Combine this loader with a request manager.

        Args:
            request_manager: Request manager to combine with. If None, uses default.

        Returns:
            RequestManagerTandem combining this loader with the manager.
        """
        from crawlee.request_loaders import RequestManagerTandem
        manager: Any = request_manager
        if manager is None:
            # Use the default request queue as the manager
            from crawlee.storages import RequestQueue
            manager = await RequestQueue.open()
        return RequestManagerTandem(self, manager)  # type: ignore[arg-type]  # duck-typed loader

    @override
    async def is_empty(self) -> bool:
        meta = await self.get_metadata()
        return meta.pending_request_count == 0 and not self._in_progress

    @override
    async def is_finished(self) -> bool:
        meta = await self.get_metadata()
        return meta.pending_request_count == 0 and not self._in_progress
