"""Storage data types for the Scrapely Python SDK.

This module contains all the data types used by the storage clients,
including metadata classes, request types, and response types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Union


class RequestState(str, Enum):
    """State of a request in the request queue."""

    UNPROCESSED = 'UNPROCESSED'
    IN_PROGRESS = 'IN_PROGRESS'
    HANDLED = 'HANDLED'


class EnqueueStrategy(str, Enum):
    """Strategy for enqueuing requests."""

    ALL = 'All'
    SAME_DOMAIN = 'SameDomain'
    SAME_HOSTNAME = 'SameHostname'
    SAME_ORIGIN = 'SameOrigin'


@dataclass
class CrawleeRequestData:
    """Crawlee-specific configuration stored in the user_data.

    This dataclass holds Crawlee-specific request configuration that is
    stored in the user_data field of Request objects.
    """

    crawl_depth: int = 0
    enqueue_strategy: Optional[EnqueueStrategy] = None
    forefront: bool = False
    label: Optional[str] = None
    last_proxy_tier: Optional[int] = None
    max_retries: Optional[int] = None
    session_rotation_count: int = 0
    state: RequestState = RequestState.UNPROCESSED

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data: Dict[str, Any] = {}
        if self.crawl_depth != 0:
            data['crawlDepth'] = self.crawl_depth
        if self.enqueue_strategy is not None:
            data['enqueueStrategy'] = self.enqueue_strategy.value
        if self.forefront:
            data['forefront'] = self.forefront
        if self.label is not None:
            data['label'] = self.label
        if self.last_proxy_tier is not None:
            data['lastProxyTier'] = self.last_proxy_tier
        if self.max_retries is not None:
            data['maxRetries'] = self.max_retries
        if self.session_rotation_count != 0:
            data['sessionRotationCount'] = self.session_rotation_count
        if self.state != RequestState.UNPROCESSED:
            data['state'] = self.state.value
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> 'CrawleeRequestData':
        """Create from dictionary."""
        enqueue_strategy_raw = data.get('enqueueStrategy')
        enqueue_strategy = None
        if enqueue_strategy_raw:
            try:
                enqueue_strategy = EnqueueStrategy(enqueue_strategy_raw)
            except ValueError:
                pass

        state_raw = data.get('state')
        state = RequestState.UNPROCESSED
        if state_raw:
            try:
                state = RequestState(state_raw)
            except ValueError:
                pass

        return cls(
            crawl_depth=int(data.get('crawlDepth') or 0),
            enqueue_strategy=enqueue_strategy,
            forefront=bool(data.get('forefront') or False),
            label=data.get('label'),
            last_proxy_tier=data.get('lastProxyTier'),
            max_retries=data.get('maxRetries'),
            session_rotation_count=int(data.get('sessionRotationCount') or 0),
            state=state,
        )


@dataclass
class StorageMetadata:
    """Common metadata shared by all storage types."""

    id: str
    name: Optional[str] = None
    user_id: Optional[str] = None
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    accessed_at: Optional[datetime] = None


@dataclass
class DatasetMetadata(StorageMetadata):
    """Metadata for a Dataset."""

    item_count: int = 0
    clean_item_count: int = 0
    act_id: Optional[str] = None
    act_run_id: Optional[str] = None


@dataclass
class KeyValueStoreMetadata(StorageMetadata):
    """Metadata for a KeyValueStore."""

    act_id: Optional[str] = None
    act_run_id: Optional[str] = None


@dataclass
class RequestQueueMetadata(StorageMetadata):
    """Metadata for a RequestQueue."""

    total_request_count: int = 0
    handled_request_count: int = 0
    pending_request_count: int = 0
    had_multiple_clients: bool = False
    act_id: Optional[str] = None
    act_run_id: Optional[str] = None


@dataclass
class DatasetItemsListPage:
    """Paginated list of dataset items."""

    items: List[Mapping[str, Any]] = field(default_factory=list)
    total: int = 0
    offset: int = 0
    count: int = 0
    limit: int = 0
    desc: bool = False


@dataclass
class KeyValueStoreRecordMetadata:
    """Metadata for a single key-value store record."""

    key: str
    content_type: Optional[str] = None
    size: int = 0


@dataclass
class KeyValueStoreRecord(KeyValueStoreRecordMetadata):
    """A key-value store record with its value."""

    value: Any = None


@dataclass
class ProcessedRequest:
    """Result of adding or updating a request in a request queue."""

    id: Optional[str] = None
    unique_key: str = ''
    was_already_present: bool = False
    was_already_handled: bool = False


@dataclass
class UnprocessedRequest:
    """A request that could not be processed."""

    unique_key: str = ''
    url: Optional[str] = None
    method: Optional[str] = None


@dataclass
class AddRequestsResponse:
    """Response to adding requests to a queue."""

    processed_requests: List[ProcessedRequest] = field(default_factory=list)
    unprocessed_requests: List[UnprocessedRequest] = field(default_factory=list)


@dataclass
class Request:
    """A request in the request queue."""

    unique_key: str
    url: str
    id: Optional[str] = None
    method: str = 'GET'
    headers: Dict[str, str] = field(default_factory=dict)
    user_data: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    handled_at: Optional[str] = None
    lock_expires_at: Optional[str] = None
    loaded_url: Optional[str] = None
    payload: Optional[Any] = None
    no_retry: bool = False
    error_messages: List[str] = field(default_factory=list)

    # Crawlee-specific properties stored in user_data
    @property
    def crawlee_data(self) -> CrawleeRequestData:
        """Get Crawlee-specific configuration from user_data."""
        crawlee_data = self.user_data.get('__crawlee', {})
        if isinstance(crawlee_data, CrawleeRequestData):
            return crawlee_data
        return CrawleeRequestData.from_dict(crawlee_data)

    @crawlee_data.setter
    def crawlee_data(self, value: CrawleeRequestData) -> None:
        """Set Crawlee-specific configuration in user_data."""
        self.user_data['__crawlee'] = value.to_dict()

    @property
    def crawl_depth(self) -> int:
        """The depth of the request in the crawl tree."""
        return self.crawlee_data.crawl_depth

    @crawl_depth.setter
    def crawl_depth(self, value: int) -> None:
        data = self.crawlee_data
        data.crawl_depth = value
        self.crawlee_data = data

    @property
    def enqueue_strategy(self) -> Optional[EnqueueStrategy]:
        """The strategy that was used for enqueuing the request."""
        return self.crawlee_data.enqueue_strategy

    @enqueue_strategy.setter
    def enqueue_strategy(self, value: Optional[EnqueueStrategy]) -> None:
        data = self.crawlee_data
        data.enqueue_strategy = value
        self.crawlee_data = data

    @property
    def forefront(self) -> bool:
        """Indicate whether the request should be enqueued at the front of the queue."""
        return self.crawlee_data.forefront

    @forefront.setter
    def forefront(self, value: bool) -> None:
        data = self.crawlee_data
        data.forefront = value
        self.crawlee_data = data

    @property
    def label(self) -> Optional[str]:
        """A string used to differentiate between arbitrary request types."""
        return self.crawlee_data.label

    @label.setter
    def label(self, value: Optional[str]) -> None:
        data = self.crawlee_data
        data.label = value
        self.crawlee_data = data

    @property
    def last_proxy_tier(self) -> Optional[int]:
        """The last proxy tier used to process the request."""
        return self.crawlee_data.last_proxy_tier

    @last_proxy_tier.setter
    def last_proxy_tier(self, value: Optional[int]) -> None:
        data = self.crawlee_data
        data.last_proxy_tier = value
        self.crawlee_data = data

    @property
    def max_retries(self) -> Optional[int]:
        """Crawlee-specific limit on the number of retries of the request."""
        return self.crawlee_data.max_retries

    @max_retries.setter
    def max_retries(self, value: Optional[int]) -> None:
        data = self.crawlee_data
        data.max_retries = value
        self.crawlee_data = data

    @property
    def session_rotation_count(self) -> int:
        """Crawlee-specific number of finished session rotations for the request."""
        return self.crawlee_data.session_rotation_count

    @session_rotation_count.setter
    def session_rotation_count(self, value: int) -> None:
        data = self.crawlee_data
        data.session_rotation_count = value
        self.crawlee_data = data

    @property
    def state(self) -> RequestState:
        """Crawlee-specific request handling state."""
        return self.crawlee_data.state

    @state.setter
    def state(self, value: RequestState) -> None:
        data = self.crawlee_data
        data.state = value
        self.crawlee_data = data

    @property
    def was_already_handled(self) -> bool:
        """Indicates whether the request was handled."""
        return self.state == RequestState.HANDLED

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API."""
        data: Dict[str, Any] = {
            'uniqueKey': self.unique_key,
            'url': self.url,
            'method': self.method,
        }
        if self.id:
            data['id'] = self.id
        if self.headers:
            data['headers'] = self.headers
        if self.user_data:
            data['userData'] = self.user_data
        if self.retry_count:
            data['retryCount'] = self.retry_count
        if self.handled_at is not None:
            data['handledAt'] = self.handled_at
        if self.loaded_url is not None:
            data['loadedUrl'] = self.loaded_url
        if self.payload is not None:
            data['payload'] = self.payload
        if self.no_retry:
            data['noRetry'] = self.no_retry
        if self.error_messages:
            data['errorMessages'] = self.error_messages
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> 'Request':
        """Create from API response."""
        return cls(
            id=data.get('id'),
            unique_key=data.get('uniqueKey', '') or '',
            url=data.get('url', '') or '',
            method=data.get('method', 'GET') or 'GET',
            headers=dict(data.get('headers') or {}),
            user_data=dict(data.get('userData') or {}),
            retry_count=int(data.get('retryCount') or 0),
            handled_at=data.get('handledAt'),
            lock_expires_at=data.get('lockExpiresAt'),
            loaded_url=data.get('loadedUrl'),
            payload=data.get('payload'),
            no_retry=bool(data.get('noRetry') or False),
            error_messages=list(data.get('errorMessages') or []),
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        method: str = 'GET',
        headers: Optional[Dict[str, str]] = None,
        payload: Optional[Any] = None,
        label: Optional[str] = None,
        session_id: Optional[str] = None,
        unique_key: Optional[str] = None,
        keep_url_fragment: bool = False,
        use_extended_unique_key: bool = False,
        always_enqueue: bool = False,
        enqueue_strategy: Optional[EnqueueStrategy] = None,
        max_retries: Optional[int] = None,
        **kwargs: Any,
    ) -> 'Request':
        """Create a new Request instance from a URL.

        This is the recommended constructor for creating new Request instances.
        It generates a Request object from a given URL with additional options
        to customize HTTP method, payload, unique key, and other request properties.
        If no unique_key or id is provided, they are computed automatically based
        on the URL, method and payload.

        Args:
            url: The URL of the request.
            method: The HTTP method of the request.
            headers: The HTTP headers of the request.
            payload: The data to be sent as the request body.
            label: A custom label to differentiate between request types.
            session_id: ID of a specific Session to which the request will be strictly bound.
            unique_key: A unique key identifying the request.
            keep_url_fragment: Determines whether the URL fragment should be included
                in the unique_key computation.
            use_extended_unique_key: Determines whether to include the HTTP method,
                ID Session and payload in the unique_key computation.
            always_enqueue: If True, the request will be enqueued even if already present.
            enqueue_strategy: The strategy that will be used for enqueuing the request.
            max_retries: Maximum number of retries for this request.
            kwargs: Additional keyword arguments.
        """
        # Build user_data with Crawlee-specific data
        user_data: Dict[str, Any] = kwargs.pop('user_data', None) or {}

        crawlee_data: Dict[str, Any] = {}
        if label is not None:
            crawlee_data['label'] = label
        if enqueue_strategy is not None:
            crawlee_data['enqueueStrategy'] = enqueue_strategy.value
        if max_retries is not None:
            crawlee_data['maxRetries'] = max_retries

        if session_id is not None:
            user_data['sessionId'] = session_id

        if crawlee_data:
            user_data['__crawlee'] = crawlee_data

        # Compute unique_key if not provided
        computed_unique_key = unique_key
        if computed_unique_key is None:
            # Normalize URL for unique_key
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(url)
            scheme = parsed.scheme.lower()
            netloc = parsed.netloc.lower()

            # Handle URL fragment
            fragment = parsed.fragment if keep_url_fragment else ''

            if use_extended_unique_key:
                # Include method and optionally payload in unique_key
                key_parts = [method.upper(), scheme, netloc, parsed.path, parsed.params, parsed.query, fragment]
                if payload is not None:
                    import json

                    try:
                        payload_str = json.dumps(payload, sort_keys=True)
                    except (TypeError, ValueError):
                        payload_str = str(payload)
                    key_parts.append(payload_str)
                computed_unique_key = ':'.join(key_parts)
            else:
                computed_unique_key = urlunparse((scheme, netloc, parsed.path, parsed.params, parsed.query, fragment))

        return cls(
            unique_key=computed_unique_key or url,
            url=url,
            method=method.upper(),
            headers=headers or {},
            user_data=user_data,
            payload=payload,
        )

    def get_query_param_from_url(self, param: str, *, default: Optional[str] = None) -> Optional[str]:
        """Get the value of a specific query parameter from the URL.

        Args:
            param: The query parameter name.
            default: Default value if parameter not found.

        Returns:
            The query parameter value or default.
        """
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(self.url)
        query_params = parse_qs(parsed.query)
        values = query_params.get(param, [])
        return values[0] if values else default


@dataclass
class RequestQueueHead:
    """Result of listing/locking the head of a request queue."""

    items: List[Request] = field(default_factory=list)
    limit: int = 0
    queue_modified_at: Optional[datetime] = None
    had_multiple_clients: bool = False
    lock_secs: Optional[int] = None
    queue_has_locked_requests: Optional[bool] = None

    @classmethod
    def from_client_locked_head(cls, client_locked_head: Mapping[str, Any]) -> 'RequestQueueHead':
        """Create a RequestQueueHead from an API client's LockedRequestQueueHead model.

        Args:
            client_locked_head: LockedRequestQueueHead instance from the API client.

        Returns:
            RequestQueueHead instance with properly converted types.
        """
        items = [Request.from_dict(item) for item in (client_locked_head.get('items') or [])]
        modified = client_locked_head.get('queueModifiedAt')

        return cls(
            items=items,
            limit=int(client_locked_head.get('limit') or 0),
            queue_modified_at=(
                datetime.fromisoformat(str(modified).replace('Z', '+00:00')) if modified else None
            ),
            had_multiple_clients=bool(client_locked_head.get('hadMultipleClients')),
            lock_secs=client_locked_head.get('lockSecs'),
            queue_has_locked_requests=client_locked_head.get('queueHasLockedRequests'),
        )


JsonSerializable = Any
StorageOpenId = Optional[str]


def parse_api_datetime(value: Any) -> Optional[datetime]:
    """Parse an API datetime value into a datetime, or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    return None


def dataset_metadata_from_dict(data: Mapping[str, Any], fallback_id: str = '') -> DatasetMetadata:
    """Build DatasetMetadata from an API response dict."""
    return DatasetMetadata(
        id=str(data.get('id') or fallback_id),
        name=data.get('name'),
        user_id=data.get('userId'),
        created_at=parse_api_datetime(data.get('createdAt')),
        modified_at=parse_api_datetime(data.get('modifiedAt')),
        accessed_at=parse_api_datetime(data.get('accessedAt')),
        item_count=int(data.get('itemCount') or 0),
        clean_item_count=int(data.get('cleanItemCount') or 0),
        act_id=data.get('actId') or data.get('actorId'),
        act_run_id=data.get('actRunId') or data.get('runId'),
    )


def kvs_metadata_from_dict(data: Mapping[str, Any], fallback_id: str = '') -> KeyValueStoreMetadata:
    """Build KeyValueStoreMetadata from an API response dict."""
    return KeyValueStoreMetadata(
        id=str(data.get('id') or fallback_id),
        name=data.get('name'),
        user_id=data.get('userId'),
        created_at=parse_api_datetime(data.get('createdAt')),
        modified_at=parse_api_datetime(data.get('modifiedAt')),
        accessed_at=parse_api_datetime(data.get('accessedAt')),
        act_id=data.get('actId') or data.get('actorId'),
        act_run_id=data.get('actRunId') or data.get('runId'),
    )


def rq_metadata_from_dict(data: Mapping[str, Any], fallback_id: str = '') -> RequestQueueMetadata:
    """Build RequestQueueMetadata from an API response dict."""
    return RequestQueueMetadata(
        id=str(data.get('id') or fallback_id),
        name=data.get('name'),
        user_id=data.get('userId'),
        created_at=parse_api_datetime(data.get('createdAt')),
        modified_at=parse_api_datetime(data.get('modifiedAt')),
        accessed_at=parse_api_datetime(data.get('accessedAt')),
        total_request_count=int(data.get('totalRequestCount') or 0),
        handled_request_count=int(data.get('handledRequestCount') or 0),
        pending_request_count=int(data.get('pendingRequestCount') or 0),
        had_multiple_clients=bool(data.get('hadMultipleClients') or False),
        act_id=data.get('actId') or data.get('actorId'),
        act_run_id=data.get('actRunId') or data.get('runId'),
    )


def normalize_request(request: Union['Request', str, Mapping[str, Any]]) -> Request:
    """Normalize a URL string, mapping, or Request into a Request."""
    if isinstance(request, Request):
        return request
    if isinstance(request, str):
        return Request.from_url(request)
    return Request.from_dict(request)


def as_sequence(
    data: Union[Mapping[str, Any], Sequence[Mapping[str, Any]]],
) -> List[Mapping[str, Any]]:
    """Normalize a single mapping or sequence of mappings into a list."""
    if isinstance(data, Mapping):
        return [data]
    return list(data)