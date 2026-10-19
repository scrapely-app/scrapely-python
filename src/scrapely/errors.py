"""
Error classes for the Scrapely Python SDK.

This module provides a hierarchy of error classes for handling API errors.

Hierarchy:
    ScrapelyClientError
    ├── ScrapelyApiError
    │   ├── InvalidRequestError (400)
    │   ├── UnauthorizedError (401)
    │   ├── ForbiddenError (403)
    │   ├── NotFoundError (404)
    │   ├── ConflictError (409)
    │   ├── RateLimitError (429)
    │   └── ServerError (5xx)
    └── InvalidResponseBodyError
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Type

if TYPE_CHECKING:
    import httpx


class ScrapelyClientError(Exception):
    """Base class for all Scrapely API client errors."""

    pass


class InvalidResponseBodyError(ScrapelyClientError):
    """Error raised when a response body cannot be parsed."""

    def __init__(self, response: 'httpx.Response') -> None:
        self.response = response
        super().__init__(f'Invalid response body: {response.text[:200]}')


class ScrapelyApiError(ScrapelyClientError):
    """Error raised when the Scrapely API returns an error response."""

    def __init__(
        self,
        response: 'httpx.Response',
        attempt: int,
        *,
        method: str = 'GET',
    ) -> None:
        self.response = response
        self.attempt = attempt
        self.method = method
        self.status_code = response.status_code

        self.type: Optional[str] = None
        self.message: Optional[str] = None
        self.data: Optional[Dict[str, Any]] = None

        try:
            body = response.json()
            self.type = body.get('type')
            self.message = body.get('message')
            self.data = body.get('data')
        except Exception:
            pass

        error_parts = [f'HTTP {response.status_code}']
        if self.type:
            error_parts.append(f'({self.type})')
        if self.message:
            error_parts.append(f'- {self.message}')
        error_parts.append(f'[attempt {attempt}]')

        super().__init__(' '.join(error_parts))


class InvalidRequestError(ScrapelyApiError):
    """Raised when the Scrapely API returns an HTTP 400 Bad Request response."""
    pass


class UnauthorizedError(ScrapelyApiError):
    """Raised when the Scrapely API returns an HTTP 401 Unauthorized response."""
    pass


class ForbiddenError(ScrapelyApiError):
    """Raised when the Scrapely API returns an HTTP 403 Forbidden response."""
    pass


class NotFoundError(ScrapelyApiError):
    """Raised when the Scrapely API returns an HTTP 404 Not Found response."""
    pass


class ConflictError(ScrapelyApiError):
    """Raised when the Scrapely API returns an HTTP 409 Conflict response."""
    pass


class RateLimitError(ScrapelyApiError):
    """Raised when the Scrapely API returns an HTTP 429 Too Many Requests response."""
    pass


class ServerError(ScrapelyApiError):
    """Raised when the Scrapely API returns an HTTP 5xx response."""
    pass


_STATUS_CODE_TO_ERROR: Dict[int, Type[ScrapelyApiError]] = {
    400: InvalidRequestError,
    401: UnauthorizedError,
    403: ForbiddenError,
    404: NotFoundError,
    409: ConflictError,
    429: RateLimitError,
}


def raise_for_status(response: 'httpx.Response', attempt: int = 1, *, method: str = 'GET') -> None:
    """Raise an appropriate error for the given HTTP response."""
    if response.status_code < 400:
        return

    status_code = response.status_code
    error_class = _STATUS_CODE_TO_ERROR.get(status_code)

    if error_class is None and 500 <= status_code < 600:
        error_class = ServerError

    if error_class is None:
        error_class = ScrapelyApiError

    raise error_class(response, attempt, method=method)


__all__ = [
    'ScrapelyClientError',
    'ScrapelyApiError',
    'InvalidResponseBodyError',
    'InvalidRequestError',
    'UnauthorizedError',
    'ForbiddenError',
    'NotFoundError',
    'ConflictError',
    'RateLimitError',
    'ServerError',
    'raise_for_status',
]
