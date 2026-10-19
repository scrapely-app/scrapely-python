"""
Webhook types for the Scrapely Python SDK.

This module defines the Webhook class and related types for creating
ad-hoc webhooks on the Scrapely platform.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class WebhookEventType(str, Enum):
    """Event types that can trigger a webhook."""
    ACTOR_RUN_CREATED = 'ACTOR.RUN.CREATED'
    ACTOR_RUN_SUCCEEDED = 'ACTOR.RUN.SUCCEEDED'
    ACTOR_RUN_FAILED = 'ACTOR.RUN.FAILED'
    ACTOR_RUN_ABORTED = 'ACTOR.RUN.ABORTED'
    ACTOR_RUN_TIMED_OUT = 'ACTOR.RUN.TIMED_OUT'


@dataclass
class Webhook:
    """A webhook definition used by the Actor SDK.
    
    The same instance can be passed as an ad-hoc webhook to Actor.start() / Actor.call(),
    or to Actor.add_webhook(), which registers it as an ad-hoc webhook for the current run
    (the condition.actor_run_id is set automatically).
    """
    
    event_types: List[WebhookEventType]
    request_url: str
    payload_template: Optional[str] = None
    headers_template: Optional[str] = None
    idempotency_key: Optional[str] = None
    do_not_retry: Optional[bool] = None
    ignore_ssl_errors: Optional[bool] = None
    # Server-assigned identifier, populated by Actor.add_webhook() / the API.
    id: Optional[str] = None
    
    def __post_init__(self) -> None:
        """Validate the webhook after initialization."""
        if not self.event_types:
            raise ValueError('At least one event type must be specified')
        if not self.request_url:
            raise ValueError('Request URL must be specified')
    
    def to_dict(self, actor_run_id: Optional[str] = None) -> Dict[str, Any]:
        """Convert webhook to dictionary for API calls.
        
        Args:
            actor_run_id: Optional run ID to bind the webhook to.
            
        Returns:
            Dictionary representation for the API.
        """
        result: Dict[str, Any] = {
            'eventTypes': [e.value for e in self.event_types],
            'requestUrl': self.request_url,
        }
        
        if self.payload_template:
            result['payloadTemplate'] = self.payload_template
        if self.headers_template:
            result['headersTemplate'] = self.headers_template
        if self.idempotency_key:
            result['idempotencyKey'] = self.idempotency_key
        if self.do_not_retry is not None:
            result['doNotRetry'] = self.do_not_retry
        if self.ignore_ssl_errors is not None:
            result['ignoreSslErrors'] = self.ignore_ssl_errors
        
        # Add condition for the current run
        if actor_run_id:
            result['condition'] = {'actorRunId': actor_run_id}
        
        return result


__all__ = ['Webhook', 'WebhookEventType']
