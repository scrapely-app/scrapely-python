# SPDX-License-Identifier: Apache-2.0
# Derived from apify-sdk-python (src/apify/events/_apify_event_manager.py),
# Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
# Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
"""Event manager for Scrapely Actors.

Extends Crawlee's EventManager with WebSocket connectivity to the Scrapely platform
for receiving platform events (persistState, migrating, aborting, systemInfo, etc.).

Uses Crawlee's Event enum which includes all 12 event types:
ABORTING, BROWSER_CLOSED, BROWSER_LAUNCHED, BROWSER_RETIRED, CRAWLER_STATUS,
EXIT, MIGRATING, PAGE_CLOSED, PAGE_CREATED, PERSIST_STATE, SESSION_RETIRED, SYSTEM_INFO
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, TypeAlias
from urllib.parse import urlencode

from pydantic import Discriminator, TypeAdapter
from typing_extensions import override

from crawlee.events import EventManager as CrawleeEventManager
from crawlee.events._types import Event, EventPersistStateData

from ._types import (
    DeprecatedEvent,
    EventMessage,
    SystemInfoEventData,
    UnknownEvent,
)

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Unpack

    from crawlee.events._event_manager import EventManagerOptions

    from scrapely.configuration import Configuration

logger = logging.getLogger(__name__)

# Re-export Event for backward compatibility
__all__ = ['Event', 'EventManager', 'ScrapelyEventManager']

# Type adapter for parsing platform WebSocket messages
event_data_adapter = TypeAdapter[EventMessage | DeprecatedEvent | UnknownEvent](
    EventMessage | DeprecatedEvent | UnknownEvent,
)


class ScrapelyEventManager(CrawleeEventManager):
    """Event manager for the Scrapely platform.

    This class extends Crawlee's ``EventManager`` to provide Scrapely-specific functionality, including
    WebSocket connectivity to the Scrapely platform for receiving platform events.

    The event manager handles:
    - Registration and emission of events and their listeners.
    - WebSocket connection to Scrapely platform events.
    - Processing and validation of platform messages.
    - Automatic event forwarding from the platform to local event listeners.

    This class should not be used directly. Use the ``Actor.on`` and ``Actor.off`` methods to interact
    with the event system.
    """

    _NON_RETRYABLE_CLOSE_CODES = frozenset(
        {
            1002,  # PROTOCOL_ERROR
            1003,  # UNSUPPORTED_DATA
            1007,  # INVALID_DATA
            1008,  # POLICY_VIOLATION
            1010,  # MANDATORY_EXTENSION
        }
    )
    """WebSocket close codes for a permanent condition, on which the connection is not re-established.

    The platform sends ``POLICY_VIOLATION`` (1008) for an unknown/missing run ID or an exceeded per-run
    connection limit. The rest are protocol, data, or mandatory-extension failures that reconnecting
    cannot resolve.
    """

    _HEALTHY_CONNECTION_MIN_DURATION = 1.0
    """Seconds a connection must stay open to count as healthy, after which a drop reconnects without backoff."""

    def __init__(
        self,
        configuration: Configuration,
        **kwargs: Unpack[EventManagerOptions],
    ) -> None:
        """Initialize a new instance.

        Args:
            configuration: The Actor configuration for the event manager.
            **kwargs: Additional event manager options passed to the parent class.
        """
        super().__init__(**kwargs)

        self._configuration = configuration
        """The Actor configuration for the event manager."""

        self._platform_events_websocket: Any = None
        """WebSocket connection to the platform events."""

        self._process_platform_messages_task: Optional[asyncio.Task] = None
        """Task for processing messages from the platform WebSocket."""

        self._connected_to_platform_websocket: Optional[asyncio.Future[bool]] = None
        """Future that resolves when the connection to the platform WebSocket is established."""

    @override
    async def __aenter__(self) -> ScrapelyEventManager:
        await super().__aenter__()
        self._connected_to_platform_websocket = asyncio.Future()

        # Run tasks but don't await them
        ws_url = self._configuration.get('actor_events_ws_url')
        if ws_url:
            # Authenticate with the token in a header and the runId in the URL (URLs end up in access
            # logs). This matches the Node SDK behavior in events-client.ts
            token = self._configuration.get('token')
            run_id = self._configuration.get('actor_run_id')
            headers: dict[str, str] = {}
            if token and run_id:
                separator = '&' if '?' in ws_url else '?'
                ws_url = f"{ws_url}{separator}{urlencode({'runId': run_id})}"
                headers['Authorization'] = f'Bearer {token}'
                logger.debug(f'Connecting to platform events websocket with runId={run_id}')
            else:
                logger.warning(
                    'Token or runId not available, connecting to platform events websocket without auth params. '
                    'This may result in authentication failures.'
                )

            # Connect in the background without blocking init, matching the Node SDK: a slow or failed
            # connection is retried and must not crash the Actor.
            self._process_platform_messages_task = asyncio.create_task(
                self._process_platform_messages(ws_url, headers)
            )
        else:
            logger.debug(
                'ACTOR_EVENTS_WEBSOCKET_URL env var not set, no events from Scrapely platform will be emitted.'
            )

        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        # Cancel the task before closing the websocket so that the closed connection is not treated as a drop
        # and followed by a reconnect attempt.
        if self._process_platform_messages_task and not self._process_platform_messages_task.done():
            self._process_platform_messages_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._process_platform_messages_task

        if self._platform_events_websocket:
            try:
                await self._platform_events_websocket.close()
            except Exception:
                pass

        await super().__aexit__(exc_type, exc_value, exc_traceback)

    async def _process_platform_messages(self, ws_url: str, headers: Optional[dict[str, str]] = None) -> None:
        """Process messages from the platform WebSocket with reconnect logic."""
        try:
            import websockets.asyncio.client
            import websockets.client
        except ImportError:
            logger.error('websockets package not installed, cannot connect to platform events')
            if self._connected_to_platform_websocket is not None and not self._connected_to_platform_websocket.done():
                self._connected_to_platform_websocket.set_result(False)
            return

        # The `websockets` reconnect iterator only backs off on failed connection *attempts*, not on a connection
        # that opens and is then closed. Track our own backoff here so a server that keeps accepting and immediately
        # closing is not hammered; it is reset after a healthy connection so a healthy drop reconnects immediately.
        backoff_delays = None

        try:
            # Used as an async iterator, `connect` reconnects with exponential backoff on failed connection attempts,
            # including the first one (timeouts, network errors and 5xx responses are retried).
            async for websocket in websockets.asyncio.client.connect(ws_url, additional_headers=headers or None):
                self._platform_events_websocket = websocket
                if self._connected_to_platform_websocket and not self._connected_to_platform_websocket.done():
                    self._connected_to_platform_websocket.set_result(True)
                else:
                    logger.info('Reconnected to the platform events websocket.')

                connection_opened_at = time.monotonic()
                connection_lost = await self._consume_messages(websocket)

                if not self._should_reconnect_after_close(websocket, connection_lost=connection_lost):
                    break

                # Reconnect a healthy connection immediately; back off only on repeated rapid drops.
                if time.monotonic() - connection_opened_at >= self._HEALTHY_CONNECTION_MIN_DURATION:
                    backoff_delays = None
                elif backoff_delays is None:
                    backoff_delays = websockets.client.backoff()
                else:
                    await asyncio.sleep(next(backoff_delays))
        except Exception:
            logger.exception('Error in websocket connection')
            if self._connected_to_platform_websocket is not None and not self._connected_to_platform_websocket.done():
                self._connected_to_platform_websocket.set_result(False)

    async def _consume_messages(self, websocket: Any) -> bool:
        """Handle platform messages until the connection closes; return whether it was lost vs. closed cleanly."""
        try:
            import websockets.exceptions
            async for message in websocket:
                await self._handle_platform_message(message)
        except websockets.exceptions.ConnectionClosed:
            return True
        return False

    async def _handle_platform_message(self, message: str | bytes) -> None:
        """Parse a single platform message and emit the matching local event."""
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')

            parsed_message = event_data_adapter.validate_json(message)

            if isinstance(parsed_message, DeprecatedEvent):
                return

            if isinstance(parsed_message, UnknownEvent):
                logger.debug(
                    f'Unknown message received: event_name={parsed_message.name}, event_data={parsed_message.data}'
                )
                return

            # Convert SystemInfoEventData to Crawlee format, pass other data as-is
            event_data = parsed_message.data
            if isinstance(event_data, SystemInfoEventData):
                dedicated_cpus = self._configuration.get('dedicated_cpus') or 1
                event_data = event_data.to_crawlee_format(dedicated_cpus)

            self.emit(event=parsed_message.name, event_data=event_data)

            if parsed_message.name == Event.MIGRATING:
                # Stop the persist state recurring task and emit a final persistState with isMigrating=True
                await self._emit_persist_state_event_rec_task.stop()
                self.emit(
                    event=Event.PERSIST_STATE,
                    event_data=EventPersistStateData(is_migrating=True),
                )
        except Exception:
            logger.exception('Cannot parse Actor event', extra={'raw_message': message})

    def _should_reconnect_after_close(
        self,
        websocket: Any,
        *,
        connection_lost: bool,
    ) -> bool:
        """Log the websocket close and report whether to reconnect (``False`` on a non-retryable close code)."""
        close_code = getattr(websocket, 'close_code', None)
        close_reason = getattr(websocket, 'close_reason', None)

        if close_code in self._NON_RETRYABLE_CLOSE_CODES:
            logger.error(
                f'Connection to platform events websocket was closed with a non-retryable code '
                f'(code={close_code}, reason={close_reason!r}); not reconnecting. '
                f'No further platform events will be received for the rest of this run, so migration and '
                f'abort handling (e.g. persisting state before a migration) is now disabled.'
            )
            return False

        if connection_lost:
            logger.warning(
                f'Connection to platform events websocket was lost '
                f'(code={close_code}, reason={close_reason!r}), reconnecting...'
            )
        else:
            logger.info(
                f'Connection to platform events websocket was closed '
                f'(code={close_code}, reason={close_reason!r}), reconnecting...'
            )
        return True


# Backward compatibility: keep the old EventManager name as an alias
# for code that imported it directly. New code should use ScrapelyEventManager.
EventManager: TypeAlias = ScrapelyEventManager