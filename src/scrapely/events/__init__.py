"""
Event management for Scrapely Actors.

Exports all event types and data models.
"""

from crawlee.events import EventManager as CrawleeEventManager
from crawlee.events import LocalEventManager
from crawlee.events._types import (
    EventAbortingData,
    EventExitData,
    EventMigratingData,
    EventPersistStateData,
    EventSystemInfoData,
)

from ._types import (
    AbortingEvent,
    ActorEventTypes,
    DeprecatedEvent,
    Event,
    EventMessage,
    EventWithoutData,
    ExitEvent,
    MigratingEvent,
    PersistStateEvent,
    SystemInfoEvent,
    SystemInfoEventData,
    UnknownEvent,
)
from .event_manager import EventManager, ScrapelyEventManager

__all__ = [
    'Event',
    'EventManager',
    'ScrapelyEventManager',
    'CrawleeEventManager',
    'LocalEventManager',
    # Event data types
    'SystemInfoEventData',
    'EventAbortingData',
    'EventExitData',
    'EventMigratingData',
    'EventPersistStateData',
    'EventSystemInfoData',
    # Event types
    'PersistStateEvent',
    'SystemInfoEvent',
    'MigratingEvent',
    'AbortingEvent',
    'ExitEvent',
    'EventWithoutData',
    'DeprecatedEvent',
    'UnknownEvent',
    'EventMessage',
    'ActorEventTypes',
]
