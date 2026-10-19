# SPDX-License-Identifier: Apache-2.0
# Derived from apify-sdk-python (src/apify/events/_types.py),
# Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
# Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
"""Scrapely event types - extends Crawlee's event types with Scrapely-specific data models."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from crawlee.events._types import (
    Event,
    EventAbortingData,
    EventExitData,
    EventMigratingData,
    EventPersistStateData,
    EventSystemInfoData,
)

# Re-export Event for convenience
__all__ = [
    'Event',
    'SystemInfoEventData',
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

ActorEventTypes = Literal['systemInfo', 'persistState', 'migrating', 'aborting']
"""Event types emitted by the Scrapely platform during an Actor run.

This is the Scrapely-specific subset of `Event`. For the full set (including framework-level events
like `SESSION_RETIRED` or `BROWSER_LAUNCHED`), use `Event` from `scrapely`.
"""


class SystemInfoEventData(BaseModel):
    """Resource usage metrics carried by a `systemInfo` event."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    mem_avg_bytes: float
    """Average memory usage over the measured interval, in bytes."""

    mem_current_bytes: float
    """Current memory usage, in bytes."""

    mem_max_bytes: float
    """Peak memory usage observed so far, in bytes."""

    cpu_avg_usage: float
    """Average CPU usage over the measured interval, in percent."""

    cpu_max_usage: float
    """Peak CPU usage observed so far, in percent."""

    cpu_current_usage: float
    """Current CPU usage, in percent."""

    is_cpu_overloaded: bool
    """Whether the CPU is currently overloaded."""

    created_at: datetime
    """Timestamp when the metrics were collected."""

    def to_crawlee_format(self, dedicated_cpus: float) -> EventSystemInfoData:
        """Convert to Crawlee's EventSystemInfoData format."""
        return EventSystemInfoData.model_validate(
            {
                'cpu_info': {
                    'used_ratio': (self.cpu_current_usage / 100) / dedicated_cpus,
                    'created_at': self.created_at,
                },
                'memory_info': {
                    'total_size': self.mem_max_bytes,
                    'current_size': self.mem_current_bytes,
                    'created_at': self.created_at,
                },
            }
        )


class PersistStateEvent(BaseModel):
    """A `persistState` event instructing the Actor to persist its state."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    name: Literal[Event.PERSIST_STATE]
    """The event name."""

    data: Annotated[EventPersistStateData, Field(default_factory=lambda: EventPersistStateData(is_migrating=False))]
    """The event payload."""


class SystemInfoEvent(BaseModel):
    """A `systemInfo` event carrying the Actor's resource usage metrics."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    name: Literal[Event.SYSTEM_INFO]
    """The event name."""

    data: SystemInfoEventData
    """The event payload."""


class MigratingEvent(BaseModel):
    """A `migrating` event signalling the Actor is about to be migrated to another host."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    name: Literal[Event.MIGRATING]
    """The event name."""

    data: Annotated[EventMigratingData, Field(default_factory=EventMigratingData)]
    """The event payload."""


class AbortingEvent(BaseModel):
    """An `aborting` event signalling the Actor run is being aborted."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    name: Literal[Event.ABORTING]
    """The event name."""

    data: Annotated[EventAbortingData, Field(default_factory=EventAbortingData)]
    """The event payload."""


class ExitEvent(BaseModel):
    """An `exit` event signalling the Actor process is about to exit."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    name: Literal[Event.EXIT]
    """The event name."""

    data: Annotated[EventExitData, Field(default_factory=EventExitData)]
    """The event payload."""


class EventWithoutData(BaseModel):
    """A framework-level event that carries no payload (e.g. browser and page lifecycle events)."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    name: Literal[
        Event.SESSION_RETIRED,
        Event.BROWSER_LAUNCHED,
        Event.BROWSER_RETIRED,
        Event.BROWSER_CLOSED,
        Event.PAGE_CREATED,
        Event.PAGE_CLOSED,
    ]
    """The event name."""

    data: Any = None
    """The event payload, always empty for this event."""


class DeprecatedEvent(BaseModel):
    """A deprecated event kept for backward compatibility (e.g. `cpuInfo`)."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    name: Literal['cpuInfo']
    """The event name."""

    data: Annotated[dict[str, Any], Field(default_factory=dict)]
    """The event payload."""


class UnknownEvent(BaseModel):
    """A fallback for any event whose name is not recognized by the SDK."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    name: str
    """The event name."""

    data: Annotated[dict[str, Any], Field(default_factory=dict)]
    """The event payload."""


EventMessage = PersistStateEvent | SystemInfoEvent | MigratingEvent | AbortingEvent | ExitEvent | EventWithoutData
"""Union of all recognized event message types."""