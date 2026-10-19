"""Charging-aware API client for Scrapely.

Creates a ``scrapely_client`` ``ApifyClientAsync`` configured with charging
hooks, so pushes to the run's default dataset are intercepted and charged
pay-per-event by the client itself. The charging logic (pricing, budgets,
ledger) stays in the SDK's ``ChargingManager``; the client only routes
``push_items()`` through the hooks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Sequence

from scrapely_client import ApifyClientAsync

# Re-exported so the SDK and the client share one marker attribute and one
# context var. Identity matters: the client's intercepted ``push_items()``
# reads/writes these, and the SDK detects interception via the attribute.
from scrapely_client import USES_PUSH_DATA_INTERCEPTION, push_data_charging_context

if TYPE_CHECKING:
    from .charging import ChargingManager

__all__ = [
    'USES_PUSH_DATA_INTERCEPTION',
    'push_data_charging_context',
    'create_charging_client',
]


class _ChargingManagerHooks:
    """Adapts the SDK's ``ChargingManager`` to ``scrapely_client``'s ``ChargingHooks`` protocol."""

    def __init__(self, charging_manager: 'ChargingManager') -> None:
        self._charging_manager = charging_manager

    def should_charge_dataset_pushes(self, dataset_id: str) -> bool:
        """Whether default-dataset-item event pricing is configured.

        Called by the client at ``dataset()`` time, so pricing is consulted
        lazily, at the same moment the former subclass override did.
        """
        from .charging import has_default_dataset_item_event_in_pricing

        pricing_info = self._charging_manager.get_pricing_info()
        # Past the spending limit, route default-dataset pushes through
        # push_data_and_charge too, which refuses rows nobody pays for.
        return has_default_dataset_item_event_in_pricing(
            pricing_info.per_event_prices or {}
        ) or bool(getattr(self._charging_manager, 'is_charge_limit_reached', lambda: False)())

    async def push_and_charge(
        self,
        *,
        items: Sequence[Dict[str, Any]],
        event_name: Optional[str],
        push_fn: Callable[[Sequence[Dict[str, Any]]], Any],
    ) -> Any:
        from .charging import push_data_and_charge

        # The client only intercepts the default dataset.
        return await push_data_and_charge(
            self._charging_manager,
            list(items),
            event_name,
            True,
            push_fn,
        )

    def merge_charge_results(self, a: Any, b: Any) -> Any:
        from .charging import merge_charge_results

        return merge_charge_results(a, b)


def create_charging_client(
    *,
    token: Optional[str] = None,
    api_url: Optional[str] = None,
    api_public_url: Optional[str] = None,
    charging_manager: Optional['ChargingManager'] = None,
    default_dataset_id: Optional[str] = None,
    **kwargs: Any,
) -> ApifyClientAsync:
    """Create an ``ApifyClientAsync`` configured with the SDK's charging hooks.

    Args:
        token: API token
        api_url: Base URL for the API
        api_public_url: Publicly accessible base URL for signed/public resource URLs.
            When omitted, the client's own default is used.
        charging_manager: Optional charging manager for pay-per-event
        default_dataset_id: The default dataset ID for this actor run
        **kwargs: Additional arguments passed to ApifyClientAsync

    Returns:
        A charging-configured ``ApifyClientAsync``
    """
    if api_url:
        kwargs['api_url'] = api_url
    if api_public_url:
        kwargs['api_public_url'] = api_public_url

    charging_hooks = _ChargingManagerHooks(charging_manager) if charging_manager is not None else None

    return ApifyClientAsync(
        token=token,
        charging_hooks=charging_hooks,
        default_dataset_id=default_dataset_id,
        **kwargs,
    )
