"""
Charging manager for pay-per-event pricing.

Each charge() call is one logical operation with a unique Idempotency-Key.
Failed POSTs keep a pending intent for flush/retry; later charge() calls are
never treated as retries of a previous operation.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import os
import logging
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from .pricing_types import (
    ActorPricingInfoResult,
    ChargeResult,
    PayPerEventActorPricingInfo,
    PricingModel,
    PricingPerEvent,
)

if TYPE_CHECKING:
    from .configuration import Configuration
    from .storage.api_client import ApiClient

logger = logging.getLogger(__name__)

# Synthetic event for default-dataset items in pay-per-event actors (legacy name).
APIFY_DEFAULT_DATASET_ITEM_EVENT = 'apify-default-dataset-item'

# Scrapely-native synthetic event for default-dataset items.
SCRAPELY_DEFAULT_DATASET_ITEM_EVENT = 'scrapely-default-dataset-item'

# Alias — imported actors expect this constant name.
DEFAULT_DATASET_ITEM_EVENT = APIFY_DEFAULT_DATASET_ITEM_EVENT

# Must stay below server CHARGE_IDEMPOTENCY_TTL (35d). Do not replay past this age.
CHARGE_IDEMPOTENCY_SAFE_AGE_SECS = 30 * 24 * 60 * 60

# Soft cap on open unresolved intents. Refuse NEW billable charges when full.
_PENDING_MAX_ENTRIES = 1024

# Same-key POST attempts before leaving an intent pending / re-queueing a void.
# The API client itself retries 429 and 5xx answers and refused or reset
# connections of these POSTs with exponential backoff (honouring Retry-After)
# for its retry window (5 minutes, SCRAPELY_API_RETRY_WINDOW_SECS): re-sending
# is safe because every attempt carries the same Idempotency-Key. These outer
# attempts cover what the client gives up on quickly (timeouts, other
# transport errors), within _RETRY_BUDGET_SECS. Then the charge stays pending
# (the void is re-queued) and is retried before the next push and at exit,
# the same as the JS SDK.
_POST_ATTEMPTS = 8

# Outer retries stop once this much time has passed since the first attempt
# (the API retry window; JS SDK parity).
_RETRY_BUDGET_SECS = 300.0


def _is_transient_status(status: Optional[int]) -> bool:
    """408/429 and 5xx: the platform could not take the request now; try again later."""
    return status is not None and (status in (408, 429) or status >= 500)


# Local-only dataset collecting one entry per logical charge operation
# (JS ChargingManager LOCAL_CHARGING_LOG_DATASET_NAME parity).
LOCAL_CHARGING_LOG_DATASET_NAME = 'charging_log'


# Charge-limit safety net for the default dataset.
#
# Once a pay-per-event run has used up its spending limit (maxTotalChargeUsd),
# rows written to the run's default dataset would go out unbilled. The SDK then
# refuses such writes: nothing is stored, one warning is logged and
# ``ChargingManager.is_charge_limit_reached()`` turns True. Pushes made by
# ``push_data_and_charge`` (``Actor.push_data``) are decided there and marked
# with this context variable, so the storage client does not check them twice.
_charge_gated_push: contextvars.ContextVar[bool] = contextvars.ContextVar(
    'scrapely_charge_gated_push', default=False
)

# The charging manager of the running Actor (set by ``Actor``); storage clients
# consult it before writing to the default dataset.
_active_charging_manager: Optional['ChargingManager'] = None


def set_active_charging_manager(manager: Optional['ChargingManager']) -> None:
    """Register the charging manager whose limit guards default-dataset writes."""
    global _active_charging_manager
    _active_charging_manager = manager


def default_dataset_id_from_env() -> Optional[str]:
    """The run's default dataset ID, as the platform passes it."""
    return os.environ.get('ACTOR_DEFAULT_DATASET_ID') or os.environ.get('APIFY_DEFAULT_DATASET_ID')


def refuse_default_dataset_push(item_count: int) -> bool:
    """True when a write of ``item_count`` rows to the default dataset must be dropped.

    Called by storage clients for writes that did not go through
    ``push_data_and_charge`` (``Actor.open_dataset().push_data()``, crawler
    ``context.push_data``, ...). Non-PPE runs and runs below their limit are
    never refused.
    """
    if _charge_gated_push.get():
        return False
    manager = _active_charging_manager
    if manager is None:
        return False
    return manager._refuse_default_dataset_push(item_count)


def is_default_dataset_item_event(event_name: str) -> bool:
    """Check if event name is a default dataset item event."""
    return event_name in (APIFY_DEFAULT_DATASET_ITEM_EVENT, SCRAPELY_DEFAULT_DATASET_ITEM_EVENT)


def resolve_default_dataset_item_event_name(pricing_info: dict) -> str:
    """Resolve the default dataset item event name from pricing info."""
    if APIFY_DEFAULT_DATASET_ITEM_EVENT in pricing_info:
        return APIFY_DEFAULT_DATASET_ITEM_EVENT
    if SCRAPELY_DEFAULT_DATASET_ITEM_EVENT in pricing_info:
        return SCRAPELY_DEFAULT_DATASET_ITEM_EVENT
    return APIFY_DEFAULT_DATASET_ITEM_EVENT


def has_default_dataset_item_event_in_pricing(per_event_prices: dict) -> bool:
    """Check if default dataset item event pricing is configured."""
    return (
        APIFY_DEFAULT_DATASET_ITEM_EVENT in per_event_prices or
        SCRAPELY_DEFAULT_DATASET_ITEM_EVENT in per_event_prices
    )


# Backwards-compatible alias for the pre-rename camelCase name.
# Deprecated — use ``has_default_dataset_item_event_in_pricing`` instead.
hasDefaultDatasetItemEventInPricing = has_default_dataset_item_event_in_pricing


def merge_charge_results(a: ChargeResult, b: ChargeResult) -> ChargeResult:
    """Merge two :class:`ChargeResult` objects (Node SDK ``mergeChargeResults`` parity).

    Sums charged counts, ORs the limit-reached flag, and keeps the most
    restrictive remaining-limit per event type.
    """
    def _min_optional(x: Optional[int], y: Optional[int]) -> Optional[int]:
        # None means "no limit known" — the least restrictive value.
        if x is None:
            return y
        if y is None:
            return x
        return min(x, y)

    merged_limits: Dict[str, Optional[int]] = {
        key: _min_optional(value, b.chargeable_within_limit.get(key, value))
        for key, value in a.chargeable_within_limit.items()
    }
    return ChargeResult(
        event_charge_limit_reached=a.event_charge_limit_reached or b.event_charge_limit_reached,
        charged_count=a.charged_count + b.charged_count,
        chargeable_within_limit=merged_limits,
    )


class ChargeUncertainError(Exception):
    """Raised when a charge POST could not be confirmed after retries.

    Local budget still reserves the count for this operation. The intent remains
    pending for later flush with the same Idempotency-Key. Do not assume the
    platform did or did not apply the charge.
    """

    def __init__(
        self,
        message: str,
        *,
        operation_id: str,
        event_name: str,
        count: int,
    ) -> None:
        super().__init__(message)
        self.operation_id = operation_id
        self.event_name = event_name
        self.count = count


class ChargeRejectedError(Exception):
    """Definitive platform rejection (402/400/409). Do not retry with the same key."""

    def __init__(
        self,
        message: str,
        *,
        operation_id: str,
        event_name: str,
        count: int,
        status_code: Optional[int] = None,
        reason: str = 'rejected',
    ) -> None:
        super().__init__(message)
        self.operation_id = operation_id
        self.event_name = event_name
        self.count = count
        self.status_code = status_code
        self.reason = reason


class ChargingManager:
    """Manages charging for pay-per-event Actors.

    Tracks event counts and enforces spending limits.
    Provides fine-grained access to pay-per-event functionality.
    """

    _PENDING_MAX_ENTRIES = _PENDING_MAX_ENTRIES
    _IDEMPOTENCY_SAFE_AGE_SECS = CHARGE_IDEMPOTENCY_SAFE_AGE_SECS
    _POST_ATTEMPTS = _POST_ATTEMPTS
    _RETRY_BUDGET_SECS = _RETRY_BUDGET_SECS

    def __init__(self, config: 'Configuration', client: 'ApiClient'):
        """Initialize the charging manager.

        Args:
            config: Configuration instance
            client: API client for making charge requests
        """
        self._config = config
        # Optional in practice: guards treat a missing client as "cannot bill".
        self._client: Optional['ApiClient'] = client
        self._pricing_info: Optional[PayPerEventActorPricingInfo] = None
        self._charged_events: Dict[str, int] = {}
        self._max_total_charge_usd: Optional[Decimal] = None
        self._total_charged_usd: Decimal = Decimal('0.0')
        # Run spend the platform counts against maxTotalChargeUsd that this SDK
        # does not charge itself: compute/proxy/browser usage and server-billed
        # events. Learnt from the charge endpoint's remainingBudgetNd (only ever
        # grows), so the local budget gates on the run's true remaining budget.
        self._platform_other_usd: Decimal = Decimal('0')
        self._last_remaining_budget_nd: Optional[int] = None
        # True once the platform reported a remaining budget (its usage is then in the local budget).
        self._platform_budget_seen = False
        self._charge_lock = RLock()
        self._flush_lock = asyncio.Lock()
        # Serializes budget calculation/reservation with PPE-aware dataset pushes.
        # The threading lock protects synchronous state mutation; this async lock
        # protects invariants across awaited push operations.
        self._operation_lock = asyncio.Lock()
        # Set during actor shutdown; rejects new charge operations after final flush.
        self._closed = False
        self._per_event_prices: Dict[str, Decimal] = {}
        self._is_at_home = bool(config.get('is_at_home'))
        # Prefer run_id from the API client (storage credentials) over env config.
        client_run_id = getattr(client, 'run_id', None) if client is not None else None
        self._actor_run_id: Optional[str] = client_run_id or config.get('actor_run_id')
        self._initialized = False
        self._not_ppe_warning_printed = False
        # Unresolved platform-billable intents: idempotency_key -> metadata.
        # Identity is the UUID key only — never keyed by event_name.
        self._pending_charge_keys: Dict[str, Dict[str, Any]] = {}
        # Record in the run's default key-value store mirroring the unconfirmed
        # charges, so a container crash does not lose them: settlement re-applies
        # them under their own idempotency keys. One record per container, so a
        # restarted container never overwrites its predecessor's.
        self._pending_record_key = f'SCRAPELY_PENDING_CHARGES-{uuid.uuid4().hex[:12]}'
        self._persisted_pending_sig = ''
        # Optional local charging-log dataset (ACTOR_USE_CHARGING_LOG_DATASET).
        self._charging_log_dataset: Any = None
        # Idempotency key of the last charge the platform accepted
        # (push_data_and_charge voids it when the push then fails).
        self._last_accepted_operation_id: Optional[str] = None
        # Voids not yet confirmed by the platform; retried before the next push and at exit.
        self._pending_voids: List[tuple[str, str, int, bool]] = []
        # Sticky: the platform refused or capped a charge (HTTP 402 / fewer
        # events accepted) or the local budget gate trimmed one.
        self._charge_limit_hit = False
        # Default-dataset rows refused after the limit (one warning, then a count).
        self._refused_default_dataset_items = 0
        # Prepaid-row balance: priced events charged through charge() minus rows
        # pushed to the default dataset without a per-row charge. An Actor that
        # charges first and then pushes plainly (charge('product'); push_data(row))
        # has a positive balance after the charge, so the row it already paid for
        # is still saved when that charge used up the spending limit. Push-then-
        # charge Actors never go positive, so they get nothing extra for free.
        self._prepaid_row_balance = 0
        # Platform charges for results that were not stored and that the platform
        # definitively refused to void (event -> count): they stay billed, so the
        # next push of that event uses them instead of charging again.
        self._void_refused_credit: Dict[str, int] = {}

    def close(self) -> None:
        """Mark the manager closed; subsequent charge/push operations are rejected."""
        self._closed = True
        if self._refused_default_dataset_items:
            logger.warning(
                '%s result(s) pushed to the default dataset after the spending limit was '
                'reached were not saved',
                self._refused_default_dataset_items,
            )

    def _remaining_budget_usd(self) -> Decimal:
        """maxTotalChargeUsd minus this SDK's charges and the platform's other run spend."""
        assert self._max_total_charge_usd is not None
        return self._max_total_charge_usd - self._total_charged_usd - self._platform_other_usd

    def _sync_platform_budget(self) -> None:
        """Fold the last charge response's remainingBudgetNd into the local budget.

        Called after the local reservation matches what the platform accepted.
        The platform's remaining budget = max - (our charges + usage + server
        events), so ``other`` = max - remaining - our charges. Monotonic: usage
        never shrinks, and a stale answer must not hand budget back.
        """
        nd = self._last_remaining_budget_nd
        self._last_remaining_budget_nd = None
        if nd is None or self._max_total_charge_usd is None or nd < 0:
            return
        self._platform_budget_seen = True
        other = self._max_total_charge_usd - Decimal(nd) / Decimal(1_000_000_000) - self._total_charged_usd
        if other > self._platform_other_usd:
            self._platform_other_usd = other

    def is_charge_limit_reached(self) -> bool:
        """Whether this pay-per-event run has used up its spending limit.

        True once a charge was refused or capped by the limit (the platform's
        HTTP 402 or a partial acceptance, or the local budget gate), or once the
        remaining budget cannot pay for even one event of the cheapest priced
        type. From then on, pushes to the default dataset that are not paid
        for are refused. Always False for actors without pay-per-event
        pricing, without a limit, or with only free events.
        """
        if self._pricing_info is None:
            return False
        if self._charge_limit_hit:
            return True
        if self._max_total_charge_usd is None:
            return False
        prices = [price for price in self._per_event_prices.values() if price > 0]
        if not prices:
            return False
        return self._remaining_budget_usd() < min(prices)

    def _mark_charge_limit_hit(self) -> None:
        if self._pricing_info is not None:
            self._charge_limit_hit = True

    def _note_refused_default_dataset_items(self, count: int) -> None:
        """Count rows dropped because of the limit; warn on the first one only."""
        if count <= 0:
            return
        if not self._refused_default_dataset_items:
            limit = (
                f'${self._max_total_charge_usd}' if self._max_total_charge_usd is not None else 'the limit'
            )
            logger.warning(
                'The spending limit of this run (maxTotalChargeUsd = %s) has been reached: '
                'results pushed to the default dataset are no longer saved. The Actor should '
                'stop now (check ChargeResult.event_charge_limit_reached or '
                'Actor.get_charging_manager().is_charge_limit_reached()).',
                limit,
            )
        self._refused_default_dataset_items += count

    def _note_prepaid_charge(self, event_name: str, charged_count: int) -> None:
        if (
            charged_count > 0
            and not is_default_dataset_item_event(event_name)
            and self._per_event_prices.get(event_name, 0) > 0
        ):
            self._prepaid_row_balance += charged_count

    def _take_unpaid_rows(self, item_count: int) -> int:
        """Account for ``item_count`` rows pushed without a per-row charge.

        Returns how many of them may be stored: all of them while the limit is
        not reached, afterwards only the rows already paid for by earlier
        charges (the positive prepaid balance).
        """
        if item_count <= 0:
            return 0
        if not self.is_charge_limit_reached():
            self._prepaid_row_balance -= item_count
            return item_count
        allowed = max(0, min(item_count, self._prepaid_row_balance))
        self._prepaid_row_balance -= allowed
        return allowed

    def _refuse_default_dataset_push(self, item_count: int) -> bool:
        if item_count <= 0 or self._pricing_info is None:
            return False
        if not self.is_charge_limit_reached():
            self._take_unpaid_rows(item_count)
            return False
        # All-or-nothing here (storage clients cannot store a slice): store the
        # batch only when earlier charges paid for every row of it.
        if self._prepaid_row_balance >= item_count:
            self._prepaid_row_balance -= item_count
            return False
        self._note_refused_default_dataset_items(item_count)
        return True

    @property
    def charge_lock(self) -> RLock:
        """Lock to synchronize charge operations."""
        return self._charge_lock

    def _load_pricing_data(self, pricing_data: Any) -> None:
        """Populate PPE price map from a pricing info object/dict."""
        if not isinstance(pricing_data, dict):
            return

        # Worker may inject either the full ActorPricingInfo or just pricingPerEvent.
        model = pricing_data.get('pricingModel')
        pricing_per_event = pricing_data.get('pricingPerEvent') or {}
        actor_charge_events = pricing_per_event.get('actorChargeEvents') or pricing_per_event

        if model and model != PricingModel.PAY_PER_EVENT.value and model != 'PAY_PER_EVENT':
            return
        if not actor_charge_events and model not in (PricingModel.PAY_PER_EVENT.value, 'PAY_PER_EVENT'):
            return

        per_event = {}
        for event_name, event_data in (actor_charge_events or {}).items():
            if not isinstance(event_data, dict):
                continue
            price = event_data.get('eventPriceUsd', event_data.get('event_price_usd', 0))
            per_event[event_name] = PricingPerEvent(
                event_price_usd=Decimal(str(price or 0)),
                event_description=event_data.get('eventDescription') or event_data.get('eventTitle'),
            )

        if not per_event and model not in (PricingModel.PAY_PER_EVENT.value, 'PAY_PER_EVENT'):
            return

        self._pricing_info = PayPerEventActorPricingInfo(pricing_per_event=per_event)
        self._per_event_prices = {
            name: info.event_price_usd
            for name, info in per_event.items()
        }

        default_event = pricing_data.get('defaultDatasetItemEventName')
        if default_event and default_event not in self._per_event_prices:
            self._per_event_prices[default_event] = Decimal('0')

    async def init(self) -> None:
        """Initialize the charging manager (JS ChargingManager.init parity).

        Loads pricing info, prior charged counts and the budget from the
        environment (when complete) or the platform run API, honors
        ACTOR_TEST_PAY_PER_EVENT, and opens the optional charging-log dataset.
        """
        if self._initialized:
            return

        use_charging_log_dataset = bool(self._config.get('use_charging_log_dataset'))
        if use_charging_log_dataset and self._is_at_home:
            raise RuntimeError(
                'Using the ACTOR_USE_CHARGING_LOG_DATASET environment variable is only supported '
                'in a local development environment'
            )

        test_pay_per_event = bool(self._config.get('test_pay_per_event'))
        if test_pay_per_event and self._is_at_home:
            raise RuntimeError(
                'Using the ACTOR_TEST_PAY_PER_EVENT environment variable is only supported '
                'in a local development environment'
            )

        pricing_data = await self._fetch_pricing_data()

        # Load max total charge (absent/zero means no cap — JS `|| Infinity` parity).
        max_charge = pricing_data.get('max_total_charge_usd')
        self._max_total_charge_usd = Decimal(str(max_charge)) if max_charge else None

        # Load pricing info (env when complete, otherwise the run API at home).
        pricing_info_raw = pricing_data.get('pricing_info')
        if pricing_info_raw:
            try:
                parsed = json.loads(pricing_info_raw) if isinstance(pricing_info_raw, str) else pricing_info_raw
                self._load_pricing_data(parsed)
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logger.warning(f'Failed to parse actor pricing info: {e}')

        # ACTOR_TEST_PAY_PER_EVENT forces pay-per-event mode locally even
        # without configured pricing (every event then costs $1).
        if test_pay_per_event and self._pricing_info is None:
            self._pricing_info = PayPerEventActorPricingInfo(pricing_per_event={})

        # Load charged event counts
        charged_counts = pricing_data.get('charged_event_counts')
        if charged_counts:
            try:
                if isinstance(charged_counts, str):
                    self._charged_events = json.loads(charged_counts)
                else:
                    self._charged_events = dict(charged_counts)
            except (json.JSONDecodeError, TypeError):
                logger.warning('Failed to parse charged event counts')

        # Recompute total charged from prior counts when prices are known.
        self._total_charged_usd = Decimal('0.0')
        for event_name, event_count in self._charged_events.items():
            price = self._per_event_prices.get(event_name, Decimal('0'))
            self._total_charged_usd += price * Decimal(str(event_count or 0))

        self._initialized = True

        # Optional charging-log dataset (pay-per-event runs only). The platform
        # branch of the JS SDK is unreachable here because the environment
        # variable is rejected at home above.
        if self._pricing_info is not None and use_charging_log_dataset:
            try:
                await self._open_charging_log_dataset()
            except Exception as e:
                logger.warning('Failed to open the charging log dataset: %s', e)

    async def _fetch_pricing_data(self) -> Dict[str, Any]:
        """Resolve pricing info, charged counts and budget (JS fetchPricingInfo parity).

        Prefers environment variables when both pricing info and charged counts are
        present; on the platform an incomplete configuration falls back to the run API.
        """
        pricing_info_raw = self._config.get('actor_pricing_info')
        charged_counts = self._config.get('charged_event_counts')
        if pricing_info_raw and charged_counts:
            return {
                'pricing_info': pricing_info_raw,
                'charged_event_counts': charged_counts,
                'max_total_charge_usd': self._config.get('max_total_charge_usd'),
            }

        if self._is_at_home:
            if not self._actor_run_id:
                raise RuntimeError(
                    'Actor run ID not found even though the Actor is running on the platform'
                )
            if self._client is not None:
                run = None
                try:
                    run = await self._client.get_json(f'/v2/actor-runs/{self._actor_run_id}')
                except Exception as e:
                    logger.warning(
                        'Failed to fetch actor run %s for pricing info: %s',
                        self._actor_run_id,
                        e,
                    )
                if run is None:
                    raise RuntimeError('Actor run not found')
                options = run.get('options') or {}
                return {
                    'pricing_info': run.get('pricingInfo'),
                    'charged_event_counts': run.get('chargedEventCounts') or {},
                    'max_total_charge_usd': options.get('maxTotalChargeUsd'),
                }
            logger.warning('No API client available; cannot fetch run pricing info')

        return {
            'pricing_info': pricing_info_raw,
            'charged_event_counts': charged_counts,
            'max_total_charge_usd': self._config.get('max_total_charge_usd'),
        }

    def _calculate_event_price(self, event_name: str) -> Optional[Decimal]:
        """Effective unit price for affordability (JS calculateEventPrice parity).

        Locally every event costs $1 so budgets constrain during development;
        on the platform unregistered events are free (None).
        """
        if not self._is_at_home:
            return Decimal('1')
        if is_default_dataset_item_event(event_name):
            if APIFY_DEFAULT_DATASET_ITEM_EVENT in self._per_event_prices:
                return self._per_event_prices[APIFY_DEFAULT_DATASET_ITEM_EVENT]
            return self._per_event_prices.get(SCRAPELY_DEFAULT_DATASET_ITEM_EVENT)
        return self._per_event_prices.get(event_name)

    def _get_event_pricing(self, event_name: str) -> tuple[Decimal, str]:
        """Unit price and title used for reservation/logging (JS getEventPricing parity)."""
        canonical_event_name = (
            resolve_default_dataset_item_event_name(self._per_event_prices)
            if is_default_dataset_item_event(event_name)
            else event_name
        )

        price = self._per_event_prices.get(canonical_event_name)
        if price is not None:
            info = (
                self._pricing_info.pricing_per_event.get(canonical_event_name)
                if self._pricing_info is not None
                else None
            )
            title = info.event_description if info is not None and info.event_description else canonical_event_name
            return price, title

        # Unknown event: free on the platform, $1 locally.
        price = Decimal('0') if self._is_at_home else Decimal('1')
        return price, f"Unknown event '{event_name}'"

    async def _open_charging_log_dataset(self) -> None:
        """Open the local charging-log dataset (JS parity: local development only)."""
        from crawlee.storages import Dataset

        if self._config.get('purge_on_start'):
            dataset = await Dataset.open(name=LOCAL_CHARGING_LOG_DATASET_NAME)
            await dataset.drop()
        self._charging_log_dataset = await Dataset.open(name=LOCAL_CHARGING_LOG_DATASET_NAME)

    def _snapshot_pending(self) -> List[tuple[str, Dict[str, Any]]]:
        with self._charge_lock:
            return [(k, dict(v)) for k, v in self._pending_charge_keys.items()]

    def _drop_pending(self, idempotency_key: str) -> None:
        with self._charge_lock:
            self._pending_charge_keys.pop(idempotency_key, None)

    def _unwind_reservation(self, event_name: str, count: int) -> None:
        """Subtract a previously reserved local count/spend (reject or partial accept)."""
        if count <= 0:
            return
        with self._charge_lock:
            event_price = self._per_event_prices.get(event_name, Decimal('0'))
            if event_name in self._charged_events:
                self._charged_events[event_name] = max(
                    0, self._charged_events[event_name] - count
                )
            self._total_charged_usd = max(
                Decimal('0'),
                self._total_charged_usd - event_price * Decimal(count),
            )

    def _take_last_accepted_operation_id(self) -> Optional[str]:
        op_id = self._last_accepted_operation_id
        self._last_accepted_operation_id = None
        return op_id

    async def _persist_pending_charges(self) -> None:
        """Mirror the unconfirmed charges to the run's default key-value store.

        Best effort, and only when the set changed since the last write.
        """
        if self._client is None or not self._is_at_home or not self._actor_run_id:
            return
        intents = [
            {
                'idempotencyKey': key,
                'eventName': str(p.get('event_name') or ''),
                'count': int(p.get('count', 0) or 0),
                'createdAt': int(p.get('created_wall_ms') or time.time() * 1000),
            }
            for key, p in self._snapshot_pending()
        ]
        sig = ','.join(sorted(i['idempotencyKey'] for i in intents))
        if sig == self._persisted_pending_sig:
            return
        store_id = (
            os.environ.get('ACTOR_DEFAULT_KEY_VALUE_STORE_ID')
            or os.environ.get('APIFY_DEFAULT_KEY_VALUE_STORE_ID')
            or self._config.get('default_key_value_store_id')
        )
        if not store_id or store_id == 'default':
            return
        try:
            await asyncio.wait_for(
                self._client.put(
                    f'/v2/key-value-stores/{store_id}/records/{self._pending_record_key}',
                    json_data={'v': 1, 'intents': intents},
                    idempotent=True,
                ),
                timeout=15,
            )
            self._persisted_pending_sig = sig
        except Exception as err:  # noqa: BLE001
            logger.warning('Could not persist %s unconfirmed charge(s): %s', len(intents), err)

    async def _cancel_uncertain_charge(self, event_name: str, operation_id: str, reserved: int) -> bool:
        """Cancel an unconfirmed charge for items that were then not stored.

        Drops the pending intent (so the exit flush never posts it) and voids it in
        case the original request did apply; a 404 means it never applied.
        """
        self._drop_pending(operation_id)
        voided = await self._void_charge(event_name, operation_id, reserved, not_found_means_voided=True)
        # A persisted copy is harmless either way (the void leaves a tombstone),
        # but keep the record small.
        await self._persist_pending_charges()
        return voided

    async def _void_outcome(self, event_name: str, operation_id: str, reserved: int, uncertain: bool) -> str:
        """Void (or cancel, when unconfirmed) a whole charge and say what happened.

        ``'voided'``: the platform confirmed (local reservation released);
        ``'stands'``: a definitive answer that it cannot be voided (charge stays);
        ``'unknown'``: not confirmed, queued for retry (reservation kept).
        """
        queued = len(self._pending_voids)
        if uncertain:
            ok = await self._cancel_uncertain_charge(event_name, operation_id, reserved)
        else:
            ok = await self._void_charge(event_name, operation_id, reserved)
        if ok:
            return 'voided'
        return 'unknown' if len(self._pending_voids) > queued else 'stands'

    def _note_unvoidable(self, event_name: str, count: int) -> None:
        """A charge for ``count`` unstored results stands (void refused): keep it as credit."""
        if count <= 0:
            return
        self._void_refused_credit[event_name] = self._void_refused_credit.get(event_name, 0) + count
        logger.warning(
            'Charge for %s unstored %r result(s) could not be voided; the next push of %r uses it instead of charging again',
            count,
            event_name,
            event_name,
        )

    def _take_void_credit(self, event_name: str, count: int) -> int:
        """Use credit for a whole charge of ``count`` events (all or nothing); returns the count used."""
        have = self._void_refused_credit.get(event_name, 0)
        if count <= 0 or have < count:
            return 0
        self._void_refused_credit[event_name] = have - count
        return count

    def _return_void_credit(self, event_name: str, count: int) -> None:
        if count > 0:
            self._void_refused_credit[event_name] = self._void_refused_credit.get(event_name, 0) + count

    async def _void_or_credit(self, event_name: str, operation_id: str, reserved: int, uncertain: bool) -> str:
        """Void (or cancel) a charge for results that were not stored; a refused void becomes credit."""
        outcome = await self._void_outcome(event_name, operation_id, reserved, uncertain)
        if outcome == 'stands':
            self._note_unvoidable(event_name, reserved)
        return outcome

    async def _void_charge(
        self,
        event_name: str,
        operation_id: str,
        reserved: int,
        not_found_means_voided: bool = False,
        deadline: Optional[float] = None,
    ) -> bool:
        """Reverse an accepted charge whose items were never stored, then unwind locally.

        POST /v2/actor-runs/{run}/charge/void with the charge's Idempotency-Key.
        The local reservation is released only once the platform confirms.
        """
        if self._client is None or not self._is_at_home or not self._actor_run_id:
            return False
        started = time.monotonic()
        budget_end = started + self._RETRY_BUDGET_SECS
        deadline = budget_end if deadline is None else min(deadline, budget_end)
        for attempt in range(self._POST_ATTEMPTS):
            try:
                await self._client.post(
                    f'/v2/actor-runs/{self._actor_run_id}/charge/void',
                    json_data={},
                    headers={'Idempotency-Key': operation_id},
                    idempotent=True,
                    retry_deadline=deadline,
                )
                self._unwind_reservation(event_name, reserved)
                return True
            except Exception as err:  # noqa: BLE001
                status = _http_status_of(err)
                # Only the void route's own answer proves it never applied; a server
                # without the route (older deployment) also answers 404.
                if status == 404 and not_found_means_voided and getattr(err, 'type', None) == 'charge-not-found':
                    # The unconfirmed charge never reached the ledger: nothing billed.
                    self._unwind_reservation(event_name, reserved)
                    return True
                # 404: nothing to void; 409: already settled; 4xx auth: permanent.
                # 408/429 (rate limited) and 5xx are not answers: re-queue below.
                if status is not None and 400 <= status < 500 and not _is_transient_status(status):
                    return False
                if not await self._backoff_before_retry(attempt, started, err, deadline):
                    break
        # Outcome unknown: keep the reservation and retry before the next push / at exit.
        self._pending_voids.append((event_name, operation_id, reserved, not_found_means_voided))
        logger.error('Void of charge %s (%r) not confirmed yet; will retry', operation_id, event_name)
        return False

    async def _retry_pending_voids(self, deadline: Optional[float] = None) -> bool:
        """Retry unconfirmed voids (caller holds the operation lock).

        With ``deadline`` (``time.monotonic()``, the exit flush) keeps retrying
        with backoff while time is left. Returns False while any earlier void
        is still unconfirmed.
        """
        from .storage.api_client import server_retry_delay

        round_ = 0
        while True:
            pending, self._pending_voids = self._pending_voids, []
            for event_name, operation_id, reserved, not_found_means_voided in pending:
                await self._void_charge(event_name, operation_id, reserved, not_found_means_voided, deadline)
            if not self._pending_voids:
                return True
            left = 0.0 if deadline is None else deadline - time.monotonic()
            if left <= 0:
                return False
            await asyncio.sleep(min(server_retry_delay(round_, None), left))
            round_ += 1

    def _quarantine_stale_pending(self) -> None:
        """Remove intents older than the safe replay window (fail closed, no replay).

        Unwinds the local reservation so quarantining cannot permanently consume budget.
        """
        now = time.monotonic()
        stale: List[tuple[str, Dict[str, Any]]] = []
        with self._charge_lock:
            for key, pending in self._pending_charge_keys.items():
                age = now - float(pending.get('created_at', now) or now)
                if age > self._IDEMPOTENCY_SAFE_AGE_SECS:
                    stale.append((key, dict(pending)))
            for key, _pending in stale:
                self._pending_charge_keys.pop(key, None)
        for key, pending in stale:
            event_name = str(pending.get('event_name') or '')
            count = int(pending.get('count', 0) or 0)
            if event_name and count > 0:
                self._unwind_reservation(event_name, count)
            logger.error(
                'Quarantined stale charge intent past safe idempotency age '
                '(key=%s event=%s count=%s); unwound local reservation; will not replay',
                key,
                pending.get('event_name'),
                pending.get('count'),
            )

    async def _post_charge_once(
        self,
        *,
        event_name: str,
        count: int,
        idempotency_key: str,
        deadline: Optional[float] = None,
    ) -> Dict[str, Any]:
        if self._client is None:
            raise RuntimeError('ChargingManager has no API client; cannot post charge intent')
        response = await self._client.post(
            f'/v2/actor-runs/{self._actor_run_id}/charge',
            json_data={
                'eventName': event_name,
                'count': count,
            },
            headers={'Idempotency-Key': idempotency_key},
            idempotent=True,
            retry_deadline=deadline,
        )
        if isinstance(response, dict):
            return response
        # httpx.Response-like
        try:
            body = response.json()
            if isinstance(body, dict):
                return body
        except Exception:
            pass
        return {}

    async def _backoff_before_retry(
        self, attempt: int, started: float, err: BaseException, deadline: Optional[float] = None
    ) -> bool:
        """Sleep before the next same-key attempt; False when no attempt is left.

        A 429/5xx answer reaching this point was already retried by the API
        client for the whole budget, so it ends the attempts. Timeouts, 408 and
        other errors back off the same way while attempts and the retry budget
        last.
        """
        from .storage.api_client import server_retry_delay

        if attempt + 1 >= self._POST_ATTEMPTS:
            return False
        status = _http_status_of(err)
        if status is not None and (status == 429 or status >= 500):
            return False
        delay = server_retry_delay(attempt, None)
        if time.monotonic() - started + delay > self._RETRY_BUDGET_SECS:
            return False
        if deadline is not None and time.monotonic() + delay > deadline:
            return False
        logger.warning('Charge request failed (%s); retrying in %.1fs', type(err).__name__, delay)
        await asyncio.sleep(delay)
        return True

    async def _post_charge_with_retries(
        self,
        *,
        event_name: str,
        count: int,
        idempotency_key: str,
        deadline: Optional[float] = None,
    ) -> Optional[int]:
        """POST charge; return acceptedCount on success, None if uncertain.

        ``deadline`` (``time.monotonic()``) bounds every retry, the API
        client's included, so a flush with a time budget ends with the intent
        still pending instead of being cancelled mid-request.

        Raises ChargeRejectedError for definitive platform rejections.
        Same idempotency key on every attempt.
        """
        last_error: Optional[BaseException] = None
        started = time.monotonic()
        # The client's own retries count against the same budget.
        budget_end = started + self._RETRY_BUDGET_SECS
        deadline = budget_end if deadline is None else min(deadline, budget_end)
        for attempt in range(self._POST_ATTEMPTS):
            try:
                body = await self._post_charge_once(
                    event_name=event_name,
                    count=count,
                    idempotency_key=idempotency_key,
                    deadline=deadline,
                )
                self._last_remaining_budget_nd = _read_remaining_budget_nd(body)
                return _read_accepted_count(body, count)
            except Exception as e:
                last_error = e
                rejected = _classify_charge_http_error(
                    e,
                    operation_id=idempotency_key,
                    event_name=event_name,
                    count=count,
                )
                if rejected is not None:
                    raise rejected from e
                if not await self._backoff_before_retry(attempt, started, e, deadline):
                    break
        if last_error is not None:
            logger.warning(
                'Failed to record charge for event %s (count=%s, key=%s): %s',
                event_name,
                count,
                idempotency_key,
                last_error,
            )
        return None

    async def flush_pending(
        self, *, max_intents: int = 32, deadline_ms: float | None = None
    ) -> int:
        """Best-effort flush under the operation lock; drain until empty or deadline."""
        if not self._is_at_home or self._client is None or not self._actor_run_id:
            return 0
        async with self._operation_lock:
            return await self._flush_pending_exclusive(
                max_intents=max_intents, deadline_ms=deadline_ms
            )

    async def _flush_pending_exclusive(
        self, *, max_intents: int = 32, deadline_ms: float | None = None
    ) -> int:
        """Caller must hold _operation_lock."""
        if not self._is_at_home or self._client is None or not self._actor_run_id:
            return 0
        deadline = None if deadline_ms is None else (time.monotonic() + max(0.0, deadline_ms) / 1000.0)
        # Voids first: an unconfirmed one means the user may be billed for items
        # that were never stored. At exit (with a deadline) keep retrying them.
        await self._retry_pending_voids(deadline)
        confirmed_total = 0
        async with self._flush_lock:
            while True:
                if deadline is not None and time.monotonic() >= deadline:
                    break
                self._quarantine_stale_pending()
                snapshot = self._snapshot_pending()[: max(0, max_intents)]
                if not snapshot:
                    break
                confirmed = 0
                for idempotency_key, pending in snapshot:
                    if deadline is not None and time.monotonic() >= deadline:
                        break
                    event_name = str(pending.get('event_name') or '')
                    count = int(pending.get('count', 0) or 0)
                    if not event_name or count <= 0:
                        self._drop_pending(idempotency_key)
                        continue
                    try:
                        accepted = await self._post_charge_with_retries(
                            event_name=event_name,
                            count=count,
                            idempotency_key=idempotency_key,
                            deadline=deadline,
                        )
                    except ChargeRejectedError as err:
                        if _is_charge_limit_rejection(err):
                            self._mark_charge_limit_hit()
                        self._unwind_reservation(event_name, count)
                        self._drop_pending(idempotency_key)
                        logger.warning(
                            'Dropping rejected pending charge (event=%s key=%s): %s',
                            event_name,
                            idempotency_key,
                            err,
                        )
                        continue
                    if accepted is not None:
                        if accepted < count:
                            self._mark_charge_limit_hit()
                            self._unwind_reservation(event_name, count - accepted)
                        self._drop_pending(idempotency_key)
                        confirmed += 1
                confirmed_total += confirmed
                if confirmed == 0:
                    break
        await self._persist_pending_charges()
        return confirmed_total

    async def charge(self, event_name: str, count: int = 1) -> ChargeResult:
        """Atomically reserve and submit one logical charge operation."""
        async with self._operation_lock:
            result = await self._charge_exclusive(event_name, count)
            self._note_prepaid_charge(event_name, result.charged_count)
            return result

    async def _charge_exclusive(self, event_name: str, count: int = 1) -> ChargeResult:
        if self._closed:
            raise RuntimeError('ChargingManager is closed (actor is shutting down); charge rejected')
        """Charge for a specified number of events.

        Args:
            event_name: Name of the event to charge
            count: Number of events to charge

        Returns:
            ChargeResult indicating if the limit was reached

        Raises:
            ChargeUncertainError: POST could not be confirmed after retries; the
                operation remains reserved locally and pending for flush.
        """
        if not self._initialized:
            await self.init()

        # Not a PPE actor — ignore charges (matches JS ChargingManager).
        if self._pricing_info is None:
            if not self._not_ppe_warning_printed:
                logger.warning(
                    'Ignored attempt to charge for an event - the Actor does not use the pay-per-event pricing'
                )
                self._not_ppe_warning_printed = True
            return ChargeResult(
                chargeable_within_limit={},
                charged_count=0,
                event_charge_limit_reached=False,
            )

        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ValueError('count must be a positive integer')

        # Best-effort: clear older unresolved intents before starting a new one.
        try:
            await self._flush_pending_exclusive()
        except Exception as e:
            logger.warning('flush_pending before charge failed: %s', e)

        idempotency_key: Optional[str] = None
        chargeable_count = 0
        will_post = False

        with self._charge_lock:
            event_price, event_title = self._get_event_pricing(event_name)
            affordability_price = self._calculate_event_price(event_name) or Decimal('0')

            # JS ChargingManager parity: canPost requires isAtHome — a local
            # process with an API client/run ID must never post platform charges.
            can_post_base = (
                self._is_at_home
                and bool(self._actor_run_id)
                and self._client is not None
                and not is_default_dataset_item_event(event_name)
                and event_name in self._per_event_prices
            )

            # Budget gate (JS parity): trim unaffordable counts, but keep the
            # legacy one-event overdraw while the budget is not yet exceeded.
            chargeable_count = count
            if self._max_total_charge_usd is not None and affordability_price > 0:
                remaining = self._remaining_budget_usd()
                max_count_by_limit = max(0, int(remaining / affordability_price))
                if count > max_count_by_limit:
                    chargeable_count = (
                        max_count_by_limit + 1
                        if self._total_charged_usd <= self._max_total_charge_usd
                        else 0
                    )

            if chargeable_count < count:
                self._mark_charge_limit_hit()

            if chargeable_count <= 0:
                return ChargeResult(
                    chargeable_within_limit=self._compute_chargeable(),
                    charged_count=0,
                    event_charge_limit_reached=True,
                )

            will_post = can_post_base and chargeable_count > 0

            if will_post:
                if len(self._pending_charge_keys) >= self._PENDING_MAX_ENTRIES:
                    logger.warning(
                        'Too many unresolved charge intents (%s); refusing new '
                        'billable charge for event %s until pending POSTs succeed',
                        len(self._pending_charge_keys),
                        event_name,
                    )
                    return ChargeResult(
                        chargeable_within_limit=self._compute_chargeable(),
                        charged_count=0,
                        event_charge_limit_reached=False,
                    )
                idempotency_key = str(uuid.uuid4())
                self._pending_charge_keys[idempotency_key] = {
                    'idempotency_key': idempotency_key,
                    'created_at': time.monotonic(),
                    'created_wall_ms': int(time.time() * 1000),
                    'event_name': event_name,
                    'count': chargeable_count,
                }

            # Reserve local budget once per logical operation.
            if event_name not in self._charged_events:
                self._charged_events[event_name] = 0
            self._charged_events[event_name] += chargeable_count
            self._total_charged_usd += event_price * Decimal(chargeable_count)

            result = ChargeResult(
                chargeable_within_limit=self._compute_chargeable(),
                charged_count=chargeable_count,
                event_charge_limit_reached=self._is_event_limit_reached(event_name),
            )

        # Record charge with the platform.
        # Synthetic default-dataset events are tracked locally only.
        if will_post and idempotency_key is not None:
            try:
                accepted = await self._post_charge_with_retries(
                    event_name=event_name,
                    count=chargeable_count,
                    idempotency_key=idempotency_key,
                )
            except ChargeRejectedError as rejected:
                if _is_charge_limit_rejection(rejected):
                    self._mark_charge_limit_hit()
                # Server definitively rejected: unwind local reservation.
                self._drop_pending(idempotency_key)
                self._unwind_reservation(event_name, chargeable_count)
                return ChargeResult(
                    chargeable_within_limit=self._compute_chargeable(),
                    charged_count=0,
                    event_charge_limit_reached=True,
                )
            if accepted is not None:
                self._drop_pending(idempotency_key)
                if accepted > 0:
                    self._last_accepted_operation_id = idempotency_key
                if accepted < chargeable_count:
                    # The platform capped the charge at the run's limit.
                    self._mark_charge_limit_hit()
                    self._unwind_reservation(event_name, chargeable_count - accepted)
                    chargeable_count = accepted
                    result = ChargeResult(
                        chargeable_within_limit=self._compute_chargeable(),
                        charged_count=chargeable_count,
                        event_charge_limit_reached=self._is_event_limit_reached(event_name),
                    )
                self._sync_platform_budget()
                if self._max_total_charge_usd is not None and self._platform_other_usd > 0:
                    # Usage may have used up what is left: report it on this result.
                    result = ChargeResult(
                        chargeable_within_limit=self._compute_chargeable(),
                        charged_count=result.charged_count,
                        event_charge_limit_reached=result.event_charge_limit_reached
                        or self._is_event_limit_reached(event_name),
                    )
            else:
                # Not confirmed: keep a durable copy so a crash before the next
                # flush cannot lose it (settlement re-applies it).
                await self._persist_pending_charges()
                raise ChargeUncertainError(
                    f'Charge for event {event_name!r} (count={chargeable_count}) '
                    f'was not confirmed by the platform; operation_id={idempotency_key}',
                    operation_id=idempotency_key,
                    event_name=event_name,
                    count=chargeable_count,
                )
        elif (
            chargeable_count > 0
            and self._is_at_home
            and not is_default_dataset_item_event(event_name)
            and event_name not in self._per_event_prices
            and self._pricing_info is not None
        ):
            logger.warning("Attempting to charge for an unknown event '%s'", event_name)

        if self._charging_log_dataset is not None:
            try:
                await self._charging_log_dataset.push_data({
                    'eventName': event_name,
                    'eventTitle': event_title,
                    'eventPriceUsd': float(event_price),
                    'chargedCount': chargeable_count,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                })
            except Exception as log_err:
                logger.warning('Failed to append charging log entry: %s', log_err)

        if chargeable_count < count:
            subject = 'instance' if count == 1 else 'instances'
            logger.info(
                "Charging %s %s of '%s' event would exceed maxTotalChargeUsd - "
                'only %s events were charged',
                count,
                subject,
                event_name,
                chargeable_count,
            )

        return result

    def calculate_max_event_charge_count_within_limit(self, event_name: str) -> Optional[int]:
        """Calculate how many events can be charged before reaching the limit.

        Args:
            event_name: Name of the inspected event

        Returns:
            Maximum count that can be charged, or None if no limit
        """
        if self._max_total_charge_usd is None:
            return None

        event_price = self._calculate_event_price(event_name)
        if event_price is None or event_price <= 0:
            return None

        remaining = self._remaining_budget_usd()
        return max(0, int(remaining / event_price))

    def calculate_total_charged_amount(self) -> Decimal:
        """Calculate the total amount charged so far.

        Returns:
            Total charged amount in USD
        """
        return self._total_charged_usd

    def compute_chargeable(self) -> Dict[str, Optional[int]]:
        """Compute max events of each type that can be charged within budget.

        Returns:
            Dictionary of event names to maximum chargeable counts
        """
        result = {}
        for event_name in self._per_event_prices:
            result[event_name] = self.calculate_max_event_charge_count_within_limit(event_name)
        return result

    def compute_push_data_limit(
        self,
        items_count: int,
        event_name: str,
        *,
        is_default_dataset: bool = False,
    ) -> int:
        """Compute how many items can be pushed and charged within budget.

        Accounts for both the explicit event and the default dataset item event.

        Args:
            items_count: Number of items to push
            event_name: Explicit event name to charge for each item
            is_default_dataset: Whether pushing to default dataset

        Returns:
            Maximum number of items that can be pushed within budget
        """
        # Calculate combined price per item (effective prices: $1 per event locally)
        explicit_price = (
            self._calculate_event_price(event_name) or Decimal('0') if event_name else Decimal('0')
        )
        default_price = Decimal('0')

        if is_default_dataset:
            default_event = self._config.get('default_dataset_item_event_name')
            if not default_event:
                default_event = resolve_default_dataset_item_event_name(self._per_event_prices)
            if default_event:
                default_price = self._calculate_event_price(default_event) or Decimal('0')

        combined_price = explicit_price + default_price

        if combined_price <= 0:
            return items_count

        if self._max_total_charge_usd is None:
            return items_count

        remaining = self._remaining_budget_usd()
        max_items = int(remaining / combined_price)

        return min(items_count, max(0, max_items))

    def get_charged_event_count(self, event_name: str) -> int:
        """Get the number of events charged so far.

        Args:
            event_name: Name of the inspected event

        Returns:
            Number of events charged
        """
        if is_default_dataset_item_event(event_name):
            return (
                self._charged_events.get(APIFY_DEFAULT_DATASET_ITEM_EVENT, 0)
                + self._charged_events.get(SCRAPELY_DEFAULT_DATASET_ITEM_EVENT, 0)
            )
        return self._charged_events.get(event_name, 0)

    def get_max_total_charge_usd(self) -> Decimal:
        """Get the configured maximum total charge.

        Returns:
            Maximum total charge in USD
        """
        # No configured cap (or zero) means unlimited — JS returns Infinity.
        return self._max_total_charge_usd if self._max_total_charge_usd else Decimal('Infinity')

    def get_pricing_info(self) -> ActorPricingInfoResult:
        """Retrieve detailed pricing information for the current Actor run.

        Returns:
            ActorPricingInfoResult with pricing details
        """
        return ActorPricingInfoResult(
            is_pay_per_event=self._pricing_info is not None,
            max_total_charge_usd=(
                self.get_max_total_charge_usd()
                if self.get_max_total_charge_usd().is_finite()
                else float('inf')
            ),
            per_event_prices=self._per_event_prices,
            pricing_model=PricingModel.PAY_PER_EVENT if self._pricing_info else None,
        )

    def is_event_charge_limit_reached(self, event_name: str) -> bool:
        """Check if the budget is insufficient to charge even one more event.

        Args:
            event_name: Name of the event to check

        Returns:
            True if no more events of this type can be charged
        """
        event_price = self._calculate_event_price(event_name) or Decimal('0')

        if self._max_total_charge_usd is None:
            return False

        remaining = self._remaining_budget_usd()
        return remaining < event_price

    def _compute_chargeable(self) -> Dict[str, Optional[int]]:
        """Internal method to compute chargeable counts."""
        return self.compute_chargeable()

    def _is_event_limit_reached(self, event_name: str) -> bool:
        """Internal method to check if event limit is reached."""
        return self.is_event_charge_limit_reached(event_name)

    def get_charged_events(self) -> Dict[str, int]:
        """Get the count of charged events by event name.

        Returns:
            Dictionary of event names to counts
        """
        return self._charged_events.copy()

    def get_total_charged_usd(self) -> Decimal:
        """Get the total amount charged in USD.

        Returns:
            Total charged amount
        """
        return self._total_charged_usd



def _is_charge_limit_rejection(err: ChargeRejectedError) -> bool:
    """Whether a rejected charge was refused because the run's limit is used up."""
    return (
        err.status_code == 402
        or err.reason == 'charge-limit-exceeded'
        or 'charge-limit-exceeded' in str(err).lower()
    )


def _read_remaining_budget_nd(body: Any) -> Optional[int]:
    """remainingBudgetNd from a charge response (platforms that send it), else None."""
    if not isinstance(body, dict):
        return None
    data = body.get('data') if isinstance(body.get('data'), dict) else body
    raw = data.get('remainingBudgetNd') if isinstance(data, dict) else None
    if isinstance(raw, (int, float)) and not isinstance(raw, bool) and raw >= 0:
        return int(raw)
    return None


def _read_accepted_count(body: Any, fallback: int) -> int:
    if not isinstance(body, dict):
        return fallback
    # Support both bare body and {data: ...} envelopes.
    data = body.get('data') if isinstance(body.get('data'), dict) else body
    raw = data.get('acceptedCount') if isinstance(data, dict) else None
    if isinstance(raw, (int, float)) and raw >= 0:
        return int(raw)
    return fallback


def _classify_charge_http_error(
    err: BaseException,
    *,
    operation_id: str,
    event_name: str,
    count: int,
) -> Optional[ChargeRejectedError]:
    status = getattr(err, 'status_code', None)
    if status is None:
        response = getattr(err, 'response', None)
        status = getattr(response, 'status_code', None) if response is not None else None

    # 401/403: ownership or scoped-run authorization failures are permanent.
    if status in (400, 401, 402, 403, 404, 405, 409):
        return ChargeRejectedError(
            f'Charge rejected by platform (HTTP {status}) for event {event_name!r}',
            operation_id=operation_id,
            event_name=event_name,
            count=count,
            status_code=int(status) if status is not None else None,
            reason='charge-limit-exceeded' if status == 402 else f'http-{status}',
        )

    # A 5xx / 408 / 429 answer is never a definitive rejection, whatever its text
    # says: the charge may have applied, and only a retry with the same
    # Idempotency-Key can tell (classifying it as rejected unwound the local
    # count and the caller charged again under a new key).
    if _is_transient_status(status if isinstance(status, int) else None):
        return None
    message = str(err)
    if any(
        token in message.lower()
        for token in (
            'charge-limit-exceeded',
            'unknown-event',
            'ledger-sealed',
            'invalid-input',
            'conflict',
        )
    ):
        return ChargeRejectedError(
            message,
            operation_id=operation_id,
            event_name=event_name,
            count=count,
            status_code=int(status) if isinstance(status, int) else None,
            reason='message-match',
        )
    return None


def _assert_items_serializable(items: List[Any]) -> None:
    """Fail before anything is charged when an item cannot be sent to the dataset.

    The storage client serializes items the same way (``json.dumps`` with
    ``default=str``); an item it cannot serialize (e.g. a circular reference or
    a non-string dict key) would otherwise fail the push after the charge.
    """
    for index, item in enumerate(items):
        try:
            json.dumps(item, ensure_ascii=False, default=str)
        except (TypeError, ValueError) as err:
            raise ValueError(f'Dataset item at index {index} is not serializable to JSON: {err}') from err


async def push_data_and_charge(
    charging_manager: ChargingManager,
    items: Union[Dict[str, Any], List[Dict[str, Any]]],
    event_name: Optional[str],
    is_default_dataset: bool,
    push_fn: Any,
) -> ChargeResult:
    """Push data to dataset and charge for each item.

    Limits items to remaining budget before push (JS parity).

    Args:
        charging_manager: Charging manager instance
        items: Items to push
        event_name: Event name for charging
        is_default_dataset: Whether pushing to default dataset
        push_fn: Function to push items

    Returns:
        ChargeResult
    """
    if not isinstance(items, list):
        items = [items]

    async with charging_manager._operation_lock:
        if charging_manager._closed:
            raise RuntimeError('ChargingManager is closed (actor is shutting down); charge rejected')
        # A void that did not go through for an earlier failed push must land
        # before this push charges again, or one unsaved result is billed twice.
        voids_confirmed = await charging_manager._retry_pending_voids()
        # Non-PPE actors push everything without charging (JS calculatePushDataLimits
        # parity: `if (!this.isPayPerEvent) return { limitedItems, eventsToCharge: {} }`).
        is_ppe = charging_manager._pricing_info is not None

        resolved_event = event_name
        if is_ppe and not resolved_event and is_default_dataset:
            fallback_event = charging_manager._config.get('default_dataset_item_event_name')
            if not fallback_event:
                fallback_event = resolve_default_dataset_item_event_name(
                    charging_manager._per_event_prices
                )
            if fallback_event:
                resolved_event = fallback_event

        # Charge-limit safety net: once the run's spending limit is used up, a
        # default-dataset push that does not pay per row (no event, or only
        # free events) stores nothing. Paid pushes go on below, where the
        # budget cap stores only the rows that are actually charged.
        # Rows the Actor already paid for with an earlier charge() (charge first,
        # then a plain push) are still stored: see ChargingManager._take_unpaid_rows.
        # The result still reports the limit, so the Actor stops after them.
        unpaid_past_limit = False
        if (
            is_ppe
            and is_default_dataset
            and items
            and not _push_is_paid_per_item(charging_manager, event_name, resolved_event)
        ):
            unpaid_past_limit = charging_manager.is_charge_limit_reached()
            allowed = charging_manager._take_unpaid_rows(len(items))
            if allowed < len(items):
                charging_manager._note_refused_default_dataset_items(len(items) - allowed)
                if allowed == 0:
                    return ChargeResult(
                        chargeable_within_limit=charging_manager.compute_chargeable(),
                        charged_count=0,
                        event_charge_limit_reached=True,
                    )
                items = items[:allowed]

        limited_count = len(items)
        if is_ppe and resolved_event:
            # Keep the explicit event separate from the implicit default-dataset
            # event. Passing resolved_event here would count the default price twice.
            limited_count = charging_manager.compute_push_data_limit(
                len(items),
                event_name or '',
                is_default_dataset=is_default_dataset,
            )

        limited_items = items[:limited_count] if limited_count < len(items) else items
        if is_ppe and is_default_dataset and limited_count < len(items):
            # Rows beyond what the budget can pay for are dropped.
            charging_manager._mark_charge_limit_hit()
            charging_manager._note_refused_default_dataset_items(len(items) - limited_count)

        if not limited_items:
            return ChargeResult(
                chargeable_within_limit=charging_manager.compute_chargeable(),
                charged_count=0,
                event_charge_limit_reached=len(items) > 0 and limited_count == 0,
            )

        # Every platform event this push would charge counts: the explicit event
        # and, on a default-dataset push, the configured default event. The
        # synthetic default-dataset event (local bookkeeping) and unpriced
        # events (never posted) do not.
        charged_names: List[str] = []
        if event_name:
            charged_names.append(event_name)
        if is_default_dataset:
            configured_default = charging_manager._config.get('default_dataset_item_event_name')
            if configured_default:
                charged_names.append(configured_default)
        if (
            not voids_confirmed
            and is_ppe
            and any(
                not is_default_dataset_item_event(name) and name in charging_manager._per_event_prices
                for name in charged_names
            )
        ):
            raise RuntimeError(
                'An earlier charge for a failed dataset push could not be voided yet; retry the push'
            )

        # Charge BEFORE persisting so a server cap/reject cannot leave unbilled
        # items in the dataset. Operation lock is held across both steps.
        if is_ppe and resolved_event:
            _assert_items_serializable(limited_items)
            results: List[ChargeResult] = []
            default_event: Optional[str] = None
            if is_default_dataset:
                default_event = charging_manager._config.get('default_dataset_item_event_name')
                if not default_event:
                    default_event = resolve_default_dataset_item_event_name(
                        charging_manager._per_event_prices
                    )

            # Synthetic storage event first (local-only); then explicit billable event.
            reserved_in_op: list[tuple[str, int]] = []
            operation_ids: list[Optional[str]] = []
            uncertain_ops: set[str] = set()
            # Events of this push paid from credit (an earlier charge that could not be voided).
            credit_used: list[int] = []

            async def charge_one(name: str) -> None:
                charging_manager._take_last_accepted_operation_id()
                credit = charging_manager._take_void_credit(name, len(limited_items))
                if credit:
                    results.append(ChargeResult(
                        chargeable_within_limit=charging_manager.compute_chargeable(),
                        charged_count=credit,
                        event_charge_limit_reached=False,
                    ))
                    reserved_in_op.append((name, credit))
                    operation_ids.append(None)
                    credit_used.append(credit)
                    return
                try:
                    r = await charging_manager._charge_exclusive(name, len(limited_items))
                    op_id = charging_manager._take_last_accepted_operation_id()
                except ChargeUncertainError as unconfirmed:
                    # The platform did not confirm the charge (5xx / timeouts after
                    # retries). Never drop the items for that: the intent stays
                    # pending with its idempotency key and is re-posted by the exit
                    # flush, so the items are stored now and billed once.
                    logger.warning(
                        'Charge for %r not confirmed yet (%s); storing the items and retrying the charge later',
                        name,
                        unconfirmed.operation_id,
                    )
                    r = ChargeResult(
                        chargeable_within_limit=charging_manager.compute_chargeable(),
                        charged_count=unconfirmed.count,
                        event_charge_limit_reached=False,
                    )
                    op_id = unconfirmed.operation_id
                    uncertain_ops.add(op_id)
                results.append(r)
                reserved_in_op.append((name, r.charged_count))
                operation_ids.append(op_id)
                credit_used.append(0)

            async def undo_op(i: int, credit_if_stands: bool = True) -> str:
                """Undo charge i of this operation: void a platform charge, unwind a local one.

                A charge paid from credit gives the credit back. A platform charge
                whose void is refused becomes credit (its results were not stored),
                unless the caller handles that itself (``credit_if_stands=False``).
                """
                name, reserved = reserved_in_op[i]
                op_id = operation_ids[i]
                if reserved <= 0:
                    return 'voided'
                if credit_used[i]:
                    charging_manager._return_void_credit(name, credit_used[i])
                    credit_used[i] = 0
                    return 'voided'
                if op_id:
                    if credit_if_stands:
                        return await charging_manager._void_or_credit(name, op_id, reserved, op_id in uncertain_ops)
                    return await charging_manager._void_outcome(name, op_id, reserved, op_id in uncertain_ops)
                charging_manager._unwind_reservation(name, reserved)
                return 'voided'

            try:
                if default_event:
                    await charge_one(default_event)
                if event_name and event_name != default_event:
                    await charge_one(event_name)
            except Exception:
                # A later event failed: undo the earlier ones, including charges the
                # platform already accepted (voided there, not only unwound here).
                for i in range(len(reserved_in_op)):
                    await undo_op(i)
                raise

            # One platform event accepted more than another (e.g. a configured,
            # posted default-dataset event and the explicit event): only the
            # smaller count of items is stored, so the larger charge is voided and
            # charged again for exactly the stored count. A charge that cannot be
            # voided (definitive answer) stays as before; one whose void is not
            # confirmed yet fails the push so the caller retries it.
            stood: set[int] = set()
            for _round in range(3):
                accepted_now = min(len(limited_items), *(r.charged_count for r in results)) if results else 0
                excess = [
                    i for i in range(len(results))
                    if operation_ids[i] and i not in stood and results[i].charged_count > accepted_now
                ]
                if accepted_now <= 0 or not excess:
                    break
                for i in excess:
                    name = reserved_in_op[i][0]
                    # A refused void here keeps its stored share; the rest becomes credit below.
                    outcome = await undo_op(i, credit_if_stands=False)
                    if outcome == 'stands':
                        stood.add(i)
                        continue
                    if outcome == 'unknown':
                        for j in range(len(reserved_in_op)):
                            if j != i:
                                await undo_op(j)
                        raise RuntimeError(
                            'An earlier charge for a failed dataset push could not be voided yet; retry the push'
                        )
                    charging_manager._take_last_accepted_operation_id()
                    try:
                        r = await charging_manager._charge_exclusive(name, accepted_now)
                        new_op = charging_manager._take_last_accepted_operation_id()
                    except ChargeUncertainError as unconfirmed:
                        r = ChargeResult(
                            chargeable_within_limit=charging_manager.compute_chargeable(),
                            charged_count=unconfirmed.count,
                            event_charge_limit_reached=False,
                        )
                        new_op = unconfirmed.operation_id
                        uncertain_ops.add(new_op)
                    results[i] = r
                    reserved_in_op[i] = (name, r.charged_count)
                    operation_ids[i] = new_op
                    credit_used[i] = 0

            if results:
                accepted_for_items = min(
                    len(limited_items),
                    *(result.charged_count for result in results),
                )
                # Unwind any event that reserved more than we will persist
                # (synthetic default-dataset is local-only and otherwise sticks).
                for i, result in enumerate(results):
                    name = reserved_in_op[i][0]
                    over = result.charged_count - accepted_for_items
                    if over <= 0:
                        continue
                    # Nothing stored and a platform charge was made: the void
                    # below releases its local count once the platform confirms.
                    if accepted_for_items <= 0 and operation_ids[i]:
                        continue
                    back = min(over, credit_used[i])
                    if back:
                        charging_manager._return_void_credit(name, back)
                        credit_used[i] -= back
                        reserved_in_op[i] = (name, reserved_in_op[i][1] - back)
                    rest = over - back
                    if rest <= 0:
                        continue
                    if i in stood:
                        # The platform refused to void it: the unstored share stays billed.
                        charging_manager._note_unvoidable(name, rest)
                    else:
                        charging_manager._unwind_reservation(name, rest)
                if accepted_for_items <= 0:
                    # Nothing will be stored (another event of this push was capped
                    # to zero): undo every platform charge this operation made, so
                    # no result is billed for items that are never saved. Local
                    # counts were unwound just above.
                    for i, ((name, reserved), op_id) in enumerate(zip(reserved_in_op, operation_ids)):
                        if credit_used[i]:
                            charging_manager._return_void_credit(name, credit_used[i])
                            reserved -= credit_used[i]
                            credit_used[i] = 0
                        if reserved <= 0 or not op_id:
                            continue
                        # Original count: released only when the platform confirms.
                        await charging_manager._void_or_credit(name, op_id, reserved, op_id in uncertain_ops)
                if is_default_dataset and accepted_for_items < len(limited_items):
                    charging_manager._note_refused_default_dataset_items(
                        len(limited_items) - accepted_for_items
                    )
                if accepted_for_items > 0:
                    try:
                        await _gated_push(push_fn, limited_items[:accepted_for_items])
                    except Exception as push_err:
                        # A result is billed only if it was saved: void this
                        # operation's platform charges and unwind local
                        # reservations, then let the caller retry the push.
                        logger.error(
                            'Dataset push failed after charge accepted (items=%s): %s; voiding the charge',
                            accepted_for_items,
                            push_err,
                        )
                        # Only when the push certainly stored nothing (the storage API
                        # answered with an error, or the request was never sent). A
                        # timeout after sending may have stored the items.
                        if not _push_certainly_failed(push_err):
                            raise
                        for i, ((name, _), op_id) in enumerate(zip(reserved_in_op, operation_ids)):
                            kept = min(accepted_for_items, results[i].charged_count)
                            if credit_used[i]:
                                back = min(kept, credit_used[i])
                                charging_manager._return_void_credit(name, back)
                                credit_used[i] -= back
                                kept -= back
                            if kept <= 0:
                                continue
                            if op_id:
                                # An unconfirmed charge is cancelled (never re-posted for
                                # items that were not stored); a refused void becomes credit.
                                await charging_manager._void_or_credit(name, op_id, kept, op_id in uncertain_ops)
                            elif is_default_dataset_item_event(name):
                                charging_manager._unwind_reservation(name, kept)
                        raise
                return ChargeResult(
                    chargeable_within_limit=charging_manager.compute_chargeable(),
                    # Rows charged for the pushed event; the
                    # implicit default-dataset event is not counted on top.
                    charged_count=accepted_for_items,
                    event_charge_limit_reached=any(
                        result.event_charge_limit_reached for result in results
                    )
                    or accepted_for_items < len(limited_items)
                    or unpaid_past_limit,
                )


        # No billable events — still persist the budget-limited items.
        await _gated_push(push_fn, limited_items)
        return ChargeResult(
            chargeable_within_limit=charging_manager.compute_chargeable(),
            charged_count=0,
            event_charge_limit_reached=unpaid_past_limit,
        )


def _validate_row_charges(charges: Any) -> Dict[str, int]:
    if not isinstance(charges, dict) or not charges:
        raise ValueError('charges must be a non-empty dict of {event_name: count_per_row}')
    out: Dict[str, int] = {}
    for name, count in charges.items():
        if not isinstance(name, str) or not name:
            raise ValueError('charges keys must be event names')
        if is_default_dataset_item_event(name):
            raise ValueError(f"Cannot charge for synthetic event '{name}' manually")
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ValueError(f'charges[{name!r}] must be a positive integer (events per row)')
        out[name] = count
    return out


async def push_data_with_charges(
    charging_manager: ChargingManager,
    items: Union[Dict[str, Any], List[Dict[str, Any]]],
    charges: Dict[str, int],
    is_default_dataset: bool,
    push_fn: Any,
    charge_per: str = 'row',
) -> ChargeResult:
    """Push rows that each cost several events, atomically per row.

    ``charge_per='batch'``: ``charges`` are charged ONCE for the whole push (e.g. one
    ``document`` for all of its page rows). If every event is accepted all rows are
    stored, otherwise none (and nothing stays billed). The prepaid-row balance is
    neither used nor changed.

    ``charges`` maps event name -> events per row, e.g. ``{'result': 1, 'page': 3}``.
    A row is stored only when every one of its events was charged: the full set
    is charged first, rows the budget cannot fully pay for are not stored, and
    any operation that came back short is voided and re-charged for the rows
    that are kept, so no row is ever stored partly paid and no event is billed
    for a row that was not stored. Unpriced events are ignored. On the default
    dataset the default-dataset-item event (when priced) is added per row.
    """
    charges = _validate_row_charges(charges)
    if charge_per not in ('row', 'batch'):
        raise ValueError("charge_per must be 'row' or 'batch'")
    batch = charge_per == 'batch'
    rows_in = items if isinstance(items, list) else [items]
    if not rows_in:
        return ChargeResult(chargeable_within_limit={}, charged_count=0, event_charge_limit_reached=False)

    async with charging_manager._operation_lock:
        if charging_manager._closed:
            raise RuntimeError('ChargingManager is closed (actor is shutting down); charge rejected')
        voids_confirmed = await charging_manager._retry_pending_voids()

        if charging_manager._pricing_info is None:
            await _gated_push(push_fn, rows_in)
            return ChargeResult(chargeable_within_limit={}, charged_count=0, event_charge_limit_reached=False)

        per_row: Dict[str, int] = {
            name: n for name, n in charges.items()
            if (charging_manager._calculate_event_price(name) or Decimal('0')) > 0
        }
        if is_default_dataset:
            default_event = charging_manager._config.get('default_dataset_item_event_name') or \
                resolve_default_dataset_item_event_name(charging_manager._per_event_prices)
            if default_event and default_event not in per_row and \
                    (charging_manager._calculate_event_price(default_event) or Decimal('0')) > 0:
                # Per stored row, also for a batch.
                per_row[default_event] = len(rows_in) if batch else 1
        if not per_row:
            # Every listed event is free: a plain (unpaid) push.
            return await _unpaid_push_locked(charging_manager, rows_in, is_default_dataset, push_fn)
        if not voids_confirmed and any(not is_default_dataset_item_event(n) for n in per_row):
            raise RuntimeError('An earlier charge for a failed dataset push could not be voided yet; retry the push')
        _assert_items_serializable(rows_in)

        # Rows the budget can pay for in full (no one-event overdraw: a row is all or nothing).
        row_cost = sum(
            (charging_manager._calculate_event_price(n) or Decimal('0')) * Decimal(c) for n, c in per_row.items()
        )
        # Units charged for: rows, or the one batch (row_cost is then the batch's cost).
        rows = 1 if batch else len(rows_in)
        if charging_manager._max_total_charge_usd is not None and row_cost > 0:
            remaining = charging_manager._remaining_budget_usd()
            rows = min(rows, max(0, int(remaining / row_cost)))

        ops: List[tuple] = []  # (event, charged, op_id, uncertain, paid_from_credit)

        async def undo(entries: List[tuple]) -> None:
            for name, charged, op_id, uncertain, credit in entries:
                if charged <= 0:
                    continue
                if credit:
                    charging_manager._return_void_credit(name, charged)
                elif op_id is None:
                    charging_manager._unwind_reservation(name, charged)
                else:
                    # Cancel / void; a refused void stays billed and becomes credit.
                    await charging_manager._void_or_credit(name, op_id, charged, uncertain)

        for _attempt in range(6):
            if rows <= 0:
                break
            ops = []
            short_to: Optional[int] = None
            for name, n in sorted(per_row.items(), key=lambda kv: (is_default_dataset_item_event(kv[0]), kv[0])):
                want = n * rows
                charging_manager._take_last_accepted_operation_id()
                if charging_manager._take_void_credit(name, want):
                    # Paid by an earlier charge whose results were not stored (void refused).
                    ops.append((name, want, None, False, True))
                    continue
                try:
                    r = await charging_manager._charge_exclusive(name, want)
                    op_id = charging_manager._take_last_accepted_operation_id()
                    ops.append((name, r.charged_count, op_id, False, False))
                    got = r.charged_count
                except ChargeUncertainError as unconfirmed:
                    # Not confirmed: the intent stays pending and is re-posted at exit,
                    # so it counts as accepted (items stored, billed once).
                    ops.append((name, unconfirmed.count, unconfirmed.operation_id, True, False))
                    got = unconfirmed.count
                if got < want:
                    short_to = got // n
                    break
            if short_to is None:
                break
            # Some event came back short: void this attempt in full and retry with
            # the whole rows the budget covers at the FULL per-row cost. Sizing
            # from the short event alone ignores the other events of the row, so
            # the retry came back short again (appstore 64df0a3608d74af88: 20
            # details -> 14, retry sized 14, app-item short, 0 saved). What this
            # attempt was accepted is budget the void gives back; with the
            # platform's remainingBudgetNd the local budget also knows the rest.
            attempt_spent = sum(
                (charging_manager._calculate_event_price(name) or Decimal('0')) * Decimal(charged)
                for name, charged, _op, _unc, _credit in ops
            )
            await undo(ops)
            ops = []
            fit = int(attempt_spent / row_cost) if row_cost > 0 else 0
            if charging_manager._platform_budget_seen and charging_manager._max_total_charge_usd is not None and row_cost > 0:
                fit = max(fit, int(charging_manager._remaining_budget_usd() / row_cost))
            # At least one more single-row try: without the platform's remaining
            # budget the leftover after the short answer is unknown, and the last
            # affordable row may still fit.
            rows = min(rows - 1, max(fit, 1))

        if rows <= 0 or not ops:
            await undo(ops)
            charging_manager._mark_charge_limit_hit()
            if is_default_dataset:
                charging_manager._note_refused_default_dataset_items(len(rows_in))
            return ChargeResult(
                chargeable_within_limit=charging_manager.compute_chargeable(),
                charged_count=0,
                event_charge_limit_reached=True,
            )

        kept = rows_in if batch else rows_in[:rows]
        if len(kept) < len(rows_in):
            charging_manager._mark_charge_limit_hit()
            if is_default_dataset:
                charging_manager._note_refused_default_dataset_items(len(rows_in) - len(kept))
        try:
            await _gated_push(push_fn, kept)
        except Exception as push_err:
            logger.error('Dataset push failed after charges accepted (rows=%s): %s; voiding the charges', rows, push_err)
            if _push_certainly_failed(push_err):
                await undo(ops)
            raise
        return ChargeResult(
            chargeable_within_limit=charging_manager.compute_chargeable(),
            charged_count=len(kept),
            event_charge_limit_reached=len(kept) < len(rows_in) or charging_manager.is_charge_limit_reached(),
        )


async def _unpaid_push_locked(
    charging_manager: ChargingManager, rows: List[Dict[str, Any]], is_default_dataset: bool, push_fn: Any
) -> ChargeResult:
    """A push whose rows carry no priced event (caller holds the operation lock)."""
    past_limit = charging_manager.is_charge_limit_reached()
    allowed = charging_manager._take_unpaid_rows(len(rows)) if is_default_dataset else len(rows)
    if allowed < len(rows):
        charging_manager._note_refused_default_dataset_items(len(rows) - allowed)
    if allowed > 0:
        await _gated_push(push_fn, rows[:allowed])
    return ChargeResult(
        chargeable_within_limit=charging_manager.compute_chargeable(),
        charged_count=0,
        event_charge_limit_reached=past_limit,
    )


def _push_is_paid_per_item(
    charging_manager: ChargingManager, event_name: Optional[str], resolved_event: Optional[str]
) -> bool:
    """Whether every row of this push is charged a positive price."""
    names = {name for name in (event_name, resolved_event) if name}
    return any((charging_manager._calculate_event_price(name) or Decimal('0')) > 0 for name in names)


async def _gated_push(push_fn: Any, items: List[Dict[str, Any]]) -> Any:
    """Run a push already decided by push_data_and_charge (storage skips its own check)."""
    token = _charge_gated_push.set(True)
    try:
        return await push_fn(items)
    finally:
        _charge_gated_push.reset(token)


def _http_status_of(err: BaseException) -> Optional[int]:
    status = getattr(err, 'status_code', None)
    if status is None:
        response = getattr(err, 'response', None)
        status = getattr(response, 'status_code', None) if response is not None else None
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _push_certainly_failed(err: BaseException) -> bool:
    """True when a dataset push certainly stored nothing.

    The storage API answered with an error status, or the connection was never
    made (refused, connect timeout, no free pool connection). Timeouts and
    resets after sending are not proof: the write may have landed,
    and neither is any error of a push one of whose earlier attempts may have been
    applied (``outcome_unknown``, set by the storage client's retries).
    """
    import httpx

    c: Optional[BaseException] = err
    for _ in range(4):
        if c is None:
            break
        if getattr(c, 'outcome_unknown', False) is True:
            return False
        c = c.__cause__ or c.__context__

    e: Optional[BaseException] = err
    for _ in range(4):
        if e is None:
            break
        status = _http_status_of(e)
        if status is not None:
            return status >= 400
        # Refused, or timed out before a connection / pool slot existed: nothing was sent.
        if isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, ConnectionRefusedError)):
            return True
        e = e.__cause__ or e.__context__
    return False
