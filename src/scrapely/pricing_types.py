"""
Pricing types for Scrapely Actors.

This module defines the pricing models and related types for Actors
on the Scrapely platform.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class PricingModel(str, Enum):
    """Pricing model types for Actors."""
    FREE = 'FREE'
    PRICE_PER_DATASET_ITEM = 'PRICE_PER_DATASET_ITEM'
    PAY_PER_EVENT = 'PAY_PER_EVENT'


class BasePricingInfo(BaseModel):
    """Base class for pricing information."""
    model_config = ConfigDict(populate_by_name=True)
    
    created_at: Optional[datetime] = Field(
        default=None,
        alias='createdAt',
        description='Timestamp when this pricing info was created.'
    )
    started_at: Optional[datetime] = Field(
        default=None,
        alias='startedAt',
        description='Timestamp when this pricing became effective.'
    )


class FreeActorPricingInfo(BasePricingInfo):
    """Pricing info for an Actor offered free of charge."""
    pass


class FlatPricePerMonthActorPricingInfo(BasePricingInfo):
    """Pricing info for an Actor billed at a flat monthly price."""
    price_per_unit_usd: Optional[float] = Field(
        default=None,
        alias='pricePerUnitUsd',
        description='Price per unit, in USD.'
    )
    trial_minutes: Optional[int] = Field(
        default=None,
        alias='trialMinutes',
        description='Length of the free trial period, in minutes.'
    )


class PricePerDatasetItemActorPricingInfo(BasePricingInfo):
    """Pricing info for an Actor billed per dataset item produced."""
    unit_name: Optional[str] = Field(
        default=None,
        alias='unitName',
        description='Name of the billed unit.'
    )


class PricingPerEvent(BaseModel):
    """Pricing details for a single event type in pay-per-event model."""
    model_config = ConfigDict(populate_by_name=True)
    
    event_price_usd: Decimal = Field(
        alias='eventPriceUsd',
        description='Price per event in USD.'
    )
    event_description: Optional[str] = Field(
        default=None,
        alias='eventDescription',
        description='Human-readable description of the event.'
    )


class PayPerEventActorPricingInfo(BasePricingInfo):
    """Pricing info for an Actor billed per charged event."""
    pricing_per_event: Dict[str, PricingPerEvent] = Field(
        default_factory=dict,
        alias='pricingPerEvent',
        description='The pay-per-event pricing details.'
    )


# Union type for all pricing info types
ActorPricingInfo = Union[
    FreeActorPricingInfo,
    FlatPricePerMonthActorPricingInfo,
    PricePerDatasetItemActorPricingInfo,
    PayPerEventActorPricingInfo,
]


class ActorChargeEvent(BaseModel):
    """Definition of a single chargeable event in the pay-per-event pricing model."""
    model_config = ConfigDict(populate_by_name=True)
    
    event_description: Optional[str] = Field(
        default=None,
        alias='eventDescription',
        description='Human-readable description of the event.'
    )


class ChargeResult(BaseModel):
    """Result of the ChargingManager.charge method."""
    model_config = ConfigDict(populate_by_name=True)
    
    chargeable_within_limit: Dict[str, Optional[int]] = Field(
        alias='chargeableWithinLimit',
        description='How many events of each known type can still be charged within the limit.'
    )
    charged_count: int = Field(
        alias='chargedCount',
        description='Total amount of charged events - may be lower than the requested amount.'
    )
    event_charge_limit_reached: bool = Field(
        alias='eventChargeLimitReached',
        description='If true, no more events of this type can be charged within the limit.'
    )


class ActorPricingInfoResult(BaseModel):
    """Result of the ChargingManager.get_pricing_info method."""
    model_config = ConfigDict(populate_by_name=True, allow_inf_nan=True)

    is_pay_per_event: bool = Field(
        alias='isPayPerEvent',
        description='A shortcut - true if the Actor runs with the pay-per-event pricing model.'
    )
    max_total_charge_usd: Union[Decimal, float] = Field(
        alias='maxTotalChargeUsd',
        description='A configured limit for the total charged amount (inf when uncapped).'
    )
    per_event_prices: Dict[str, Decimal] = Field(
        default_factory=dict,
        alias='perEventPrices',
        description='Price of every known event type.'
    )
    pricing_model: Optional[PricingModel] = Field(
        default=None,
        alias='pricingModel',
        description='The currently effective pricing model.'
    )


__all__ = [
    'PricingModel',
    'BasePricingInfo',
    'FreeActorPricingInfo',
    'FlatPricePerMonthActorPricingInfo',
    'PricePerDatasetItemActorPricingInfo',
    'PayPerEventActorPricingInfo',
    'PricingPerEvent',
    'ActorPricingInfo',
    'ActorChargeEvent',
    'ChargeResult',
    'ActorPricingInfoResult',
]
