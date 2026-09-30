"""
Interface definitions for instrument registry.

Defines Protocols for type safety and Pydantic models for typed returns.
"""

from datetime import date
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel
from pydantic_market_data.models import (
    Currency,
    CurrencyCode,
    Price,
    Security,
    SecurityQuery,
    Symbol,
)

from .models import AssetClass, Instrument, InstrumentType


class ProviderName(StrEnum):
    YAHOO = "yahoo"
    FT = "ft"
    GOOGLE = "google"


class DataProvider(Protocol):
    """Protocol for market data providers (Yahoo, FT, etc.)"""

    def resolve(self, criteria: SecurityQuery) -> Security | None:
        """Resolve security using search criteria"""
        ...

    def validate(self, symbol: Symbol.Input, date: date, price: Price.Input) -> bool:
        """Verify price against historical data"""
        ...

    def get_price(self, symbol: Symbol.Input, date: date | None = None) -> float | None:
        """Fetch the price for a ticker (current or historical)"""
        ...


class InstrumentLookup(Protocol):
    """Protocol for instrument lookup operations"""

    def find_by_isin(self, isin: str, currency: Currency | None = None) -> Instrument | None: ...

    def find_candidates(self, criteria: SecurityQuery) -> list[Instrument]: ...

    def find_by_ticker(self, provider: str, symbol: Symbol.Input) -> Instrument | None: ...


class SearchResult(BaseModel):
    """Result from multi-provider search"""

    provider: ProviderName | None = None
    symbol: Symbol.Input
    name: str
    currency: CurrencyCode.Input | None = None
    asset_class: AssetClass | None = None
    instrument_type: InstrumentType | None = None
    price: Price.Input | None = None
    price_date: date | None = None
    country: str | None = None
    isin: str | None = None
    figi: str | None = None
    ticker: str | None = None
    metadata: dict[str, Any] | None = None
