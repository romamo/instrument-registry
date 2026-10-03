"""Output records shared by the commands that return instruments."""

import datetime
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Self

from pydantic_market_data.models import Price
from treaty import Out

from ..interfaces import SearchResult
from ..models import AssetClass, Instrument, InstrumentType
from ..registry import SaveEffect


class Effect(StrEnum):
    """What a mutating command did to the registry, or would do on a dry run"""

    CREATED = "created"
    UPDATED = "updated"
    NOOP = "noop"
    WOULD_CREATE = "would_create"
    WOULD_UPDATE = "would_update"
    WOULD_NOOP = "would_noop"

    @classmethod
    def of_save(cls, saved: SaveEffect | None, *, dry_run: bool) -> Effect:
        """``saved`` is None when nothing needed saving"""
        match saved, dry_run:
            case SaveEffect.CREATED, False:
                return cls.CREATED
            case SaveEffect.CREATED, True:
                return cls.WOULD_CREATE
            case SaveEffect.UPDATED, False:
                return cls.UPDATED
            case SaveEffect.UPDATED, True:
                return cls.WOULD_UPDATE
            case None, False:
                return cls.NOOP
            case None, True:
                return cls.WOULD_NOOP
        raise AssertionError(f"unhandled save effect {saved!r}")


@dataclass(frozen=True, slots=True)
class TickersRecord:
    yahoo: str | None
    ft: str | None
    google: str | None
    ibkr: int | None


@dataclass(frozen=True, slots=True)
class ValidationPointRecord:
    date: datetime.date
    price: float


@dataclass(frozen=True, slots=True, kw_only=True)
class InstrumentRecord:
    """A registry instrument; ``Instrument.model_validate`` reads it back"""

    effect: Effect
    symbol: str
    name: str | None = Out(external=True)
    isin: str | None
    figi: str | None
    instrument_type: InstrumentType | None
    asset_class: AssetClass | None
    currency: str | None
    issuer: str | None = Out(external=True)
    underlying: str | None = Out(external=True)
    tickers: TickersRecord | None
    validation_points: list[ValidationPointRecord] = Out(sort_key="date")
    provider: str | None
    country: str | None = Out(external=True)
    price: float | None = None
    price_date: datetime.date | None = None
    metadata: dict[str, object] = Out(default_factory=dict, ordered=True, external=True)

    @classmethod
    def from_instrument(cls, instrument: Instrument, effect: Effect) -> Self:
        tickers = instrument.tickers
        return cls(
            effect=effect,
            symbol=instrument.symbol,
            name=instrument.name,
            isin=str(instrument.isin) if instrument.isin else None,
            figi=instrument.figi,
            instrument_type=instrument.instrument_type,
            asset_class=instrument.asset_class,
            currency=str(instrument.currency),
            issuer=instrument.issuer,
            underlying=instrument.underlying,
            tickers=(
                TickersRecord(
                    yahoo=tickers.yahoo, ft=tickers.ft, google=tickers.google, ibkr=tickers.ibkr
                )
                if tickers
                else None
            ),
            validation_points=[
                ValidationPointRecord(date=vp.date, price=_price(vp.price))
                for vp in instrument.validation_points or []
            ],
            provider=instrument.provider,
            country=instrument.country,
            metadata=instrument.metadata or {},
        )

    @classmethod
    def from_search_result(cls, res: SearchResult, effect: Effect) -> Self:
        """A provider match that is not a registry record, such as a currency"""
        return cls(
            effect=effect,
            symbol=str(res.symbol),
            name=res.name,
            isin=res.isin,
            figi=res.figi,
            instrument_type=res.instrument_type,
            asset_class=res.asset_class,
            currency=str(res.currency) if res.currency is not None else None,
            issuer=None,
            underlying=None,
            tickers=None,
            validation_points=[],
            provider=res.provider.value if res.provider else None,
            country=res.country,
            price=_price(res.price) if res.price is not None else None,
            price_date=res.price_date,
            metadata=res.metadata or {},
        )

    def with_price(self, res: SearchResult) -> Self:
        """The price ``res`` carries, as fetched by --report-price"""
        if res.price is None:
            return self
        return replace(self, price=_price(res.price), price_date=res.price_date)


def _price(value: Price | float) -> float:
    return value.root if isinstance(value, Price) else float(value)
