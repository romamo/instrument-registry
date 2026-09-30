import datetime
import logging
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator
from pydantic_market_data.models import (
    ISIN,
    CurrencyCode,
    Price,
)

logger = logging.getLogger(__name__)


class InstrumentType(StrEnum):
    ETF = "ETF"
    ETC = "ETC"
    ETN = "ETN"
    STOCK = "Stock"
    INDEX = "Index"
    FUTURE = "Future"
    CRYPTO = "Crypto"
    CASH = "Cash"


class AssetClass(StrEnum):
    EQUITY_ETF = "EquityETF"
    FIXED_INCOME_ETF = "FixedIncomeETF"
    COMMODITY_ETF = "CommodityETF"
    MONEY_MARKET_ETF = "MoneyMarketETF"
    STOCK = "Stock"
    CASH = "Cash"
    CRYPTO = "Crypto"
    COMMODITY = "Commodity"


_ASSET_CLASS_MAP: list[tuple[str, AssetClass]] = [
    ("ETF", AssetClass.EQUITY_ETF),
    ("FIXED_INCOME", AssetClass.FIXED_INCOME_ETF),
    ("STOCK", AssetClass.STOCK),
    ("EQUITY", AssetClass.STOCK),
    ("INDEX", AssetClass.STOCK),
    ("COMMODITY", AssetClass.COMMODITY),
    ("CRYPTO", AssetClass.CRYPTO),
    ("FOREX", AssetClass.CASH),
    ("CASH", AssetClass.CASH),
    ("CURRENCY", AssetClass.CASH),
    ("FX", AssetClass.CASH),
]


def _map_asset_class(raw: str | AssetClass | None) -> AssetClass | None:
    """Maps a raw asset class string or Enum to an AssetClass enum."""
    if raw is None:
        return None
    if isinstance(raw, AssetClass):
        return raw

    # Use the string value directly (.upper() on str subclasses like pmd.AssetClass uses the value)
    upper = raw.upper()
    for keyword, aclass in _ASSET_CLASS_MAP:
        if keyword in upper:
            return aclass
    return None


class Tickers(BaseModel):
    yahoo: str | None = None
    ft: str | None = Field(None, description="Financial Times ticker")
    google: str | None = None
    ibkr: int | None = Field(None, description="Interactive Brokers conid")


class ValidationPoint(BaseModel):
    date: datetime.date = Field(..., description="ISO 8601 date YYYY-MM-DD")
    price: Price.Input = Field(
        ...,
        description=(
            "Expected price on that date. Verification passes if this price "
            "is within the intraday High-Low range."
        ),
    )

    @field_validator("price", mode="before")
    @classmethod
    def validate_price(cls, v: Any) -> Any:
        if isinstance(v, (int, float)):
            return Price(float(v))
        return v


class Instrument(BaseModel):
    symbol: str = Field(..., description="Canonical financial symbol", pattern=r"^\S+$")
    name: str | None = None
    isin: ISIN.Input | None = None
    figi: str | None = Field(None, description="Composite FIGI identifier")

    instrument_type: InstrumentType
    asset_class: AssetClass
    currency: CurrencyCode.Input
    issuer: str | None = None
    underlying: str | None = None
    tickers: Tickers | None = None
    validation_points: list[ValidationPoint] | None = Field(
        None, description="Historical verification price points"
    )
    provider: str | None = None
    country: str | None = Field(None, description="Country or region of origin")
    metadata: dict[str, Any] | None = Field(None, description="Extra metadata")


class InstrumentFile(BaseModel):
    instruments: list[Instrument]
