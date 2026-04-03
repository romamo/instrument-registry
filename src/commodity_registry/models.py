import datetime
import logging
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

from pydantic import BaseModel, Field, field_validator
from pydantic_market_data.models import (
    ISIN,
    CurrencyCode,
    Price,
)


class InstrumentType(str, Enum):
    ETF = "ETF"
    ETC = "ETC"
    ETN = "ETN"
    STOCK = "Stock"
    INDEX = "Index"
    FUTURE = "Future"
    CRYPTO = "Crypto"
    CASH = "Cash"


class AssetClass(str, Enum):
    EQUITY_ETF = "EquityETF"
    FIXED_INCOME_ETF = "FixedIncomeETF"
    COMMODITY_ETF = "CommodityETF"
    MONEY_MARKET_ETF = "MoneyMarketETF"
    STOCK = "Stock"
    CASH = "Cash"
    CRYPTO = "Crypto"
    COMMODITY = "Commodity"


_ASSET_CLASS_MAP: list[tuple[str, AssetClass]] = []


def _get_asset_class_map() -> list[tuple[str, AssetClass]]:
    """Lazy-load the asset class mapping."""
    global _ASSET_CLASS_MAP
    if not _ASSET_CLASS_MAP:
        _ASSET_CLASS_MAP = [
            ("ETF", AssetClass.EQUITY_ETF),
            ("STOCK", AssetClass.STOCK),
            ("EQUITY", AssetClass.STOCK),
            ("CRYPTO", AssetClass.CRYPTO),
            ("FOREX", AssetClass.CASH),
            ("CASH", AssetClass.CASH),
            ("CURRENCY", AssetClass.CASH),
        ]
    return _ASSET_CLASS_MAP


def _map_asset_class(raw: str | AssetClass | None) -> AssetClass | None:
    """Maps a raw asset class string or Enum to an AssetClass enum."""
    if raw is None:
        return None
    if isinstance(raw, AssetClass):
        return raw

    upper = str(raw).upper()
    for keyword, aclass in _get_asset_class_map():
        if keyword in upper:
            return aclass
    return None


class RiskProfile(str, Enum):
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    SPECULATIVE = "Speculative"


class Liquidity(str, Enum):
    INSTANT = "Instant"
    T_PLUS_2 = "T+2"
    LOCKED = "Locked"


class Tickers(BaseModel):
    yahoo: str | None = None
    ft: str | None = Field(None, description="Financial Times ticker")
    google: str | None = None


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


class Commodity(BaseModel):
    name: str = Field(..., description="Canonical financial symbol", pattern=r"^\S+$")
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
    risk_profile: RiskProfile | None = None
    liquidity: Liquidity | None = None


class CommodityFile(BaseModel):
    commodities: list[Commodity]
