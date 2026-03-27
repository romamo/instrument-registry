import datetime

import pytest
from pydantic import ValidationError
from pydantic_market_data.models import Price

from commodity_registry.models import (
    AssetClass,
    Commodity,
    InstrumentType,
    Tickers,
    ValidationPoint,
)


def test_validation_point_valid():
    vp = ValidationPoint(date="2024-01-01", price=Price(100.5))
    assert vp.date == datetime.date(2024, 1, 1)
    assert vp.price.value == 100.5


def test_tickers_valid():
    tickers = Tickers(yahoo="AAPL", ft="AAPL:NSQ")
    assert tickers.yahoo == "AAPL"
    assert tickers.ft == "AAPL:NSQ"


def test_commodity_minimal():
    c = Commodity(
        name="TEST_ETF",
        asset_class=AssetClass.EQUITY_ETF,
        instrument_type=InstrumentType.ETF,
        currency="USD",
    )
    assert c.name == "TEST_ETF"
    assert str(c.currency) == "USD"
    assert c.asset_class == AssetClass.EQUITY_ETF


def test_commodity_full():
    c = Commodity(
        name="TEST",
        isin="US0378331005",  # Valid AAPL ISIN
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="USD",
        tickers={"yahoo": "TEST"},
        validation_points=[{"date": "2024-01-01", "price": 100.0}],
    )
    assert str(c.isin) == "US0378331005"
    assert c.tickers.yahoo == "TEST"
    assert len(c.validation_points) == 1


def test_isin_validation():
    # Valid ISIN (Apple)
    Commodity(
        name="AAPL",
        isin="US0378331005",
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="USD",
    )

    # Invalid Length
    with pytest.raises(ValidationError, match="Invalid ISIN format"):
        Commodity(
            name="BAD",
            isin="US123",
            instrument_type=InstrumentType.STOCK,
            asset_class=AssetClass.STOCK,
            currency="USD",
        )

    # Invalid Country Code
    with pytest.raises(ValidationError, match="Invalid ISIN format"):
        Commodity(
            name="BAD",
            isin="120378331005",
            instrument_type=InstrumentType.STOCK,
            asset_class=AssetClass.STOCK,
            currency="USD",
        )

    # Invalid Checksum (last digit changed from 5 to 6)
    with pytest.raises(ValidationError, match="Invalid ISIN"):
        Commodity(
            name="BAD",
            isin="US0378331006",
            instrument_type=InstrumentType.STOCK,
            asset_class=AssetClass.STOCK,
            currency="USD",
        )


def test_commodity_invalid_currency():
    with pytest.raises(ValidationError):
        Commodity(
            name="Test",
            asset_class=AssetClass.EQUITY_ETF,
            instrument_type=InstrumentType.ETF,
            currency="INVALID",  # Should be 3-4 chars
        )


def test_enums():
    assert AssetClass.EQUITY_ETF == "EquityETF"
    assert InstrumentType.ETF == "ETF"

    with pytest.raises(ValueError):
        InstrumentType("InvalidType")


def test_commodity_name_relaxed():
    # Test symbols that were previously invalid under Beancount rules
    valid_names = ["^GSPC", "EURUSD=X", "BTC-USD", "4GLD.DE", "ETH:USD", "123"]
    for name in valid_names:
        c = Commodity(
            name=name,
            asset_class=AssetClass.STOCK,
            instrument_type=InstrumentType.STOCK,
            currency="USD",
        )
        assert c.name == name

    # Test invalid names (containing whitespace)
    invalid_names = ["AAPL ", " AAPL", "AAPL Ticker"]
    for name in invalid_names:
        with pytest.raises(ValidationError):
            Commodity(
                name=name,
                asset_class=AssetClass.STOCK,
                instrument_type=InstrumentType.STOCK,
                currency="USD",
            )
