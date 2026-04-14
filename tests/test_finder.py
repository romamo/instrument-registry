import datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic_market_data.models import Price, SecurityCriteria, Symbol

from commodity_registry.finder import fetch_metadata, search_isin, verify_ticker


@pytest.fixture
def mock_yahoo_source():
    with patch("commodity_registry.finder.YFinanceDataSource") as mock:
        yield mock


@pytest.fixture
def mock_ft_source():
    with patch("commodity_registry.finder.FTDataSource") as mock:
        yield mock


def test_search_isin_yahoo_only(mock_yahoo_source, mock_ft_source):
    # Setup mocks
    mock_yahoo_instance = MagicMock()
    mock_yahoo_source.return_value = mock_yahoo_instance

    # Simulate FT not installed by making it None in the finder module
    with patch("commodity_registry.finder.FTDataSource", None):
        from pydantic_market_data.models import Ticker

        mock_symbol = Symbol(
            ticker=Ticker(root="AAPL"), name="Apple Inc.", currency="USD", exchange="NASDAQ"
        )

        mock_yahoo_instance.resolve.return_value = mock_symbol

        criteria = SecurityCriteria(isin="US0378331005")
        results = search_isin(criteria)

        assert len(results) == 1
        assert results[0].provider == "yahoo"
        assert str(results[0].ticker) == "AAPL"
        assert results[0].name == "Apple Inc."


def test_fetch_metadata_failure(mock_yahoo_source):
    mock_yahoo_instance = MagicMock()
    mock_yahoo_source.return_value = mock_yahoo_instance
    mock_yahoo_instance.resolve.return_value = None

    data = fetch_metadata("INVALID", provider="yahoo")
    assert data is None


def test_verify_ticker_success(mock_yahoo_source):
    mock_yahoo_instance = MagicMock()
    mock_yahoo_source.return_value = mock_yahoo_instance
    mock_yahoo_instance.validate.return_value = True

    test_date = datetime.date(2024, 1, 1)
    test_price = Price(190.0)
    result = verify_ticker("AAPL", test_date, test_price, provider="yahoo")
    assert result is True
    from pydantic_market_data.models import Ticker

    mock_yahoo_instance.validate.assert_called_with(Ticker(root="AAPL"), test_date, test_price)


def test_resolve_currency():
    from commodity_registry.finder import resolve_currency

    # Simple pair
    res = resolve_currency("EURUSD")
    assert str(res.ticker) == "EURUSD=X"
    assert str(res.currency) == "USD"

    # With slash
    res = resolve_currency("EUR/GBP")
    assert str(res.ticker) == "EURGBP=X"
    assert str(res.currency) == "GBP"

    # Inverse USD
    res = resolve_currency("USDEUR")
    assert str(res.ticker) == "EUR=X"

    # Invalid
    assert resolve_currency("INVALID") is None
    assert resolve_currency("ABC/DEFG") is None


def test_resolve_security_registry_match():
    from pydantic_market_data.models import SecurityCriteria

    from commodity_registry.finder import resolve_security

    mock_reg = MagicMock()
    mock_comm = MagicMock()
    mock_comm.name = "GOLD"
    mock_comm.currency = "USD"
    mock_comm.tickers = MagicMock()
    mock_comm.tickers.yahoo = "GC=F"
    mock_comm.asset_class = "Commodity"
    mock_comm.instrument_type = "Future"
    mock_comm.country = None
    mock_comm.metadata = None

    mock_reg.find_candidates.return_value = [mock_comm]

    criteria = SecurityCriteria(symbol="GOLD")
    res = resolve_security(criteria, registry=mock_reg)

    assert res.name == "GOLD"
    assert str(res.ticker) == "GC=F"
    assert str(res.currency) == "USD"
    mock_reg.find_candidates.assert_called_once_with(criteria)
