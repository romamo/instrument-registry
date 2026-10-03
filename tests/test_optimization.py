from unittest.mock import MagicMock, patch

import pytest

pd = pytest.importorskip("pandas")
py_yfinance_source = pytest.importorskip("py_yfinance.source")
YFinanceDataSource = py_yfinance_source.YFinanceDataSource


@patch("py_yfinance.source.yf.Ticker")
def test_get_price_optimization(mock_ticker_cls):
    """
    Unit test for py-yfinance optimization: ensure get_price makes minimal calls.
    """
    # Setup the mock ticker instance
    mock_ticker_instance = MagicMock()
    mock_ticker_cls.return_value = mock_ticker_instance

    # One daily bar; get_price drops bars without prices, then reads the last Close
    mock_hist = pd.DataFrame(
        {"Open": [149.0], "High": [151.0], "Low": [148.0], "Close": [150.0], "Volume": [1000]}
    )

    mock_ticker_instance.history.return_value = mock_hist

    ds = YFinanceDataSource()
    from pydantic_market_data.models import Symbol

    price = ds.get_price(Symbol(root="AAPL"))

    from pydantic_market_data.models import Price

    assert price == Price(150.0), f"Expected Price(150.0), got {price}"

    # 2. history() should be called EXACTLY once
    assert mock_ticker_instance.history.call_count == 1

    # 3. Verify arguments: interval="1d", auto_adjust=False, actions=False
    call_args = mock_ticker_instance.history.call_args
    _, kwargs = call_args

    assert kwargs.get("interval") == "1d"
    assert kwargs.get("auto_adjust") is False
    assert kwargs.get("actions") is False
    assert "start" in kwargs
    assert "end" in kwargs

    print("Optimization verified: Single history call with strict parameters.")
