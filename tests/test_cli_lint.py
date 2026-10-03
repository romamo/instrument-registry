from unittest.mock import MagicMock, patch

import pytest
from pydantic_market_data.models import Currency

from instrument_registry.cli.treaty_app import app
from instrument_registry.interfaces import ProviderName
from instrument_registry.models import (
    AssetClass,
    Instrument,
    InstrumentType,
    Tickers,
    ValidationPoint,
)
from instrument_registry.registry import save_instrument

ENV = {"INSTRUMENT_REG_AUDIT_LOG": "0"}
AAPL = Instrument(
    symbol="AAPL",
    isin="US0378331005",
    instrument_type=InstrumentType.STOCK,
    asset_class=AssetClass.STOCK,
    currency="USD",
    tickers=Tickers(yahoo="AAPL"),
    validation_points=[ValidationPoint(date="2024-01-01", price=150.0)],
)


@pytest.fixture
def reg_file(tmp_path):
    path = tmp_path / "manual.yaml"
    save_instrument(AAPL, path)
    return path


def test_lint_file(reg_file):
    env = app.call("lint", {"path": str(reg_file)}, env=ENV)

    assert env.exit_code == 0
    assert env.data["instrument_count"] == 1
    assert env.data["checked"] == ["AAPL"]
    assert env.data["error_count"] == 0
    assert env.data["verifications"] == []


def test_lint_bundled_registry_is_clean():
    env = app.call("lint", {}, env=ENV)

    assert env.exit_code == 0
    assert env.data["instrument_count"] > 0


def test_lint_duplicate_isin_fails(tmp_path):
    (tmp_path / "a.yaml").write_text(
        "instruments:\n"
        "  - {symbol: AAPL, isin: US0378331005, instrument_type: Stock,"
        " asset_class: Stock, currency: USD}\n"
    )
    (tmp_path / "b.yaml").write_text(
        "instruments:\n"
        "  - {symbol: APPLE, isin: US0378331005, instrument_type: Stock,"
        " asset_class: Stock, currency: USD}\n"
    )

    env = app.call("lint", {"path": str(tmp_path)}, env=ENV)

    assert env.exit_code == 80
    assert env.error.code == "LINT_FAILED"
    assert env.data["error_count"] == 1
    assert "Duplicate ISIN US0378331005" in env.data["errors"][0]


@patch("instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO])
@patch("instrument_registry.finder.verify_ticker", return_value=True)
@patch("instrument_registry.finder.fetch_metadata")
def test_lint_verify(mock_fetch, mock_verify, mock_providers, reg_file):
    mock_fetch.return_value = MagicMock(symbol="AAPL", currency=Currency("USD"), isin=None)

    env = app.call("lint", {"path": str(reg_file), "verify": True}, env=ENV)

    assert env.exit_code == 0
    (verification,) = env.data["verifications"]
    assert verification["status"] == "ok"
    assert verification["provider"] == "yahoo"
    assert "Ticker: AAPL [OK]" in verification["details"]
    assert any("OK: Range Match" in line for line in verification["details"])
    assert env.data["warnings"] == []


@patch("instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO])
@patch("instrument_registry.finder.verify_ticker", return_value=False)
@patch("instrument_registry.finder.fetch_metadata")
def test_lint_verify_mismatch_warns(mock_fetch, mock_verify, mock_providers, reg_file):
    mock_fetch.return_value = MagicMock(symbol="AAPL", currency=Currency("EUR"), isin=None)

    env = app.call("lint", {"path": str(reg_file), "verify": True}, env=ENV)

    assert env.exit_code == 0
    assert env.data["verifications"][0]["status"] == "failed"
    assert env.data["warnings"] == [
        "AAPL: Currency mismatch",
        "AAPL: Price verification failed on 2024-01-01",
    ]


@patch("instrument_registry.finder.get_available_providers", return_value=[])
def test_lint_verify_requires_providers(mock_providers, reg_file):
    env = app.call("lint", {"path": str(reg_file), "verify": True}, env=ENV)

    assert env.exit_code == 79
    assert "requires the yahoo provider (`py-yfinance`)" in env.error.message


def test_lint_verify_only_unknown_symbol(reg_file):
    env = app.call("lint", {"path": str(reg_file), "verify": True, "only": "NOPE"}, env=ENV)

    assert env.exit_code == 5


def test_lint_only_needs_verify(reg_file):
    env = app.call("lint", {"path": str(reg_file), "only": "AAPL"}, env=ENV)

    assert env.exit_code == 2
