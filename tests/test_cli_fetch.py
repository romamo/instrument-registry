import json
import os
from unittest.mock import patch

import pytest
from pydantic_market_data.models import Currency, Price

from instrument_registry.cli import main
from instrument_registry.cli.treaty_app import app
from instrument_registry.interfaces import ProviderName, SearchResult

ENV = {"INSTRUMENT_REG_AUDIT_LOG": "off"}
APPLE = SearchResult(
    provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
)


@pytest.fixture
def providers():
    with patch(
        "instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO]
    ) as mock:
        yield mock


def test_fetch_success(providers):
    with patch("instrument_registry.finder.resolve_security", return_value=APPLE) as resolve:
        env = app.call("fetch", {"symbol": "AAPL"}, env=ENV)

    assert env.exit_code == 0
    assert env.data["symbol"] == "AAPL"
    assert env.data["name"] == "Apple Inc."
    assert env.data["currency"] == "USD"
    assert env.data["provider"] == "yahoo"
    assert env.data["metadata"] == {}
    assert resolve.call_args.kwargs["registry"] is not None


def test_fetch_with_price(providers):
    with (
        patch("instrument_registry.finder.resolve_security", return_value=APPLE.model_copy()),
        patch("instrument_registry.finder.fetch_price", return_value=Price(150.0)),
    ):
        env = app.call("fetch", {"symbol": "AAPL", "price": True}, env=ENV)

    assert env.exit_code == 0
    assert env.data["price"] == 150.0


def test_fetch_price_unavailable_warns(providers):
    with (
        patch("instrument_registry.finder.resolve_security", return_value=APPLE.model_copy()),
        patch("instrument_registry.finder.fetch_price", return_value=None),
    ):
        env = app.call("fetch", {"symbol": "AAPL", "price": True}, env=ENV)

    assert env.exit_code == 0
    assert env.data["price"] is None
    assert "PRICE_UNAVAILABLE" in [w.code for w in env.warnings]


def test_fetch_price_without_provider_warns(providers):
    no_provider = APPLE.model_copy(update={"provider": None})
    with (
        patch("instrument_registry.finder.resolve_security", return_value=no_provider),
        patch("instrument_registry.finder.fetch_price") as fetch_price,
    ):
        env = app.call("fetch", {"symbol": "AAPL", "price": True}, env=ENV)

    assert env.exit_code == 0
    assert "PRICE_UNAVAILABLE" in [w.code for w in env.warnings]
    fetch_price.assert_not_called()


def test_fetch_no_results(providers):
    with patch("instrument_registry.finder.resolve_security", return_value=None):
        env = app.call("fetch", {"symbol": "NONEXISTENT"}, env=ENV)

    assert env.error.code == "NOT_FOUND"
    assert env.error.context == {"isin": None, "figi": None, "symbol": "NONEXISTENT"}


def test_fetch_requires_providers():
    with patch("instrument_registry.finder.get_available_providers", return_value=[]):
        env = app.call("fetch", {"symbol": "AAPL"}, env=ENV)

    assert env.exit_code == 79
    assert env.error.code == "MISSING_PROVIDER"
    assert "requires the yahoo provider (`py-yfinance`)" in env.error.message


def test_fetch_figi_only_hints_ft_provider():
    with patch("instrument_registry.finder.get_available_providers", return_value=[]):
        env = app.call("fetch", {"figi": "BBG000B9XRY4"}, env=ENV)

    assert env.error.context == {"provider": "ft"}


def test_fetch_requires_an_identifier():
    env = app.call("fetch", {}, env=ENV)

    assert env.exit_code == 2
    assert env.error.code == "ARG_ERROR"


def test_fetch_rejects_malformed_isin():
    env = app.call("fetch", {"isin": "NOT-AN-ISIN"}, env=ENV)

    assert env.error.code == "ARG_ERROR"


def test_main_routes_fetch_to_treaty(providers, capsys):
    with (
        patch.dict(os.environ, ENV),
        patch("instrument_registry.finder.resolve_security", return_value=APPLE),
        pytest.raises(SystemExit) as exc,
    ):
        main(["fetch", "--symbol", "AAPL"])

    assert exc.value.code == 0
    assert json.loads(capsys.readouterr().out)["data"]["symbol"] == "AAPL"


def test_main_routes_manifest_to_treaty(capsys):
    with patch.dict(os.environ, ENV), pytest.raises(SystemExit) as exc:
        main(["manifest"])

    assert exc.value.code == 0
    assert "fetch" in json.loads(capsys.readouterr().out)["data"]["commands"]
