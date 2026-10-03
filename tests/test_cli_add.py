from unittest.mock import patch

import yaml
from pydantic_market_data.models import Currency

from instrument_registry.cli.treaty_app import app
from instrument_registry.interfaces import ProviderName, SearchResult

ENV = {"INSTRUMENT_REG_AUDIT_LOG": "0"}
STOCK = {"instrument_type": "Stock", "asset_class": "Stock", "currency": "USD"}


def add(registry, **arguments):
    return app.call(
        "add",
        {"registry_path": [str(registry)], "no_bundled": True, **arguments},
        env=ENV,
    )


def saved(path):
    return yaml.safe_load(path.read_text())["instruments"]


def test_add_creates_then_updates(tmp_path):
    first = add(tmp_path, query="US0378331005", symbol="AAPL", **STOCK)
    second = add(tmp_path, query="US0378331005", symbol="AAPL", **STOCK)

    assert first.exit_code == 0
    assert first.data["effect"] == "created"
    assert second.data["effect"] == "updated"
    (record,) = saved(tmp_path / "manual.yaml")
    assert record["isin"] == "US0378331005"
    assert record["tickers"] == {"yahoo": "AAPL"}


def test_add_dry_run_writes_nothing(tmp_path):
    env = add(tmp_path, query="AAPL", dry_run=True, **STOCK)

    assert env.data["effect"] == "would_create"
    assert env.data["symbol"] == "AAPL"
    assert not (tmp_path / "manual.yaml").exists()


def test_add_validation_point(tmp_path):
    env = add(
        tmp_path, query="AAPL", validation_date="2024-01-02", validation_price="185.5", **STOCK
    )

    assert env.data["validation_points"] == [{"date": "2024-01-02", "price": 185.5}]
    assert saved(tmp_path / "manual.yaml")[0]["validation_points"][0]["price"] == 185.5


def test_add_requires_write_target():
    env = app.call("add", {"query": "AAPL", **STOCK}, env=ENV)

    assert env.exit_code == 4
    assert env.error.message == "No registry write path configured."
    assert "INSTRUMENT_REGISTRY_PATH" in env.error.suggestion


def test_add_uses_env_write_target(tmp_path):
    env = app.call(
        "add",
        {"query": "AAPL", "no_bundled": True, **STOCK},
        env={**ENV, "INSTRUMENT_REGISTRY_PATH": str(tmp_path)},
    )

    assert env.exit_code == 0
    assert saved(tmp_path / "manual.yaml")[0]["symbol"] == "AAPL"


def test_add_without_fetch_needs_every_detail(tmp_path):
    env = add(tmp_path, query="AAPL", currency="USD")

    assert env.exit_code == 2
    assert "--instrument-type" in env.error.message


def test_add_needs_an_identifier(tmp_path):
    env = add(tmp_path, fetch=True, **STOCK)

    assert env.exit_code == 2


def test_add_registry_path_before_command_is_arg_error(tmp_path, capsys):
    code = app.run(
        ["--registry-path", str(tmp_path), "add", "AAPL", "--currency", "USD"],
        env=ENV,
    )

    assert code == 2
    assert not (tmp_path / "manual.yaml").exists()


@patch("instrument_registry.finder.search_isin")
def test_add_fetch_fills_details(mock_search, tmp_path):
    mock_search.return_value = [
        SearchResult(
            provider=ProviderName.YAHOO,
            symbol="AAPL",
            name="Apple Inc.",
            currency=Currency("USD"),
            asset_class="Stock",
            instrument_type="Stock",
        )
    ]

    env = add(tmp_path, query="US0378331005", fetch=True)

    assert env.exit_code == 0
    assert env.data["name"] == "Apple Inc."
    assert env.data["currency"] == "USD"
    assert env.data["tickers"]["yahoo"] == "AAPL"


@patch("instrument_registry.finder.search_isin", return_value=[])
def test_add_fetch_without_match_warns(mock_search, tmp_path):
    env = add(tmp_path, query="AAPL", fetch=True, **STOCK)

    assert env.exit_code == 0
    assert "METADATA_NOT_FOUND" in [w.code for w in env.warnings]


def test_add_symbol_collision_is_conflict(tmp_path):
    add(tmp_path, query="GOLD", **STOCK)

    env = add(
        tmp_path,
        query="GOLD",
        instrument_type="ETC",
        asset_class="Commodity",
        currency="USD",
        registry_path=[str(tmp_path / "manual.yaml")],
    )

    assert env.exit_code == 6
    assert env.error.code == "CONFLICT"


def test_add_preserves_existing_symbol(tmp_path):
    add(tmp_path, isin="US0378331005", canonical="CUSTOM_NAME", symbol="AAPL", **STOCK)

    env = add(
        tmp_path,
        isin="US0378331005",
        symbol="AAPL",
        registry_path=[str(tmp_path / "manual.yaml")],
        **STOCK,
    )

    assert env.data["symbol"] == "CUSTOM_NAME"
    assert env.data["effect"] == "updated"
