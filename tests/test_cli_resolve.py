import io
import json
from unittest.mock import patch

import pytest
from pydantic_market_data.models import Currency

from instrument_registry.cli.treaty_app import app
from instrument_registry.interfaces import ProviderName, SearchResult
from instrument_registry.models import AssetClass, Instrument, InstrumentType

ENV = {"INSTRUMENT_REG_AUDIT_LOG": "off"}
APPLE = SearchResult(
    provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
)
APPLE_INSTRUMENT = Instrument(
    symbol="AAPL",
    name="Apple Inc.",
    isin="US0378331005",
    instrument_type=InstrumentType.STOCK,
    asset_class=AssetClass.STOCK,
    currency="USD",
)


def run_batch(stdin: str, *argv: str) -> tuple[int, list[dict]]:
    out = io.StringIO()
    code = app.run(
        ["resolve-batch", "--no-bundled", *argv, "--format", "json"],
        stdin=io.StringIO(stdin),
        stdout=out,
        stderr=io.StringIO(),
        env=ENV,
    )
    return code, [json.loads(line) for line in out.getvalue().splitlines()]


@patch("instrument_registry.finder.resolve_and_persist", return_value=(APPLE, None))
def test_resolve_provider_match(mock_resolve):
    env = app.call("resolve", {"query": "AAPL", "no_bundled": True}, env=ENV)

    assert env.exit_code == 0
    assert env.data["symbol"] == "AAPL"
    assert env.data["name"] == "Apple Inc."
    assert env.data["effect"] == "noop"
    assert mock_resolve.call_args.args[0].symbol.root == "AAPL"
    assert mock_resolve.call_args.args[0].isin is None


@patch("instrument_registry.finder.resolve_and_persist")
def test_resolve_new_discovery_is_created(mock_resolve):
    mock_resolve.return_value = (APPLE, APPLE_INSTRUMENT)

    env = app.call("resolve", {"query": "US0378331005", "no_bundled": True}, env=ENV)

    assert env.data["effect"] == "created"
    assert env.data["isin"] == "US0378331005"
    assert env.data["instrument_type"] == "Stock"
    assert mock_resolve.call_args.args[0].isin == "US0378331005"
    assert Instrument.model_validate(env.data).symbol == "AAPL"


@patch("instrument_registry.finder.resolve_and_persist")
def test_resolve_dry_run_reports_would_create(mock_resolve):
    mock_resolve.return_value = (APPLE, APPLE_INSTRUMENT)

    env = app.call(
        "resolve", {"query": "US0378331005", "no_bundled": True, "dry_run": True}, env=ENV
    )

    assert env.data["effect"] == "would_create"
    assert mock_resolve.call_args.kwargs["dry_run"] is True


@patch("instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO])
@patch("instrument_registry.finder.resolve_and_persist", return_value=None)
def test_resolve_not_found(mock_resolve, mock_providers):
    env = app.call("resolve", {"query": "INVALIDTICKER", "no_bundled": True}, env=ENV)

    assert env.exit_code == 5
    assert env.error.code == "NOT_FOUND"
    assert env.error.context == {"query": "INVALIDTICKER"}


@patch("instrument_registry.finder.get_available_providers", return_value=[])
@patch("instrument_registry.finder.resolve_and_persist", return_value=None)
def test_resolve_without_providers_says_how_to_install(mock_resolve, mock_providers):
    env = app.call("resolve", {"query": "AAPL", "no_bundled": True}, env=ENV)

    assert env.exit_code == 79
    assert "uv tool install" in env.error.message


@patch("instrument_registry.finder.resolve_and_persist")
def test_resolve_passes_price_on(mock_resolve):
    mock_resolve.return_value = (APPLE, None)

    app.call(
        "resolve",
        {"query": "AAPL", "no_bundled": True, "date": "2024-01-02", "price": "185.00"},
        env=ENV,
    )

    (price_on,) = mock_resolve.call_args.args[0].price_on
    assert str(price_on.date) == "2024-01-02"
    assert price_on.price.root == 185.0


def test_resolve_date_needs_price():
    env = app.call("resolve", {"query": "AAPL", "date": "2024-01-02"}, env=ENV)

    assert env.exit_code == 2


def test_resolve_ibkr_conid_from_registry(tmp_path):
    reg = tmp_path / "manual.yaml"
    reg.write_text(
        "instruments:\n"
        "  - symbol: AAPL\n"
        "    instrument_type: Stock\n"
        "    asset_class: Stock\n"
        "    currency: USD\n"
        "    tickers:\n"
        "      yahoo: AAPL\n"
        "      ibkr: 265598\n"
    )

    env = app.call(
        "resolve",
        {"query": "265598", "registry_path": [str(reg)], "no_bundled": True},
        env=ENV,
    )

    assert env.data["symbol"] == "AAPL"
    assert env.data["tickers"]["ibkr"] == 265598
    assert env.data["effect"] == "noop"


@patch("instrument_registry.finder.resolve_and_persist")
def test_resolve_saves_to_registry_path(mock_resolve, tmp_path):
    mock_resolve.return_value = (APPLE, None)

    app.call(
        "resolve", {"query": "AAPL", "registry_path": [str(tmp_path)], "no_bundled": True}, env=ENV
    )

    assert mock_resolve.call_args.kwargs["target_path"] == tmp_path


@patch("instrument_registry.finder.resolve_and_persist")
def test_resolve_batch_one_result_per_record(mock_resolve):
    cspx = SearchResult(
        provider=ProviderName.YAHOO,
        symbol="CSPX",
        name="iShares Core S&P 500",
        currency=Currency("USD"),
    )
    mock_resolve.side_effect = [(APPLE, None), (cspx, None)]

    code, lines = run_batch(
        '{"isin": "US0378331005", "symbol": "AAPL"}\n{"isin": "IE00B5BMR087", "symbol": "CSPX"}'
    )

    assert code == 0
    data = lines[-1]["data"]
    assert data["summary"] == {"total": 2, "succeeded": 2, "failed": 0}
    assert [r["name"] for r in data["results"]] == ["Apple Inc.", "iShares Core S&P 500"]


@patch("instrument_registry.finder.resolve_and_persist", return_value=(APPLE, None))
def test_resolve_batch_reads_upstream_envelope(mock_resolve):
    envelope = {"ok": True, "data": [{"isin": "US0378331005"}], "error": None}

    code, lines = run_batch(json.dumps(envelope))

    assert code == 0
    assert mock_resolve.call_args.args[0].isin == "US0378331005"


@patch("instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO])
@patch("instrument_registry.finder.resolve_and_persist")
def test_resolve_batch_reports_failed_records(mock_resolve, mock_providers):
    mock_resolve.side_effect = [(APPLE, None), None]

    code, lines = run_batch('{"symbol": "AAPL"}\n{"symbol": "NOPE"}\n{"isin": "BAD"}')

    assert code == 3
    results = lines[-1]["data"]["results"]
    assert [r["ok"] for r in results] == [True, False, False]
    assert results[1]["error"]["code"] == "NOT_FOUND"
    assert results[2]["error"]["code"] == "INVALID_RECORD"


@patch("instrument_registry.finder.resolve_and_persist", return_value=(APPLE, None))
def test_resolve_batch_price_on_list(mock_resolve):
    record = {"isin": "US0378331005", "price_on": [{"price": 185.0, "date": "2024-01-02"}]}

    run_batch(json.dumps(record))

    (price_on,) = mock_resolve.call_args.args[0].price_on
    assert str(price_on.date) == "2024-01-02"


@pytest.mark.parametrize("stdin", ['{"isin": "US0378331005"}\nnot-json', ""])
def test_resolve_batch_rejects_unreadable_input(stdin):
    code, lines = run_batch(stdin)

    assert code != 0
    assert lines[-1]["ok"] is False
