from unittest.mock import MagicMock, patch

import pytest
from pydantic_market_data.models import Currency

from instrument_registry.cli import common as cli_common
from instrument_registry.cli import main
from instrument_registry.interfaces import ProviderName, SearchResult


@pytest.fixture
def mock_registry(tmp_path):
    reg_dir = tmp_path / "reg"
    reg_dir.mkdir()
    manual_file = reg_dir / "manual.yaml"
    with open(manual_file, "w") as f:
        f.write("instruments: []")
    return reg_dir


def test_cli_verbosity_debug():
    # Test only the root flag propagation logic
    with patch("instrument_registry.cli.common.setup_logging") as mock_log:
        with patch("instrument_registry.finder.resolve_and_persist", return_value=None):
            with pytest.raises(SystemExit):
                main(["-vv", "resolve", "AAPL"])
        mock_log.assert_called_once_with(2)


@patch("instrument_registry.cli.common.get_registry")
def test_cli_vv_preserves_debug_state(mock_get_reg, capsys):
    mock_reg = MagicMock()
    mock_reg.get_all.return_value = []
    mock_reg.load_errors = []
    mock_get_reg.return_value = mock_reg

    main(["-vv", "lint", "--format", "table"])
    assert cli_common.STATE.debug is True


def test_main_dispatches_root_app():
    with patch("instrument_registry.cli.app") as mock_app:
        main(["lint", "-vv"])
    mock_app.assert_called_once_with(args=["lint", "-vv"])


@patch("instrument_registry.cli.common.get_registry")
@patch("instrument_registry.cli.common.setup_logging")
def test_cli_registry_path_before_subcommand_is_not_supported(mock_log, mock_get_reg, tmp_path):
    target = tmp_path / "registry"
    target.mkdir()

    mock_reg = MagicMock()
    mock_reg.find_by_isin.return_value = None
    mock_get_reg.return_value = mock_reg

    with patch("instrument_registry.registry.add_instrument") as mock_add:
        with pytest.raises(SystemExit) as exc:
            main(
                [
                    "--registry-path",
                    str(target),
                    "--no-bundled",
                    "add",
                    "--isin",
                    "US0378331005",
                    "--currency",
                    "USD",
                    "--symbol",
                    "AAPL",
                    "--instrument-type",
                    "Stock",
                    "--asset-class",
                    "Stock",
                    "--format",
                    "table",
                ]
            )

    assert exc.value.code == 3  # distinct from general failure (1): callers can spot bad input
    assert mock_add.call_count == 0


@patch("instrument_registry.cli.common.get_registry")
@patch("instrument_registry.cli.common.setup_logging")
def test_cli_registry_path_after_subcommand_reaches_write_target(mock_log, mock_get_reg, tmp_path):
    target = tmp_path / "registry"
    target.mkdir()

    mock_reg = MagicMock()
    mock_reg.find_by_isin.return_value = None
    mock_get_reg.return_value = mock_reg

    with patch("instrument_registry.registry.add_instrument") as mock_add:
        mock_add.return_value = type("InstrumentResult", (), {"symbol": "AAPL"})()
        main(
            [
                "add",
                "--registry-path",
                str(target),
                "--no-bundled",
                "--isin",
                "US0378331005",
                "--currency",
                "USD",
                "--symbol",
                "AAPL",
                "--instrument-type",
                "Stock",
                "--asset-class",
                "Stock",
                "--format",
                "table",
            ]
        )

    assert mock_add.call_args.kwargs["target_path"] == target / "manual.yaml"


@patch("instrument_registry.cli.common.get_registry")
@patch("instrument_registry.cli.common.setup_logging")
def test_cli_lint_empty_registry(mock_log, mock_get_reg, capsys):
    mock_reg = MagicMock()
    mock_reg.get_all.return_value = []
    mock_reg.load_errors = []
    mock_get_reg.return_value = mock_reg

    main(["lint", "--format", "json"])
    captured = capsys.readouterr()
    assert '"instrument_count": 0' in captured.out
    assert '"error_count": 0' in captured.out


@patch("instrument_registry.cli.common.get_registry")
@patch("instrument_registry.cli.common.setup_logging")
def test_cli_lint_debug_lists_each_symbol(mock_log, mock_get_reg, capsys):
    first = MagicMock()
    first.symbol = "AAPL"
    first.isin = "US0378331005"
    first.currency = "USD"
    second = MagicMock()
    second.symbol = "CSPX"
    second.isin = "IE00B5BMR087"
    second.currency = "USD"

    mock_reg = MagicMock()
    mock_reg.get_all.return_value = [first, second]
    mock_reg.load_errors = []
    mock_get_reg.return_value = mock_reg

    main(["-vv", "lint", "--format", "table"])

    captured = capsys.readouterr()
    assert "AAPL: OK" in captured.out
    assert "CSPX: OK" in captured.out


@patch("instrument_registry.cli.common.get_registry")
@patch("instrument_registry.cli.common.setup_logging")
def test_cli_lint_json_output(mock_log, mock_get_reg, capsys):
    mock_instrument = MagicMock()
    mock_instrument.symbol = "AAPL"
    mock_instrument.isin = "US0378331005"
    mock_instrument.currency = "USD"

    mock_reg = MagicMock()
    mock_reg.get_all.return_value = [mock_instrument]
    mock_reg.load_errors = []
    mock_get_reg.return_value = mock_reg

    main(["lint", "--format", "json"])

    captured = capsys.readouterr()
    assert '"instrument_count": 1' in captured.out
    assert '"error_count": 0' in captured.out
    assert '"checked": [' in captured.out
    assert '"AAPL"' in captured.out


@patch("instrument_registry.cli.common.get_registry")
@patch("instrument_registry.cli.common.setup_logging")
def test_cli_lint_debug_json_streams_events(mock_log, mock_get_reg, capsys):
    first = MagicMock()
    first.symbol = "AAPL"
    first.isin = "US0378331005"
    first.currency = "USD"
    second = MagicMock()
    second.symbol = "CSPX"
    second.isin = "IE00B5BMR087"
    second.currency = "USD"

    mock_reg = MagicMock()
    mock_reg.get_all.return_value = [first, second]
    mock_reg.load_errors = []
    mock_get_reg.return_value = mock_reg

    main(["-vv", "lint", "--format", "json"])

    captured = capsys.readouterr()
    assert '{"event": "instrument_checked", "symbol": "AAPL", "status": "OK"}' in captured.out
    assert '{"event": "instrument_checked", "symbol": "CSPX", "status": "OK"}' in captured.out
    assert '"instrument_count": 2' in captured.out


@patch("instrument_registry.finder.search_isin")
@patch("instrument_registry.registry.add_instrument")
def test_cli_add_success(mock_add, mock_search, mock_registry, capsys):
    mock_search.return_value = [
        SearchResult(
            provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
        )
    ]

    mock_comm = MagicMock()
    mock_comm.symbol = "AAPL"
    mock_comm.model_dump.return_value = {"symbol": "AAPL"}
    mock_add.return_value = mock_comm

    args = [
        "instrument-reg",
        "add",
        "AAPL",
        "--instrument-type",
        "ETF",
        "--asset-class",
        "CommodityETF",
        "--currency",
        "USD",
        "--registry-path",
        str(mock_registry),
        "--fetch",
    ]

    main([*args[1:], "--format", "json"])

    mock_add.assert_called_once()
    assert "AAPL" in capsys.readouterr().out


@patch("instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO])
@patch("instrument_registry.finder.resolve_security")
def test_cli_fetch_success(mock_resolve, mock_get_available_providers, capsys):
    mock_resolve.return_value = SearchResult(
        provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
    )

    main(["fetch", "--symbol", "AAPL", "--format", "json"])

    captured = capsys.readouterr()
    assert "AAPL" in captured.out
    assert "Apple Inc." in captured.out
    assert mock_resolve.call_args.kwargs["registry"] is not None


def test_cli_help(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "COMMAND" in captured.err


@patch("instrument_registry.finder.search_isin")
@patch("instrument_registry.registry.add_instrument")
def test_cli_add_no_metadata(mock_add, mock_search, mock_registry):
    """Test add command when no online metadata is found and no identifier is provided."""
    mock_search.return_value = []

    # Missing both ticker and isin, and fetch is on
    args = [
        "instrument-reg",
        "add",
        "--instrument-type",
        "ETF",
        "--asset-class",
        "CommodityETF",
        "--currency",
        "USD",
        "--registry-path",
        str(mock_registry),
        "--fetch",
    ]

    with pytest.raises(SystemExit) as e:
        main(args[1:])
    assert e.value.code == 1


def test_cli_add_requires_explicit_write_target(capsys, monkeypatch):
    monkeypatch.delenv("INSTRUMENT_REGISTRY_PATH", raising=False)

    args = [
        "instrument-reg",
        "add",
        "AAPL",
        "--instrument-type",
        "ETF",
        "--asset-class",
        "CommodityETF",
        "--currency",
        "USD",
    ]

    with pytest.raises(SystemExit) as exc:
        main(args[1:])

    assert exc.value.code == 1
    assert (
        "No registry write path configured. Set INSTRUMENT_REGISTRY_PATH or pass --registry-path."
        in capsys.readouterr().err
    )


@patch("instrument_registry.finder.search_isin")
@patch("instrument_registry.registry.add_instrument")
def test_cli_add_uses_env_write_target(mock_add, mock_search, mock_registry, monkeypatch, capsys):
    mock_search.return_value = []
    mock_comm = MagicMock()
    mock_comm.symbol = "AAPL"
    mock_comm.model_dump.return_value = {"symbol": "AAPL"}
    mock_add.return_value = mock_comm
    monkeypatch.setenv("INSTRUMENT_REGISTRY_PATH", str(mock_registry))

    args = [
        "instrument-reg",
        "add",
        "AAPL",
        "--instrument-type",
        "ETF",
        "--asset-class",
        "CommodityETF",
        "--currency",
        "USD",
    ]

    main([*args[1:], "--format", "json"])

    mock_add.assert_called_once()
    assert str(mock_add.call_args.kwargs["target_path"]).endswith("manual.yaml")
    assert "AAPL" in capsys.readouterr().out


@patch("instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO])
@patch("instrument_registry.finder.verify_ticker")
@patch("instrument_registry.finder.fetch_metadata")
@patch("instrument_registry.finder.resolve_security")
def test_cli_lint_with_verify(
    mock_resolve,
    mock_fetch,
    mock_verify,
    mock_get_available_providers,
    mock_registry,
    capsys,
):
    """Test lint command with verification enabled and detailed audit path."""
    from instrument_registry.models import (
        AssetClass,
        Instrument,
        InstrumentType,
        Tickers,
        ValidationPoint,
    )
    from instrument_registry.registry import _save_instrument_to_file

    mock_fetch.return_value = MagicMock(
        symbol="AAPL", name="Apple Inc.", currency=Currency("USD"), isin="US0378331005"
    )
    mock_verify.return_value = True

    # Create a small registry with ticker and validation points
    reg_file = mock_registry / "manual.yaml"
    comm = Instrument(
        symbol="AAPL",
        isin="US0378331005",
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="USD",
        tickers=Tickers(yahoo="AAPL"),
        validation_points=[ValidationPoint(date="2024-01-01", price=150.0)],
    )
    _save_instrument_to_file(comm, reg_file)
    _save_instrument_to_file(comm, reg_file)

    args = ["instrument-reg", "-vv", "lint", "--path", str(reg_file), "--verify"]

    main([*args[1:], "--format", "table"])

    output = capsys.readouterr().out
    assert "AAPL(yahoo AAPL): OK" in output
    assert "ISIN:     US0378331005 [OK]" in output
    assert "OK: Range Match" in output


@patch("instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO])
def test_cli_fetch_no_results(mock_get_available_providers, capsys):
    """Test fetch command when no results are found."""
    with patch("instrument_registry.finder.resolve_security", return_value=None):
        main(["fetch", "--symbol", "NONEXISTENT", "--format", "table"])

    # Just ensure it doesn't crash


@patch("instrument_registry.finder.get_available_providers", return_value=[])
def test_cli_fetch_requires_providers(mock_get_available_providers, capsys):
    with pytest.raises(SystemExit) as exc:
        main(["fetch", "--symbol", "AAPL"])

    assert exc.value.code == 1
    assert "requires the yahoo provider (`py-yfinance`)" in capsys.readouterr().err


@patch("instrument_registry.finder.get_available_providers", return_value=[])
def test_cli_lint_verify_requires_providers(mock_get_available_providers, mock_registry, capsys):
    reg_file = mock_registry / "manual.yaml"
    reg_file.write_text(
        "instruments:\n"
        "  - symbol: AAPL\n"
        "    isin: US0378331005\n"
        "    instrument_type: Stock\n"
        "    asset_class: Stock\n"
        "    currency: USD\n"
        "    tickers:\n"
        "      yahoo: AAPL\n"
    )

    with pytest.raises(SystemExit) as exc:
        main(["lint", "--path", str(reg_file), "--verify"])

    assert exc.value.code == 1
    assert "requires the yahoo provider (`py-yfinance`)" in capsys.readouterr().err


@patch("instrument_registry.finder.get_available_providers", return_value=[ProviderName.YAHOO])
@patch("instrument_registry.finder.fetch_price")
@patch("instrument_registry.finder.resolve_security")
def test_cli_fetch_with_price(mock_resolve, mock_price, mock_get_available_providers, capsys):
    """Test fetch command with price fetching enabled."""
    from pydantic_market_data.models import Price

    mock_resolve.return_value = SearchResult(
        provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
    )
    mock_price.return_value = Price(150.0)

    args = ["instrument-reg", "fetch", "--symbol", "AAPL", "--price"]

    main([*args[1:], "--format", "json"])

    output = capsys.readouterr().out
    assert "AAPL" in output
    assert "150" in output


@patch("instrument_registry.finder.resolve_and_persist")
def test_cli_resolve_json(mock_resolve, mock_registry, capsys):
    """Test resolve command with JSON output."""
    mock_resolve.return_value = (
        SearchResult(
            provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
        ),
        None,
    )

    args = ["instrument-reg", "resolve", "--format", "json", "AAPL"]

    main(args[1:])

    output = capsys.readouterr().out
    assert '"symbol": "AAPL"' in output
