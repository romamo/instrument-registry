from unittest.mock import patch

import pytest

from instrument_registry.cli import main


@patch("instrument_registry.finder.resolve_and_persist")
def test_cli_resolve_success(mock_resolve, capsys):
    from pydantic_market_data.models import Currency

    from instrument_registry.interfaces import ProviderName, SearchResult

    mock_resolve.return_value = SearchResult(
        provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
    )

    try:
        main(["resolve", "AAPL", "--format", "table"])
    except SystemExit as e:
        assert getattr(e, "code", 0) == 0

    captured = capsys.readouterr()
    assert "Resolved: Apple Inc. -> AAPL (yahoo)" in captured.out


@patch("instrument_registry.finder.resolve_and_persist")
def test_cli_resolve_not_found(mock_resolve):
    mock_resolve.return_value = None

    with pytest.raises(SystemExit) as exc:
        main(["resolve", "INVALIDTICKER"])
    assert exc.value.code == 1


@patch("instrument_registry.finder.resolve_and_persist")
def test_cli_resolve_single_json_pipe(mock_resolve, capsys):
    from pydantic_market_data.models import Currency

    from instrument_registry.interfaces import ProviderName, SearchResult

    mock_resolve.return_value = SearchResult(
        provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
    )

    with (
        patch("sys.stdin.isatty", return_value=False),
        patch("sys.stdin.read", return_value='{"isin": "US0378331005", "symbol": "AAPL"}'),
    ):
        main(["resolve", "--format", "table"])

    assert "Resolved: Apple Inc. -> AAPL (yahoo)" in capsys.readouterr().out
    assert mock_resolve.call_count == 1


@patch("instrument_registry.finder.resolve_and_persist")
def test_cli_resolve_jsonl_pipe_multiple_records(mock_resolve, capsys):
    from pydantic_market_data.models import Currency

    from instrument_registry.interfaces import ProviderName, SearchResult

    mock_resolve.side_effect = [
        SearchResult(
            provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
        ),
        SearchResult(
            provider=ProviderName.YAHOO,
            symbol="CSPX",
            name="iShares Core S&P 500",
            currency=Currency("USD"),
        ),
    ]

    jsonl = '{"isin": "US0378331005", "symbol": "AAPL"}\n{"isin": "IE00B5BMR087", "symbol": "CSPX"}'
    with (
        patch("sys.stdin.isatty", return_value=False),
        patch("sys.stdin.read", return_value=jsonl),
    ):
        main(["resolve", "--format", "table"])

    out = capsys.readouterr().out
    assert "Apple Inc." in out
    assert "iShares Core S&P 500" in out
    assert mock_resolve.call_count == 2


@patch("instrument_registry.finder.resolve_and_persist")
def test_cli_resolve_jsonl_pipe_skips_blank_lines(mock_resolve, capsys):
    from pydantic_market_data.models import Currency

    from instrument_registry.interfaces import ProviderName, SearchResult

    mock_resolve.return_value = SearchResult(
        provider=ProviderName.YAHOO, symbol="AAPL", name="Apple Inc.", currency=Currency("USD")
    )

    jsonl = '\n{"isin": "US0378331005"}\n\n'
    with (
        patch("sys.stdin.isatty", return_value=False),
        patch("sys.stdin.read", return_value=jsonl),
    ):
        main(["resolve", "--format", "table"])

    assert mock_resolve.call_count == 1


def test_cli_resolve_jsonl_pipe_invalid_line_fails():
    with (
        patch("sys.stdin.isatty", return_value=False),
        patch("sys.stdin.read", return_value='{"isin": "US0378331005"}\nnot-json'),
        pytest.raises(SystemExit) as exc,
    ):
        main(["resolve", "--format", "table"])

    assert exc.value.code == 1
