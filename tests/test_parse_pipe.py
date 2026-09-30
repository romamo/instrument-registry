"""Stdin parsing for `resolve`: bare records, arrays, and ok/data/error envelopes."""

import json

import pytest

from instrument_registry.cli.resolve import _parse_pipe

AAPL = {"isin": "US0378331005", "symbol": "AAPL"}
MSFT = {"isin": "US5949181045", "symbol": "MSFT"}


def _envelope(data: object, ok: bool = True, error: object = None) -> dict[str, object]:
    return {"ok": ok, "data": data, "error": error, "warnings": [], "meta": {}}


def test_jsonl_records() -> None:
    assert _parse_pipe(f"{json.dumps(AAPL)}\n{json.dumps(MSFT)}\n") == [AAPL, MSFT]


def test_top_level_array() -> None:
    assert _parse_pipe(json.dumps([AAPL, MSFT])) == [AAPL, MSFT]


def test_envelope_with_list_data() -> None:
    assert _parse_pipe(json.dumps(_envelope([AAPL, MSFT]), indent=2)) == [AAPL, MSFT]


def test_envelope_with_object_data() -> None:
    assert _parse_pipe(json.dumps(_envelope(AAPL))) == [AAPL]


def test_concatenated_pretty_envelopes() -> None:
    raw = json.dumps(_envelope(AAPL), indent=2) + "\n" + json.dumps(_envelope([MSFT]), indent=2)
    assert _parse_pipe(raw) == [AAPL, MSFT]


def test_streamed_envelopes_skip_terminal_null_data() -> None:
    lines = [_envelope(AAPL), _envelope(MSFT), _envelope(None)]
    assert _parse_pipe("\n".join(json.dumps(line) for line in lines)) == [AAPL, MSFT]


def test_failed_upstream_envelope_raises() -> None:
    raw = "\n".join(
        [
            json.dumps(_envelope(AAPL)),
            json.dumps(_envelope(None, ok=False, error={"code": "TIMEOUT", "message": "slow"})),
        ]
    )
    with pytest.raises(ValueError, match=r"Upstream command failed \(line 2\): TIMEOUT: slow"):
        _parse_pipe(raw)


def test_record_named_like_envelope_key_is_still_a_record() -> None:
    record = {"symbol": "OK", "data": "x"}  # no "ok" key: not an envelope
    assert _parse_pipe(json.dumps(record)) == [record]


def test_empty_stdin_raises() -> None:
    with pytest.raises(ValueError, match="Stdin is empty"):
        _parse_pipe("  \n")


def test_invalid_json_reports_line() -> None:
    with pytest.raises(ValueError, match="Invalid JSON on line 3"):
        _parse_pipe(f"{json.dumps(AAPL)}\n{json.dumps(MSFT)}\nnot-json")


def test_non_object_item_reports_line() -> None:
    raw = json.dumps(_envelope(AAPL), indent=2) + "\n" + json.dumps(_envelope([1]))
    with pytest.raises(ValueError, match="Line 11: expected a JSON object, got int"):
        _parse_pipe(raw)
