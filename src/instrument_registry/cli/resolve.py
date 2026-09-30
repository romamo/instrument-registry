from __future__ import annotations

import json
import logging
import sys
from typing import Any

import agentyper as typer
import pandas as pd  # type: ignore[import-untyped]
from pydantic_market_data import AssetClass as PmdAssetClass
from pydantic_market_data.models import (
    Currency,
    CurrencyCode,
    Price,
    PriceOnDate,
    PriceVerificationError,
    SecurityQuery,
)

from . import common

logger = logging.getLogger(__name__)


class ResolutionFailed(Exception):
    pass


_LOCAL_TO_PMD: dict[str, PmdAssetClass] = {
    "stock": PmdAssetClass.EQUITY,
    "equityetf": PmdAssetClass.EQUITY,
    "fixedincomeetf": PmdAssetClass.FIXED_INCOME,
    "moneymarketetf": PmdAssetClass.FIXED_INCOME,
    "commodityetf": PmdAssetClass.COMMODITY,
    "commodity": PmdAssetClass.COMMODITY,
    "crypto": PmdAssetClass.CRYPTO,
    "cash": PmdAssetClass.CASH,
    "forex": PmdAssetClass.FX,
}


def _coerce_asset_class(raw: str | None) -> PmdAssetClass | None:
    if not raw:
        return None
    try:
        return PmdAssetClass(raw.lower().replace(" ", "_"))
    except ValueError:
        pass
    return _LOCAL_TO_PMD.get(raw.lower().replace(" ", "").replace("_", ""))


def _read_pipe() -> list[dict[str, Any]]:
    raw = sys.stdin.read()
    try:
        return _parse_pipe(raw)
    except ValueError as exc:
        common.exit_with_error(str(exc), error_type="ArgError")
        raise AssertionError("unreachable") from None


def _parse_pipe(raw: str) -> list[dict[str, Any]]:
    """Query records from stdin text: JSON objects, arrays, or ok/data/error envelopes.

    Values may be concatenated in any layout (JSONL, pretty-printed, or back to back).
    An envelope contributes its ``data`` (each item of a list); ``data: null`` contributes
    nothing; ``ok: false`` raises, so a failed upstream command is never read as input.
    """
    if not raw.strip():
        raise ValueError("Stdin is empty")
    decoder = json.JSONDecoder()
    records: list[dict[str, Any]] = []
    pos = 0
    line_num = 1  # line where the next value starts, counted as pos advances
    while True:
        skip_from = pos
        while pos < len(raw) and raw[pos] in " \t\n\r":
            pos += 1
        line_num += raw.count("\n", skip_from, pos)
        if pos >= len(raw):
            break
        value_start = pos
        try:
            value, pos = decoder.raw_decode(raw, pos)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON on line {exc.lineno}: {exc.msg}") from None
        records.extend(_pipe_value_records(value, line_num))
        line_num += raw.count("\n", value_start, pos)
    return records


def _pipe_value_records(value: Any, line_num: int) -> list[dict[str, Any]]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, dict) and "ok" in value and "data" in value:
        if value["ok"] is not True:
            error = value.get("error") or {}
            raise ValueError(
                f"Upstream command failed (line {line_num}): "
                f"{error.get('code', 'ERROR')}: {error.get('message', 'no message')}"
            )
        data = value["data"]
        items = [] if data is None else data if isinstance(data, list) else [data]
    else:
        items = [value]
    for item in items:
        if not isinstance(item, dict):
            raise ValueError(f"Line {line_num}: expected a JSON object, got {type(item).__name__}")
    return items


def _resolve_criteria(
    *,
    ctx: typer.Context,
    isin: str | None,
    symbol: str | None,
    figi: str | None,
    currency: str | None,
    asset_class: str | None,
    price: float | None,
    date: str | None,
    price_on: PriceOnDate | None = None,
    dry_run: bool,
    report_price: bool,
    reg: Any,
    target_path: Any,
) -> None:
    from ..finder import get_available_providers, resolve_and_persist, verify_ticker

    query_label = isin or figi or symbol or "(stdin)"
    logger.info("Resolving query: %s", query_label)

    if price is not None and date:
        price_on = PriceOnDate(
            price=Price(price),
            date=pd.to_datetime(date).date(),
        )

    criteria = SecurityQuery(
        isin=isin,
        symbol=symbol,
        figi=figi,
        currency=CurrencyCode(Currency(currency.upper())) if currency else None,
        price_on=price_on,
        asset_class=_coerce_asset_class(asset_class),
    )

    result = resolve_and_persist(
        criteria,
        registry=reg,
        store=True,
        target_path=target_path,
        dry_run=dry_run,
        include_price=report_price,
    )

    if not result:
        providers = get_available_providers()
        if not providers:
            raise ResolutionFailed(
                f"Could not resolve '{query_label}'. "
                f"Install providers: uv tool install 'instrument-registry[providers]'"
            )
        raise ResolutionFailed(f"Could not resolve '{query_label}'")

    res, new_instrument = result

    if date and price is not None and new_instrument is not None:
        logger.info("Verifying price %s on %s...", price, date)
        if price_on is None:
            raise AssertionError(f"price_on must be set when date={date!r} and price are given")
        try:
            if verify_ticker(res.symbol, price_on.date, price, provider=res.provider):
                typer.echo(f"  [OK] Verified {res.name} via {res.provider.upper()} ({res.symbol})")
            else:
                raise ResolutionFailed(
                    f"  [!] FAILED: Price {price} on {date} does not match {res.symbol}"
                )
        except PriceVerificationError as exc:
            raise ResolutionFailed(f"  [!] FAILED: {exc}") from exc

    if new_instrument is not None:
        typer.output(new_instrument)
    elif _candidates := reg.find_candidates(criteria):
        typer.output(_candidates[0])
    else:
        typer.output(res)


def command(
    ctx: typer.Context,
    query: str | None = typer.Argument(None, help="Instrument query or identifier"),  # noqa: B008
    registry_path: str | None = common.REGISTRY_PATH_OPTION,
    no_bundled: bool = common.NO_BUNDLED_OPTION,
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Preferred provider name for external resolution",
    ),  # noqa: B008
    figi: str | None = typer.Option(
        None,
        "--figi",
        help="FIGI identifier for direct security lookup via OpenFIGI",
    ),  # noqa: B008
    currency: str | None = typer.Option(
        None,
        "--currency",
        help="Restrict matches to this currency code",
    ),  # noqa: B008
    date: str | None = typer.Option(
        None,
        "--date",
        help="Historical date used with --price verification",
    ),  # noqa: B008
    price: float | None = typer.Option(
        None,
        "--price",
        help="Historical price used with --date verification",
    ),  # noqa: B008
    report_price: bool = typer.Option(  # noqa: B008
        False,
        "--report-price",
        is_flag=True,
        help="Fetch and include the current price (or historical price if --date is given)",
    ),
    asset_class: str | None = typer.Option(
        None,
        "--asset-class",
        help="Restrict matches to this asset class",
    ),  # noqa: B008
    no_save: bool = typer.Option(  # noqa: B008
        False,
        "--no-save",
        is_flag=True,
        help="Resolve and return output without writing to registry file",
    ),
) -> None:
    """Resolve a query from local registries first, then external providers."""
    del provider  # Provider filtering is not implemented yet.
    from ..interfaces import ProviderName, SearchResult

    common.configure_registry_scope(
        ctx=ctx,
        registry_path=registry_path,
        no_bundled=no_bundled,
    )

    reg = common.registry()
    target_path = common.primary_registry_path()

    if query is not None and common.is_ibkr_conid(query):
        logger.info("Numeric query detected. Checking registry for IBKR conid: %s...", query)
        res_comp = reg.find_by_ticker("IBKR", query)
        if res_comp:
            conid_result = SearchResult(
                provider=ProviderName.YAHOO,
                symbol=res_comp.tickers.yahoo if res_comp.tickers else res_comp.symbol,
                name=res_comp.name or res_comp.symbol,
                currency=res_comp.currency,
                asset_class=res_comp.asset_class,
                instrument_type=res_comp.instrument_type,
                country=res_comp.country,
            )
            typer.output(conid_result)
            return

    if query is not None:
        isin = query if common.is_isin(query) else None
        symbol = query if not isin else None
        try:
            _resolve_criteria(
                ctx=ctx,
                isin=isin,
                symbol=symbol,
                figi=figi,
                currency=currency,
                asset_class=asset_class,
                price=price,
                date=date,
                dry_run=no_save,
                report_price=report_price,
                reg=reg,
                target_path=target_path,
            )
        except ResolutionFailed as exc:
            common.exit_with_error(str(exc))
    else:
        if sys.stdin.isatty():
            common.exit_with_error("No query provided", error_type="ArgError")
            raise AssertionError("unreachable")
        for pipe_data in _read_pipe():
            rec_price = (
                price
                if price is not None
                else (
                    float(pipe_data["target_price"])
                    if pipe_data.get("target_price") is not None
                    else None
                )
            )
            rec_date = (
                date
                if date is not None
                else (
                    str(pipe_data["target_date"])
                    if pipe_data.get("target_date") is not None
                    else None
                )
            )
            raw_price_on_field = pipe_data.get("price_on") if rec_price is None else None
            # Accept both a single dict and a list (pmdp >= 0.4.1 emits a list)
            if isinstance(raw_price_on_field, list):
                raw_price_on_field = raw_price_on_field[0] if raw_price_on_field else None
            pipe_price_on: PriceOnDate | None = None
            if (
                raw_price_on_field
                and raw_price_on_field.get("price") is not None
                and raw_price_on_field.get("date")
            ):
                pipe_price_on = PriceOnDate(
                    price=Price(float(raw_price_on_field["price"])),
                    date=pd.to_datetime(str(raw_price_on_field["date"])).date(),
                )
            try:
                _resolve_criteria(
                    ctx=ctx,
                    isin=pipe_data.get("isin"),
                    symbol=pipe_data.get("symbol"),
                    figi=figi or pipe_data.get("figi"),
                    currency=currency or pipe_data.get("currency"),
                    asset_class=asset_class or pipe_data.get("asset_class"),
                    price=rec_price,
                    date=rec_date,
                    price_on=pipe_price_on,
                    dry_run=no_save,
                    report_price=report_price,
                    reg=reg,
                    target_path=target_path,
                )
            except ResolutionFailed as exc:
                logger.error("%s", exc)
