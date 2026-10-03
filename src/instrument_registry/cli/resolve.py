import datetime
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from pydantic_market_data import AssetClass as PmdAssetClass
from pydantic_market_data.models import (
    Currency,
    CurrencyCode,
    Price,
    PriceOnDate,
    SecurityQuery,
)
from treaty import Arg, Batch, CliExit, Ctx, Exit, Flag, Item, ItemError, ParseError

from ..registry import InstrumentRegistry, SaveEffect
from . import common
from .records import Effect, InstrumentRecord
from .treaty_app import Registry, RegistryScope, WriteTarget, app, raise_missing_provider

logger = logging.getLogger(__name__)
COMMAND_NAME = "instrument-reg resolve"


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


def _query(
    *,
    isin: str | None,
    symbol: str | None,
    figi: str | None,
    currency: str | None,
    asset_class: str | None,
    price_on: PriceOnDate | None,
) -> SecurityQuery:
    return SecurityQuery(
        isin=isin,
        symbol=symbol,
        figi=figi,
        currency=CurrencyCode(Currency(currency.upper())) if currency else None,
        price_on=[price_on] if price_on else None,
        asset_class=_coerce_asset_class(asset_class),
    )


def _resolve(
    criteria: SecurityQuery,
    *,
    label: str,
    lookup: InstrumentRegistry,
    target_path: Path | None,
    dry_run: bool,
    report_price: bool,
) -> InstrumentRecord:
    """Resolve one query from the registry first, then providers, saving a new discovery"""
    from ..finder import get_available_providers, resolve_and_persist

    logger.info("Resolving query: %s", label)
    try:
        result = resolve_and_persist(
            criteria,
            registry=lookup,
            store=True,
            target_path=target_path,
            dry_run=dry_run,
            include_price=report_price,
        )
    except ImportError:
        raise_missing_provider(None, COMMAND_NAME)

    if not result:
        if not get_available_providers():
            raise_missing_provider(None, COMMAND_NAME)
        raise Exit.NOT_FOUND(
            f"Could not resolve '{label}'.",
            context={"query": label},
            suggestion="check the identifier, or narrow it with --currency or --asset-class",
        )

    res, new_instrument = result
    if new_instrument is not None:
        effect = Effect.of_save(SaveEffect.CREATED, dry_run=dry_run)
        record = InstrumentRecord.from_instrument(new_instrument, effect)
    elif candidates := lookup.find_candidates(criteria):
        record = InstrumentRecord.from_instrument(
            candidates[0], Effect.of_save(None, dry_run=dry_run)
        )
    else:
        record = InstrumentRecord.from_search_result(res, Effect.of_save(None, dry_run=dry_run))
    return record.with_price(res) if report_price else record


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolveOptions(RegistryScope):
    currency: str | None = Flag(
        default=None, pattern="[A-Za-z]{3}", description="Restrict matches to this currency code"
    )
    asset_class: str | None = Flag(default=None, description="Restrict matches to this asset class")
    report_price: bool = Flag(
        default=False,
        description="Fetch and include the current price (or historical price if --date is given)",
    )
    dry_run: bool = Flag(
        default=False, description="Resolve without writing new discoveries to the registry"
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolveArgs(ResolveOptions):
    query: str = Arg(description="ISIN, provider symbol, IBKR conid, or currency pair")
    figi: str | None = Flag(
        default=None, description="FIGI identifier for direct security lookup via OpenFIGI"
    )
    date: datetime.date | None = Flag(
        default=None, description="Historical date to verify --price on"
    )
    price: Decimal | None = Flag(
        default=None, description="Historical price the instrument traded at on --date"
    )

    def __post_init__(self) -> None:
        if (self.date is None) != (self.price is None):
            raise ParseError("pass --date and --price together")
        try:
            self.criteria()
        except ValidationError as exc:
            raise ParseError(f"invalid query: {exc.errors()[0]['msg']}") from exc

    def criteria(self) -> SecurityQuery:
        isin = self.query if common.is_isin(self.query) else None
        price_on = (
            PriceOnDate(price=Price(float(self.price)), date=self.date)
            if self.price is not None and self.date is not None
            else None
        )
        return _query(
            isin=isin,
            symbol=None if isin else self.query,
            figi=self.figi,
            currency=self.currency,
            asset_class=self.asset_class,
            price_on=price_on,
        )


@app.command(
    "resolve",
    description="Resolve a query from local registries first, then external providers",
    danger_level="mutating",
    has_network_io=True,
    timeout=120,
    exit_codes=["NOT_FOUND", "MISSING_PROVIDER"],
    examples=[
        ("Resolve Apple by ISIN", "instrument-reg resolve US0378331005"),
        ("Resolve a currency pair", "instrument-reg resolve EUR/JPY"),
        (
            "Resolve and check a historical price, saving nothing",
            "instrument-reg resolve US0378331005 --date 2024-01-02 --price 185.00 --dry-run",
        ),
    ],
)
def resolve(
    args: ResolveArgs, ctx: Ctx, registry: Registry, target: WriteTarget
) -> InstrumentRecord:
    if common.is_ibkr_conid(args.query):
        logger.info("Numeric query detected. Checking registry for IBKR conid: %s", args.query)
        known = registry.lookup.find_by_ticker("IBKR", args.query)
        if known is not None:
            return InstrumentRecord.from_instrument(
                known, Effect.of_save(None, dry_run=args.dry_run)
            )

    return _resolve(
        args.criteria(),
        label=args.query,
        lookup=registry.lookup,
        target_path=target.path,
        dry_run=args.dry_run,
        report_price=args.report_price,
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolveBatchArgs(ResolveOptions):
    pass


def _record_price_on(record: dict[str, Any]) -> PriceOnDate | None:
    """``target_price`` with ``target_date``, else the first ``price_on`` entry"""
    if record.get("target_price") is not None and record.get("target_date") is not None:
        return PriceOnDate(
            price=Price(float(record["target_price"])),
            date=datetime.date.fromisoformat(str(record["target_date"])[:10]),
        )
    raw = record.get("price_on")
    # pmdp >= 0.4.1 emits a list
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if raw and raw.get("price") is not None and raw.get("date"):
        return PriceOnDate(
            price=Price(float(raw["price"])),
            date=datetime.date.fromisoformat(str(raw["date"])[:10]),
        )
    return None


@app.command(
    "resolve-batch",
    description="Resolve every query record read from stdin, one result per record",
    danger_level="mutating",
    has_network_io=True,
    stdin_input=True,
    timeout=900,
    exit_codes=["NOT_FOUND", "MISSING_PROVIDER"],
    examples=[
        (
            "Resolve the securities of an IBKR statement",
            "ibkr-converter securities statement.xml | instrument-reg resolve-batch",
        ),
        (
            "Resolve records from a file, saving nothing",
            "instrument-reg resolve-batch --input-file queries.jsonl --dry-run",
        ),
    ],
)
def resolve_batch(
    args: ResolveBatchArgs, ctx: Ctx, registry: Registry, target: WriteTarget
) -> Batch[InstrumentRecord]:
    assert ctx.stdin_text is not None  # stdin_input=True
    try:
        records = _parse_pipe(ctx.stdin_text)
    except ValueError as exc:
        raise Exit.ARG_ERROR(
            str(exc), suggestion="pipe JSON objects, one per query, or a JSON envelope"
        ) from exc

    items: list[Item[InstrumentRecord]] = []
    for number, record in enumerate(records, start=1):
        label = str(record.get("isin") or record.get("figi") or record.get("symbol") or number)
        try:
            criteria = _query(
                isin=record.get("isin"),
                symbol=record.get("symbol"),
                figi=record.get("figi"),
                currency=args.currency or record.get("currency"),
                asset_class=args.asset_class or record.get("asset_class"),
                price_on=_record_price_on(record),
            )
        except ValueError as exc:  # pydantic's ValidationError included
            items.append(Item(number, error=ItemError("INVALID_RECORD", f"{label}: {exc}")))
            continue
        try:
            value = _resolve(
                criteria,
                label=label,
                lookup=registry.lookup,
                target_path=target.path,
                dry_run=args.dry_run,
                report_price=args.report_price,
            )
        except CliExit as exc:
            items.append(Item(number, error=exc))
            continue
        items.append(Item(number, value))
    return Batch(items)
