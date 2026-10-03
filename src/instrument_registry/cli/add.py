import datetime
import logging
from dataclasses import dataclass
from decimal import Decimal

from pydantic import ValidationError
from pydantic_market_data.models import Currency, CurrencyCode, Price, PriceOnDate, SecurityQuery
from treaty import Arg, Ctx, Exit, Flag, ParseError

from ..interfaces import SearchResult
from ..models import AssetClass, InstrumentType
from . import common
from .records import Effect, InstrumentRecord
from .treaty_app import Registry, RegistryScope, WriteTarget, app, raise_missing_provider

logger = logging.getLogger(__name__)
COMMAND_NAME = "instrument-reg add"


@dataclass(frozen=True, slots=True)
class AddArgs(RegistryScope):
    query: str | None = Arg(description="ISIN or provider symbol of the instrument")
    canonical: str | None = Flag(
        default=None,
        description="Canonical instrument symbol to store (overrides auto-derived symbol)",
    )
    isin: str | None = Flag(default=None, description="ISIN code")
    symbol: str | None = Flag(
        default=None, description="Provider symbol to store when no fetch result is used"
    )
    instrument_type: InstrumentType | None = Flag(
        default=None, description="Instrument type to store; --fetch may supply it"
    )
    asset_class: AssetClass | None = Flag(
        default=None, description="Asset class to store; --fetch may supply it"
    )
    currency: str | None = Flag(
        default=None,
        pattern="[A-Za-z]{3}",
        description="Primary trading currency; --fetch may supply it",
    )
    ibkr: int | None = Flag(default=None, description="Interactive Brokers conid")
    country: str | None = Flag(default=None, description="Country or region of origin")
    validation_date: datetime.date | None = Flag(
        default=None, description="Historical date for an initial validation point"
    )
    validation_price: Decimal | None = Flag(
        default=None, description="Historical price for an initial validation point"
    )
    fetch: bool = Flag(default=False, description="Look up missing details from providers")
    dry_run: bool = Flag(default=False, description="Show the record that would be saved")

    def __post_init__(self) -> None:
        errors: list[ParseError] = []
        isin, ticker = self.identifiers()
        if not isin and not ticker:
            errors.append(ParseError("pass a QUERY, --isin, or --symbol"))
        if (self.validation_date is None) != (self.validation_price is None):
            errors.append(ParseError("pass --validation-date and --validation-price together"))
        if not self.fetch:
            missing = [
                f"--{name.replace('_', '-')}"
                for name in ("currency", "instrument_type", "asset_class")
                if getattr(self, name) is None
            ]
            if missing:
                errors.append(
                    ParseError(f"missing {', '.join(missing)}; pass them, or --fetch to look up")
                )
        try:
            self.criteria(self.currency)
        except ValidationError as exc:
            errors.append(ParseError(f"invalid identifier: {exc.errors()[0]['msg']}"))
        if errors:
            raise ParseError.combine(errors)

    def identifiers(self) -> tuple[str | None, str | None]:
        """The ISIN and the provider symbol, with QUERY filling whichever it looks like"""
        isin, ticker = self.isin, self.symbol
        if self.query:
            if common.is_isin(self.query):
                isin = isin or self.query
            else:
                ticker = ticker or self.query
        return isin, ticker

    def criteria(self, currency: str | None) -> SecurityQuery:
        isin, ticker = self.identifiers()
        price_on = (
            PriceOnDate(price=Price(float(self.validation_price)), date=self.validation_date)
            if self.validation_price is not None and self.validation_date is not None
            else None
        )
        return SecurityQuery(
            isin=isin,
            symbol=ticker,
            currency=CurrencyCode(Currency(currency.upper())) if currency else None,
            price_on=[price_on] if price_on else None,
        )


@app.command(
    "add",
    description="Add or update an instrument in the user registry write target",
    danger_level="mutating",
    has_network_io=True,
    timeout=120,
    exit_codes=["CONFLICT", "MISSING_PROVIDER"],
    examples=[
        (
            "Add Apple by ISIN with its details",
            "instrument-reg add US0378331005 --symbol AAPL --currency USD"
            " --instrument-type Stock --asset-class Stock --registry-path ~/registry",
        ),
        (
            "Preview a record filled in from providers",
            "instrument-reg add US0378331005 --fetch --dry-run --registry-path ~/registry",
        ),
    ],
)
def add(args: AddArgs, ctx: Ctx, registry: Registry, target: WriteTarget) -> InstrumentRecord:
    from ..finder import search_isin
    from ..registry import SymbolCollision, build_instrument, save_instrument

    target_path = target.require()
    if target_path.is_dir():
        target_path = target_path / "manual.yaml"

    isin, ticker = args.identifiers()
    currency = args.currency
    canonical = args.canonical
    existing = (
        registry.lookup.find_by_isin(isin, Currency(currency.upper()))
        if isin and currency
        else None
    )
    if existing and not canonical:
        logger.info(
            "Preserving existing symbol '%s' for instrument %s/%s", existing.symbol, isin, currency
        )
        canonical = existing.symbol

    criteria = args.criteria(currency)
    metadata: SearchResult | None = None
    if args.fetch:
        logger.info("Searching for metadata...")
        try:
            results = search_isin(criteria)
        except ImportError:
            raise_missing_provider(None, COMMAND_NAME)
        if results:
            metadata = results[0]
            logger.info(
                "Found candidate: %s (%s) - %s", metadata.symbol, metadata.provider, metadata.name
            )
            if not criteria.symbol:
                criteria.symbol = str(metadata.symbol)
            if not criteria.currency and metadata.currency:
                criteria.currency = metadata.currency
        else:
            ctx.warn("METADATA_NOT_FOUND", "no provider returned details; saving the given ones")

    if not criteria.currency:
        raise ParseError(
            "no currency: pass --currency, since providers returned none",
            context={"field": "currency"},
        )

    try:
        instrument = build_instrument(
            criteria=criteria,
            metadata=metadata,
            instrument_type=args.instrument_type,
            asset_class=args.asset_class,
            symbol=canonical,
            registry=registry.lookup,
            country=args.country,
            ibkr=args.ibkr,
        )
    except SymbolCollision as exc:
        raise Exit.CONFLICT(
            str(exc),
            context={"canonical": canonical},
            suggestion="pass --canonical with a symbol that is not taken",
        ) from exc
    except ValueError as exc:
        raise ParseError(str(exc)) from exc

    saved = save_instrument(instrument, target_path, dry_run=args.dry_run)
    return InstrumentRecord.from_instrument(instrument, Effect.of_save(saved, dry_run=args.dry_run))
