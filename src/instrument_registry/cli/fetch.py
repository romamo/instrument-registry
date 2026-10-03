import datetime
import logging
from dataclasses import dataclass
from typing import Self

from pydantic import ValidationError
from pydantic_market_data.models import Price, SecurityQuery
from treaty import Ctx, Exit, Flag, Out, ParseError

from ..interfaces import ProviderName, SearchResult
from ..models import AssetClass, InstrumentType
from .treaty_app import Registry, RegistryScope, app, raise_missing_provider, require_live_providers

logger = logging.getLogger(__name__)
COMMAND_NAME = "instrument-reg fetch"


@dataclass(frozen=True, slots=True)
class FetchArgs(RegistryScope):
    isin: str | None = Flag(default=None, description="Fetch using an ISIN")
    figi: str | None = Flag(default=None, description="Fetch using a FIGI (FT Markets only)")
    symbol: str | None = Flag(default=None, description="Fetch using a provider symbol")
    price: bool = Flag(default=False, description="Also fetch the latest price")

    def __post_init__(self) -> None:
        if not (self.isin or self.figi or self.symbol):
            raise ParseError("pass --isin, --figi, or --symbol")
        try:
            self.query()
        except ValidationError as exc:
            raise ParseError(f"invalid identifier: {exc.errors()[0]['msg']}") from exc

    def query(self) -> SecurityQuery:
        return SecurityQuery(isin=self.isin, figi=self.figi, symbol=self.symbol)

    def provider_hint(self) -> ProviderName:
        figi_only = self.figi and not self.isin and not self.symbol
        return ProviderName.FT if figi_only else ProviderName.YAHOO


@dataclass(frozen=True, slots=True)
class FetchResult:
    provider: ProviderName | None
    symbol: str
    name: str = Out(external=True)
    currency: str | None = None
    asset_class: AssetClass | None = None
    instrument_type: InstrumentType | None = None
    price: float | None = None
    price_date: datetime.date | None = None
    country: str | None = Out(default=None, external=True)
    isin: str | None = None
    figi: str | None = None
    ticker: str | None = None
    metadata: dict[str, object] = Out(default_factory=dict, ordered=True, external=True)

    @classmethod
    def from_search_result(cls, res: SearchResult) -> Self:
        return cls(
            provider=res.provider,
            symbol=str(res.symbol),
            name=res.name,
            currency=str(res.currency) if res.currency is not None else None,
            asset_class=res.asset_class,
            instrument_type=res.instrument_type,
            price=res.price.root if isinstance(res.price, Price) else res.price,
            price_date=res.price_date,
            country=res.country,
            isin=res.isin,
            figi=res.figi,
            ticker=res.ticker,
            metadata=res.metadata or {},
        )


@app.command(
    "fetch",
    description="Fetch provider details after checking user and bundled registries first",
    danger_level="safe",
    has_network_io=True,
    exit_codes=["NOT_FOUND", "MISSING_PROVIDER"],
    examples=[
        ("Fetch Apple by ISIN", "instrument-reg fetch --isin US0378331005"),
        ("Fetch a symbol with its latest price", "instrument-reg fetch --symbol AAPL --price"),
    ],
)
def fetch(args: FetchArgs, ctx: Ctx, registry: Registry) -> FetchResult:
    from ..finder import fetch_price, resolve_security

    provider_hint = args.provider_hint()
    require_live_providers(provider_hint, COMMAND_NAME)
    logger.info(
        "Fetching details for ISIN=%s, FIGI=%s, Ticker=%s", args.isin, args.figi, args.symbol
    )
    try:
        res = resolve_security(args.query(), verify=True, registry=registry.lookup)
    except ImportError:
        raise_missing_provider(provider_hint, COMMAND_NAME)

    if res is None:
        raise Exit.NOT_FOUND(
            "no provider or registry returned a match",
            context={"isin": args.isin, "figi": args.figi, "symbol": args.symbol},
            suggestion="check the identifier, or try another of --isin, --figi, --symbol",
        )

    if args.price:
        if res.provider is None:
            ctx.warn("PRICE_UNAVAILABLE", "the match names no provider to fetch a price from")
        else:
            try:
                fetched_price = fetch_price(res.symbol, provider=res.provider)
            except ImportError:
                raise_missing_provider(res.provider, COMMAND_NAME)
            if fetched_price is None:
                ctx.warn("PRICE_UNAVAILABLE", f"{res.provider} returned no price for {res.symbol}")
            else:
                res.price = fetched_price

    return FetchResult.from_search_result(res)
