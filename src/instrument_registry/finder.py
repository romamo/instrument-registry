from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import Instrument
    from .registry import InstrumentRegistry

import diskcache  # type: ignore[import-untyped]
import platformdirs
from pydantic import ValidationError as _PydanticValidationError
from pydantic_market_data.models import (
    Currency,
    CurrencyCode,
    Price,
    PriceVerificationError,
    SecurityQuery,
    Symbol,
)

from .interfaces import DataProvider, InstrumentLookup, ProviderName, SearchResult
from .models import AssetClass, InstrumentType, _map_asset_class

logger = logging.getLogger(__name__)

CACHE_DIR_ENV_VAR = "INSTRUMENT_REGISTRY_CACHE_DIR"

_RAW_TYPE_MAP: list[tuple[str, InstrumentType, AssetClass]] = [
    ("ETF", InstrumentType.ETF, AssetClass.EQUITY_ETF),
    ("ETP", InstrumentType.ETF, AssetClass.EQUITY_ETF),
    ("MUTUALFUND", InstrumentType.ETF, AssetClass.EQUITY_ETF),
    ("INDEX", InstrumentType.INDEX, AssetClass.STOCK),
    ("CRYPTO", InstrumentType.CRYPTO, AssetClass.CRYPTO),
    ("CURRENCY", InstrumentType.CASH, AssetClass.CASH),
    ("CASH", InstrumentType.CASH, AssetClass.CASH),
]


def _infer_types(
    raw_asset_class: str | None, security_type: str | None = None
) -> tuple[InstrumentType, AssetClass]:
    # securityType is more specific than marketSector — check it first
    for raw in (security_type, raw_asset_class):
        s = (raw or "").upper()
        for keyword, inst_type, asset_class in _RAW_TYPE_MAP:
            if keyword in s:
                return inst_type, asset_class
    return InstrumentType.STOCK, AssetClass.STOCK


def _get_cache_dir() -> Path:
    """Return the cache directory, allowing an env-var override for sandboxed runs."""
    cache_dir = os.getenv(CACHE_DIR_ENV_VAR)
    if cache_dir:
        return Path(cache_dir).expanduser()
    return Path(platformdirs.user_cache_dir("instrument-registry"))


def _fallback_cache_dir() -> Path:
    """Use a local cache directory when the platform cache is unavailable."""
    return Path.cwd() / ".cache" / "instrument-registry"


def _init_cache() -> diskcache.Cache:
    cache_dir = _get_cache_dir()
    try:
        return diskcache.Cache(str(cache_dir))
    except (OSError, sqlite3.OperationalError) as exc:
        if os.getenv(CACHE_DIR_ENV_VAR):
            raise

        fallback_dir = _fallback_cache_dir()
        logger.warning(
            "Could not open cache directory %s (%s). Falling back to %s.",
            cache_dir,
            exc,
            fallback_dir,
        )
        return diskcache.Cache(str(fallback_dir))


# Initialize cache (expires in 1 day by default)
cache = _init_cache()


try:
    from py_yfinance import YFinanceDataSource as _YFDS

    YFinanceDataSource: type[Any] | None = _YFDS
except ImportError as e:
    # Optional dependency exception handling:
    # py-yfinance is an optional plugin. If it's missing, we just log and proceed
    # with other available providers (or local registry data only).
    logger.debug(f"Failed to import py_yfinance: {e}")
    YFinanceDataSource = None

try:
    from ftmarkets import FTDataSource as _FTDS

    FTDataSource: type[Any] | None = _FTDS
except ImportError as e:
    logger.debug(f"Failed to import ftmarkets: {e}")
    FTDataSource = None

try:
    from openfigi import OpenFIGIDataSource as _OFDS

    OpenFIGIDataSource: type[Any] | None = _OFDS
except ImportError as e:
    logger.debug(f"Failed to import openfigi: {e}")
    OpenFIGIDataSource = None


def get_data_provider(provider: ProviderName) -> DataProvider:
    if provider == ProviderName.YAHOO:
        if YFinanceDataSource is None:
            raise ImportError("py-yfinance is not installed. Install with [providers] extra.")
        return YFinanceDataSource()
    if provider == ProviderName.FT:
        if FTDataSource is None:
            raise ImportError("ftmarkets is not installed. Install with [providers] extra.")
        return FTDataSource()
    raise ValueError(f"Unknown provider: {provider}")


def get_available_providers() -> list[ProviderName]:
    """Returns a list of available data providers."""
    providers: list[ProviderName] = []
    if YFinanceDataSource is not None:
        providers.append(ProviderName.YAHOO)
    if FTDataSource is not None:
        providers.append(ProviderName.FT)
    return providers


@cache.memoize(expire=86400)  # 24 hours
def fetch_metadata(
    symbol: Symbol.Input, isin: str | None = None, provider: ProviderName = ProviderName.YAHOO
) -> SearchResult | None:
    """
    Fetches common metadata for a security.
    """
    data_provider = get_data_provider(provider)

    # try...except removed to fail fast.
    # data_provider.resolve may raise ValidationError or other source-specific errors.
    # Boundary conversion
    symbol_vo = Symbol(symbol) if not isinstance(symbol, Symbol) else symbol
    criteria = SecurityQuery(symbol=symbol_vo, isin=isin)
    security = data_provider.resolve(criteria)

    if not security:
        logger.debug(f"No security resolved for {symbol} via {provider}")
        return None

    logger.debug(
        f"Fetched metadata for {symbol} ({provider}): ISIN={getattr(security, 'isin', None)}"
    )

    currency_val = (
        CurrencyCode(Currency(str(security.currency).upper())) if security.currency else None
    )

    return SearchResult(
        provider=provider,
        symbol=security.symbol,
        name=security.name,
        currency=currency_val,
        asset_class=None,  # Not currently resolved via basic symbol search
        instrument_type=None,
        price=None,
    )


@cache.memoize(expire=86400)
def derive_provider_ticker(
    symbol: str, asset_class: AssetClass | str | None, provider: ProviderName | str
) -> str | None:
    """
    Derives a provider-specific ticker based on an instrument's symbol and asset class.
    Used when an explicit ticker is missing from the registry.
    """
    # Normalize provider name
    p_name = provider.value if isinstance(provider, ProviderName) else str(provider).lower()

    if p_name == ProviderName.YAHOO.value:
        # Yahoo Crypto Pattern: {TOKEN}-USD
        ac = str(asset_class).upper() if asset_class else ""
        if ("CRYPTO" in ac) and "-" not in symbol:
            return f"{symbol}-USD"

    return None


def _resolve_via_openfigi(criteria: SecurityQuery) -> SearchResult | None:
    """Use OpenFIGI to resolve ISIN/FIGI to ticker, name, and FIGI metadata.

    Priority: FIGI → ISIN (matches resolve_candidates order).
    Falls back to ISIN when FIGI yields no results (e.g. currency filter excludes it).
    """
    if OpenFIGIDataSource is None:
        return None
    if not criteria.isin and not criteria.figi:
        return None

    ds = OpenFIGIDataSource()

    candidates, _ = ds.resolve_candidates(criteria)

    if not candidates:
        if criteria.figi and criteria.isin:
            logger.warning(
                "OpenFIGI: no results for FIGI %s (currency=%s). Falling back to ISIN %s.",
                criteria.figi,
                criteria.currency or "none",
                criteria.isin,
            )
            # Also clear symbol: resolve_candidates post-filters by symbol when figi is None,
            # which would reject all valid listings if the input symbol is not an OpenFIGI ticker.
            isin_criteria = criteria.model_copy(update={"figi": None, "symbol": None})
            candidates, _ = ds.resolve_candidates(isin_criteria)
            if not candidates:
                logger.debug("OpenFIGI: ISIN fallback also returned no data for %s", criteria.isin)
                return None
            resolved_id = str(criteria.isin)
        else:
            logger.debug("OpenFIGI: no data for %s", criteria.figi or criteria.isin)
            return None
    else:
        resolved_id = str(criteria.figi or criteria.isin)

    best = candidates[0]
    sym = str(best.symbol)
    if not sym:
        return None

    inst_type, asset_class = _infer_types(best.asset_class, security_type=best.security_type)

    logger.info(
        "OpenFIGI: %s → ticker=%s exchCode=%s figi=%s securityType=%s name=%s",
        resolved_id,
        sym,
        best.exchange,
        best.figi,
        best.security_type,
        best.name,
    )

    currency_val = (
        CurrencyCode(Currency(str(criteria.currency).upper()))
        if criteria.currency
        else (CurrencyCode(Currency(str(best.currency).upper())) if best.currency else None)
    )

    return SearchResult(
        provider=ProviderName.YAHOO,
        symbol=sym,
        name=best.name or sym,
        ticker=sym,
        currency=currency_val,
        isin=str(criteria.isin) if criteria.isin else (best.isin or None),
        figi=best.figi,
        asset_class=asset_class,
        instrument_type=inst_type,
    )


def _find_price_provider(
    ticker: str,
    isin: str | None = None,
    currency: CurrencyCode | None = None,
    validation_date: date | None = None,
    validation_price: float | None = None,
) -> tuple[ProviderName, str] | None:
    """Find the first provider that confirms this security; return (provider, provider_symbol).

    When isin is provided each provider resolves it to their own ticker format
    (e.g. FT → SEMU:LSE:USD, Yahoo → CHIP.SW) before attempting validation.
    When a validation point is supplied the provider's price must match.
    """
    for p in get_available_providers():
        dp = get_data_provider(p)

        # Prefer ISIN-based resolution to get provider-specific symbol format
        sym = ticker
        if isin:
            try:
                resolved = dp.resolve(SecurityQuery(isin=isin, currency=currency))
            except _PydanticValidationError as exc:
                # External provider returned a field value incompatible with pmdp's model
                # (e.g. py_yfinance passes raw quoteType strings like "ETF" to Security.asset_class
                # which only accepts PmdAssetClass literals). Treat as "provider can't resolve".
                logger.debug(
                    "Provider %s: validation error resolving ISIN %s: %s", p.value, isin, exc
                )
                continue
            if resolved:
                sym = str(resolved.symbol)
            else:
                logger.debug("Provider %s: could not resolve ISIN %s", p.value, isin)
                continue

        if validation_date is not None and validation_price is not None:
            try:
                valid = dp.validate(Symbol(sym), validation_date, Price(validation_price))
            except (RuntimeError, ValueError) as exc:
                logger.info(
                    "Provider %s: no data for %s on %s: %s", p.value, sym, validation_date, exc
                )
                continue
            except PriceVerificationError as exc:
                logger.info("Provider %s: price mismatch for %s: %s", p.value, sym, exc)
                valid = False
            if valid:
                return p, sym
            logger.info("Provider %s: price mismatch, skipping", p.value)
        else:
            logger.info("Provider: %s resolved %s → %s", p.value, isin or ticker, sym)
            return p, sym

    logger.info("Provider: none confirmed for %s", isin or ticker)
    return None


def search_isin(criteria: SecurityQuery) -> list[SearchResult]:
    """
    Searches for securities across all providers using SecurityQuery.
    Returns typed search results from each provider.
    """
    results = []

    # OpenFIGI first: resolves ISIN/FIGI to ticker and metadata, then finds a price provider
    if (criteria.isin or criteria.figi) and OpenFIGIDataSource is not None:
        openfigi_result = _resolve_via_openfigi(criteria)
        if openfigi_result:
            ticker_str = str(openfigi_result.symbol)
            price_on = criteria.price_on[0] if criteria.price_on else None
            logger.debug(
                "Provider lookup: ticker=%s isin=%s currency=%s date=%s price=%s",
                ticker_str,
                str(criteria.isin) if criteria.isin else None,
                str(criteria.currency) if criteria.currency else None,
                price_on.date if price_on else None,
                float(price_on.price.root) if price_on else None,
            )
            found = _find_price_provider(
                ticker_str,
                isin=str(criteria.isin) if criteria.isin else None,
                currency=criteria.currency,
                validation_date=price_on.date if price_on else None,
                validation_price=float(price_on.price.root) if price_on else None,
            )
            if found:
                price_provider, confirmed_sym = found
                openfigi_result.provider = price_provider
                openfigi_result.symbol = confirmed_sym
                logger.info("Provider: %s selected for %s", price_provider.value, confirmed_sym)
            elif price_on:
                # price_on was supplied but no provider confirmed the price — reject the result
                # to avoid persisting an unvalidated validation_point
                logger.info(
                    "Rejecting result: price_on %.4f on %s not confirmed by any provider",
                    float(price_on.price.root),
                    price_on.date,
                )
                return []
            else:
                logger.info(
                    "Provider: none confirmed for %s, defaulting to %s",
                    ticker_str,
                    openfigi_result.provider.value,
                )
            return [openfigi_result]

    providers = get_available_providers()

    for p in providers:
        try:
            data_provider = get_data_provider(p)
            security_result = data_provider.resolve(criteria)
            if security_result:
                aclass = (
                    _map_asset_class(security_result.asset_class)
                    if security_result.asset_class
                    else None
                )

                # Filtering by requested asset_class if provided
                if criteria.asset_class:
                    req_aclass = _map_asset_class(criteria.asset_class)
                    if req_aclass and aclass and aclass != req_aclass:
                        logger.debug(
                            f"Skipping result {security_result.symbol} due to asset class mismatch"
                        )
                        continue

                inst_type, resolved_aclass = _infer_types(security_result.asset_class)
                results.append(
                    SearchResult(
                        provider=p,
                        symbol=security_result.symbol,
                        name=security_result.name,
                        currency=security_result.currency,
                        asset_class=aclass or resolved_aclass,
                        instrument_type=inst_type,
                        price=None,
                        price_date=None,
                    )
                )
                # Optimization: First match is enough
                return results
        except Exception as e:
            logger.warning(
                f"Provider {p} failed to resolve {criteria.isin or criteria.symbol}: {e}"
            )
            continue
        else:
            logger.debug(f"No results from provider {p} for {criteria.isin or criteria.symbol}")

    if not results and criteria.symbol and criteria.asset_class:
        derived = derive_provider_ticker(
            str(criteria.symbol), criteria.asset_class, ProviderName.YAHOO
        )
        if derived and derived != str(criteria.symbol):
            crypto_criteria = criteria.model_copy(update={"symbol": derived})
            logger.debug(f"Retrying resolution with derived ticker: {crypto_criteria.symbol}")
            return search_isin(crypto_criteria)

    return results


def verify_ticker(
    symbol: Symbol.Input,
    date: date,
    price: Price.Input,
    provider: ProviderName = ProviderName.YAHOO,
) -> bool:
    """
    Verifies if a ticker traded at a specific price on a given date.
    """
    data_provider = get_data_provider(provider)
    # try...except removed to fail fast.
    # Boundary conversion
    symbol_vo = Symbol(symbol) if not isinstance(symbol, Symbol) else symbol
    price_vo = Price(price) if not isinstance(price, Price) else price
    return data_provider.validate(symbol_vo, date, price_vo)


def resolve_currency(
    symbol: str, target_currency: CurrencyCode.Input | None = None, verify: bool = False
) -> SearchResult | None:
    """
    Programmatically resolves standard currencies to Yahoo tickers (e.g. EUR -> EURUSD=X).
    Supports pairs strings like 'EURUSD', 'EUR/USD', 'EUR-USD'.
    If verify=True, performs a live lookup to ensure the ticker exists.
    Returns a SearchResult or None if not a valid currency pair or not found.
    """
    if target_currency is None:
        target_currency = "USD"

    quote_str = str(target_currency).upper()
    if not symbol:
        return None

    base = symbol.upper()

    # Handle composite inputs (e.g. "EURUSD", "EUR/USD")
    if len(base) == 6 and base.isalpha():
        # "EURUSD" -> base=EUR, quote=USD
        quote_str = base[3:].upper()
        base = base[:3]
    elif "/" in base:
        parts = base.split("/")
        if len(parts) == 2:
            base = parts[0]
            quote_str = parts[1].upper()
    elif "-" in base:
        parts = base.split("-")
        if len(parts) == 2:
            base = parts[0]
            quote_str = parts[1].upper()

    # Validate lengths and characters (must be 3 alphabetic letters each)
    if len(base) != 3 or len(quote_str) != 3 or not base.isalpha() or not quote_str.isalpha():
        return None

    if base == quote_str:
        return SearchResult(
            symbol=Symbol(root=base),
            name=base,
            currency=CurrencyCode(Currency(base)),
            asset_class=AssetClass.CASH,
            instrument_type=InstrumentType.CASH,
        )

    # 1. Target is USD (e.g. EUR in USD -> EURUSD=X)
    if quote_str == "USD":
        ticker = f"{base}USD=X"

    # 2. Symbol is USD (e.g. USD in EUR -> EUR=X)
    # Yahoo convention: "{CURRENCY}=X" usually means "USD/{CURRENCY}" (Price of USD in CURRENCY)
    elif base == "USD":
        ticker = f"{quote_str}=X"

    # 3. Cross Rates (e.g. EUR in JPY -> EURJPY=X)
    else:
        ticker = f"{base}{quote_str}=X"

    if verify:
        # Avoid circular import if fetch_metadata moved, but here they are in same file
        if not fetch_metadata(ticker, provider=ProviderName.YAHOO):
            return None

    return SearchResult(
        provider=ProviderName.YAHOO,
        symbol=Symbol(root=ticker),
        name=base,
        currency=CurrencyCode(Currency(quote_str)),
        asset_class=AssetClass.CASH,
        instrument_type=InstrumentType.CASH,
    )


def resolve_security(
    criteria: SecurityQuery,
    verify: bool = False,
    registry: InstrumentLookup | None = None,
    include_price: bool = False,
) -> SearchResult | None:
    """
    Unified security resolution routine.
    1. Check Registry (Strict fields)
    2. Try Programmatic Currency Resolution (if it looks like a pair)
    3. Perform Online Search (ISIN then Symbol)
    """
    # 1. Registry Match (Final Truth)
    if registry:
        candidates = registry.find_candidates(criteria)
        if candidates:
            cand = candidates[0]
            # Convert Instrument to SearchResult
            # Determine the best ticker and source from registry + derivation
            best_ticker = None
            source = ProviderName.YAHOO

            if cand.tickers:
                if cand.tickers.yahoo:
                    best_ticker = cand.tickers.yahoo
                    source = ProviderName.YAHOO
                elif cand.tickers.ft:
                    best_ticker = cand.tickers.ft
                    source = ProviderName.FT
                elif cand.tickers.google:
                    best_ticker = cand.tickers.google
                    source = ProviderName.GOOGLE

            if not best_ticker:
                # Better way to bypass hardcoding: Derive ticker if missing from registry
                best_ticker = derive_provider_ticker(
                    cand.symbol, cand.asset_class, ProviderName.YAHOO
                )
                source = ProviderName.YAHOO

            # Optional: Fetch price (current or historical)
            price = None
            price_date = None
            if best_ticker and include_price:
                target_date = criteria.price_on[0].date if criteria.price_on else None
                price = fetch_price(best_ticker, provider=source, date=target_date)
                price_date = target_date or date.today()

            return SearchResult(
                provider=source,
                symbol=Symbol(root=best_ticker) if best_ticker else Symbol(root=cand.symbol),
                name=cand.name or cand.symbol,
                currency=cand.currency,
                asset_class=cand.asset_class,
                instrument_type=cand.instrument_type,
                price=price,
                price_date=price_date,
                country=cand.country,
                isin=str(cand.isin) if cand.isin else None,
                figi=cand.figi,
                metadata=cand.metadata,
            )

    # 2. Programmatic FX Resolution
    # Only perform FX resolution if the user is searching for CASH/FOREX
    # or hasn't specified an asset class. Symbols like 'TRX' should be resolved
    # via online search if looking for Crypto.
    if criteria.symbol:
        from .models import AssetClass

        is_cash_search = (
            criteria.asset_class is None
            or _map_asset_class(criteria.asset_class) == AssetClass.CASH
        )
        if is_cash_search:
            fx_res = resolve_currency(
                str(criteria.symbol), target_currency=criteria.currency, verify=verify
            )
            if fx_res:
                if include_price:
                    target_date = criteria.price_on[0].date if criteria.price_on else None
                    fx_res.price = fetch_price(
                        fx_res.symbol,
                        provider=fx_res.provider or ProviderName.YAHOO,
                        date=target_date,
                    )
                    fx_res.price_date = target_date or date.today()
                return fx_res

    # 3. Online Search
    results = search_isin(criteria)
    if results:
        res = results[0]
        # Fetch price (current or historical)
        if include_price:
            target_date = criteria.price_on[0].date if criteria.price_on else None
            res.price = fetch_price(
                res.symbol, provider=res.provider or ProviderName.YAHOO, date=target_date
            )
            res.price_date = target_date or date.today()
        return res

    return None


def resolve_and_persist(
    criteria: SecurityQuery,
    registry: InstrumentRegistry | None = None,
    store: bool = True,
    target_path: Path | None = None,
    dry_run: bool = False,
    include_price: bool = False,
) -> tuple[SearchResult, Instrument | None] | None:
    """
    High-level resolution workflow:
    1. Check Registry (Local)
    2. Online Search + Validation
    3. Auto-Save to Registry (if new and store=True)

    Returns (SearchResult, Instrument | None) or None.
    The Instrument is set for new discoveries (including dry-run); None for registry hits
    (caller retrieves the Instrument directly from the registry).
    """
    # 1. Resolve (logic encapsulated in resolve_security)
    # verify=True ensures online results are validated (price check)
    res = resolve_security(criteria, verify=True, registry=registry, include_price=include_price)

    if not res:
        return None

    new_instrument: Instrument | None = None

    # 2. Check if we need to persist it
    if store and registry:
        # Check if known using a stricter criteria
        known_candidates = registry.find_candidates(criteria)
        logger.debug(
            f"Persistence check: found {len(known_candidates)} candidates "
            f"for {criteria.isin or criteria.symbol}"
        )
        if not known_candidates:
            # It's a new discovery!
            logger.info(f"Persisting new discovery: {res.symbol} ({res.name})")

            from .registry import add_instrument

            inst_type, asset_class = _infer_types(res.asset_class)

            # Do not persist CASH/CURRENCY to file
            if inst_type == InstrumentType.CASH or asset_class == AssetClass.CASH:
                logger.debug(f"Skipping persistence for {res.name} as it is CASH/CURRENCY.")
                return res, None

            # Determine target path
            if target_path:
                final_target_path = Path(target_path).expanduser()
            else:
                # Default logic
                import os

                import platformdirs

                env_path = os.getenv("INSTRUMENT_REGISTRY_PATH")
                if env_path:
                    final_target_path = Path(env_path).expanduser()
                else:
                    final_target_path = Path(platformdirs.user_data_dir("instrument-registry"))

            new_instrument = add_instrument(
                criteria=criteria,
                metadata=res,
                target_path=final_target_path,
                instrument_type=inst_type,
                asset_class=asset_class,
                dry_run=dry_run,
            )

            if not dry_run:
                registry.reload(final_target_path)

    return res, new_instrument


def fetch_price(
    symbol: Symbol.Input, provider: ProviderName = ProviderName.YAHOO, date: date | None = None
) -> Price | None:
    """
    Fetches the price for a ticker (current or historical).
    """
    data_provider = get_data_provider(provider)
    # Boundary conversion
    symbol_vo = Symbol(symbol) if not isinstance(symbol, Symbol) else symbol
    try:
        val = data_provider.get_price(symbol_vo, date=date)
    except ValueError:
        return None
    if val is None:
        return None
    # Support both float (from Protocol) and Price (from actual implementations)
    if isinstance(val, Price):
        return val
    return Price(val)
