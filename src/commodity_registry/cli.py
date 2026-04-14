from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Annotated, Any, Literal

import pandas as pd  # type: ignore[import-untyped]
from pydantic import (
    AfterValidator,
    BaseModel,
    Field,
)
from pydantic_market_data.cli_models import (
    CLASS,
    CURR,
    DATE,
    NAME,
    PATHS,
    PRICE,
    SYMBOL,
    GlobalArgs,
    PatchedCliSettingsSource,
)
from pydantic_market_data.cli_models import (
    ISIN as ISIN_HELP,
)
from pydantic_market_data.models import (
    ISIN,
    Currency,
    CurrencyCode,
    Price,
    PriceVerificationError,
    SecurityCriteria,
)
from pydantic_settings import (
    BaseSettings,
    CliApp,
    CliPositionalArg,
    CliSubCommand,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from .models import AssetClass, InstrumentType
from .registry import get_registry

logger = logging.getLogger(__name__)


def setup_logging(v: bool, vv: bool):
    """Set up logging based on v (INFO) and vv (DEBUG) flags."""
    if vv:
        level = logging.DEBUG
        verbosity = 2
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    elif v:
        level = logging.INFO
        verbosity = 1
        fmt = "[%(levelname)s] %(name)s: %(message)s"
    else:
        level = logging.WARNING
        verbosity = 0
        fmt = "%(message)s"

    # Configure root logger
    logging.basicConfig(
        level=level, format=fmt, datefmt="%Y-%m-%d %H:%M:%S", stream=sys.stderr, force=True
    )

    # If verbosity is low (WARNING/INFO), silence noisy libraries.
    if verbosity < 2:
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("py_ibkr").setLevel(logging.WARNING)


class CommonArgs(GlobalArgs, BaseModel):
    model_config = SettingsConfigDict(
        populate_by_name=True,
        cli_kebab_case=True,
        cli_implicit_flags="toggle",
        cli_hide_none_type=True,
        extra="forbid",
    )
    registry_path: list[PATHS] = Field(
        default=[],
        description="Comma-separated paths to your user registry files or directories",
    )
    bundled: bool = Field(True, description="Exclude the bundled database")

    def get_registry(self):
        extra_paths = []
        for p_str in self.registry_path:
            p_obj = Path(p_str).expanduser()
            if p_obj.exists():
                extra_paths.append(p_obj)
        return get_registry(include_bundled=self.bundled, extra_paths=extra_paths)


def _validate_token_not_subcommand(v: str | None) -> str | None:
    if v in {"resolve", "lint", "add", "fetch"}:
        raise ValueError(f"'{v}' is a subcommand name, not a token")
    return v


Token = Annotated[str, AfterValidator(_validate_token_not_subcommand)]
OptionalToken = Annotated[str | None, AfterValidator(_validate_token_not_subcommand)]


def _is_isin(token: str) -> bool:
    """Returns True if token looks like an ISIN (12 alphanumeric chars, 2-letter country prefix)."""
    upper = token.upper()
    return len(token) == 12 and upper[:2].isalpha() and upper.isalnum()


def _is_ibkr_conid(token: str) -> bool:
    """Returns True if token is a numeric string (IBKR conid)."""
    return token.isdigit()


class resolve(CommonArgs):
    command: Literal["resolve"] = "resolve"
    token: CliPositionalArg[Token]
    provider: NAME | None = Field(None, description="Filter candidates by provider name")
    currency: CURR | None = Field(None, description="Filter candidates by currency code")
    date: DATE | None = Field(None, description="Validation date for price matching")
    price: PRICE | None = Field(None, description="Validation price for matching")
    dry_run: bool = Field(False, description="Parse and resolve without persisting changes")
    report_price: bool = Field(False, description="Fetch and include current price in output")
    asset_class: CLASS | None = Field(
        None, description="Asset class (Equity, Stock, Crypto, ETF, Index, etc.)"
    )

    def cli_cmd(self) -> None:
        from .finder import get_available_providers, resolve_and_persist, verify_ticker

        logger.info(f"Resolving token: {self.token}")
        reg = self.get_registry()

        # 1. Check for IBKR conid (pure numeric)
        if _is_ibkr_conid(self.token):
            logger.info(
                f"Numeric token detected. Checking registry for IBKR conid: {self.token}..."
            )
            # Numeric tokens are matched directly against IBKR ticker index
            res_comp = reg.find_by_ticker("IBKR", self.token)
            if res_comp:
                # Mock a search result for CLI display
                from .interfaces import ProviderName, SearchResult

                res = SearchResult(
                    provider=ProviderName.YAHOO,  # Fallback provider
                    ticker=res_comp.tickers.yahoo if res_comp.tickers else res_comp.name,
                    name=res_comp.name,
                    currency=res_comp.currency,
                    asset_class=res_comp.asset_class,
                    instrument_type=res_comp.instrument_type,
                    country=res_comp.country,
                )
                print(
                    f"Resolved (via IBKR conid): {res.name} -> "
                    f"{str(res.ticker)} (IBKR:{self.token})"
                )
                return

        # 2. Build Standard Criteria
        isin = self.token if _is_isin(self.token) else None
        symbol = self.token if not isin else None
        criteria = SecurityCriteria(
            isin=isin,
            symbol=symbol,
            currency=CurrencyCode(Currency(self.currency.upper())) if self.currency else None,
            target_price=Price(self.price) if self.price else None,
            target_date=pd.to_datetime(self.date).date() if self.date else None,
            asset_class=self.asset_class,
        )

        # By default, use the first registry path as the target for new discoveries
        target_path = Path(self.registry_path[0]).expanduser() if self.registry_path else None

        res = resolve_and_persist(
            criteria,
            registry=reg,
            store=True,
            target_path=target_path,
            dry_run=self.dry_run,
            include_price=self.report_price,
        )

        if not res:
            providers = get_available_providers()

            if not providers:
                logger.error(
                    f"Could not resolve '{self.token}'. "
                    "Install providers: uv tool install 'commodity-registry[providers]'"
                )
                sys.exit(1)

            logger.error(f"Could not resolve '{self.token}'")
            sys.exit(1)

        # Verification (if requested)
        if self.date and self.price:
            logger.info(f"Verifying price {self.price} on {self.date}...")
            v_date = pd.to_datetime(self.date).date()
            try:
                if verify_ticker(res.ticker, v_date, self.price, provider=res.provider):
                    print(f"  [OK] Verified {res.name} via {res.provider.upper()} ({res.ticker})")
                else:
                    logger.error(
                        f"  [!] FAILED: Price {self.price} on {self.date} "
                        f"does not match {res.ticker}"
                    )
                    sys.exit(1)
            except PriceVerificationError as e:
                logger.error(f"  [!] FAILED: {e}")
                sys.exit(1)

        if self.format == "json":
            print(res.model_dump_json(indent=2))
        else:
            p_val = getattr(res.price, "value", res.price) if res.price else 0.0
            p_label = f"Price {res.price_date}" if res.price_date else "Last Price"
            price_str = f" [{p_label}: {p_val:.2f} {res.currency}]" if res.price else ""

            country_str = f" | Country: {res.country}" if res.country else ""
            meta_str = f" | Meta: {res.metadata}" if res.metadata else ""

            msg = (
                f"Resolved: {res.name} -> {str(res.ticker)} "
                f"({res.provider.value}){price_str}{country_str}{meta_str}"
            )
            print(msg)


class lint(CommonArgs):
    command: Literal["lint"] = "lint"
    path: str | None = Field(None, description="Specific file or directory to lint")
    verify: bool = Field(False, description="Perform live verification against online sources")
    only: str | None = Field(None, description="Verify only a specific commodity name")

    def cli_cmd(self) -> None:
        from .finder import fetch_metadata, verify_ticker
        from .interfaces import ProviderName

        if self.path:
            # Validate specific path
            path_obj = Path(self.path).expanduser()
            logger.info(f"Linting external path: {path_obj}")
            reg = get_registry(include_bundled=False, extra_paths=[path_obj])
        else:
            # Validate configured registry
            reg = self.get_registry()
            logger.info(f"Linting registry ({len(reg.get_all())} commodities)...")

        # Validation already happened during loading via Pydantic.
        errors = reg.load_errors.copy()
        warnings = []

        # Check 1: Unique ISIN + Currency pairs
        seen_isinc: dict[tuple[str, str], str] = {}  # (isin, currency) -> name
        for c in reg.get_all():
            if c.isin:
                key = (str(c.isin).upper(), str(c.currency).upper())
                if key in seen_isinc:
                    errors.append(
                        f"Duplicate ISIN {c.isin} with currency {c.currency} in {c.name} "
                        f"and {seen_isinc[key]}"
                    )
                seen_isinc[key] = c.name

        # Check 2: Live Verification
        if self.verify:
            targets = reg.get_all()
            if self.only:
                targets = [c for c in targets if c.name == self.only]
                if not targets:
                    logger.error(f"Commodity '{self.only}' not found.")
                    sys.exit(1)

            print(f"\n=== Granular Data Audit (Live) - {len(targets)} items ===")
            for c in targets:
                ticker = None
                provider = ProviderName.YAHOO

                if c.tickers:
                    if c.tickers.yahoo:
                        ticker = c.tickers.yahoo
                        provider = ProviderName.YAHOO
                    elif c.tickers.ft:
                        ticker = c.tickers.ft
                        provider = ProviderName.FT
                    elif c.tickers.google:
                        ticker = c.tickers.google
                        provider = ProviderName.GOOGLE

                if ticker:
                    audit_log = []
                    success = True

                    # Fetch metadata
                    logger.debug(f"Fetching live metadata for {ticker} via {provider}...")
                    ext_data = fetch_metadata(ticker, provider=provider)

                    if not ext_data:
                        audit_log.append(f"  [!] FAILED: No external data found for {ticker}")
                        warnings.append(f"{c.name}: No external metadata found")
                        success = False
                    else:
                        # ISIN Check
                        # SearchResult doesn't currently store ISIN directly
                        # but if it does later
                        ext_isin = ext_data.isin if hasattr(ext_data, "isin") else None
                        if c.isin and ext_isin:
                            if str(c.isin).upper() != ext_isin.upper():
                                audit_log.append(f"  ISIN:     {c.isin} [MISMATCH: {ext_isin}]")
                                warnings.append(
                                    f"{c.name}: ISIN mismatch (Registry: {c.isin}, "
                                    f"Provider: {ext_isin})"
                                )
                                success = False
                            else:
                                audit_log.append(f"  ISIN:     {c.isin} [OK]")
                        else:
                            audit_log.append(
                                f"  ISIN:     {c.isin or 'N/A'} (Provider: {ext_isin or 'N/A'})"
                            )

                        # Ticker Check
                        ext_ticker = ext_data.ticker
                        if ticker and ext_ticker:
                            if ticker.upper() != str(ext_ticker).upper():
                                audit_log.append(f"  Ticker:   {ticker} [MISMATCH: {ext_ticker}]")
                                warnings.append(
                                    f"{c.name}: Ticker mismatch (Registry: {ticker}, "
                                    f"Provider: {ext_ticker})"
                                )
                                success = False
                            else:
                                audit_log.append(f"  Ticker:   {ticker} [OK]")
                        else:
                            audit_log.append(
                                f"  Ticker:   {ticker or 'N/A'} (Provider: {ext_ticker or 'N/A'})"
                            )

                        # Currency Check
                        ext_curr = (
                            str(ext_data.currency.root)
                            if (ext_data.currency and hasattr(ext_data.currency, "root"))
                            else str(ext_data.currency)
                            if ext_data.currency
                            else None
                        )
                        status_curr = (
                            "[OK]"
                            if (
                                ext_curr
                                and c.currency
                                and ext_curr.upper() == str(c.currency).upper()
                            )
                            else f"[MISMATCH: {ext_curr}]"
                        )
                        audit_log.append(f"  Currency: {c.currency} {status_curr}")
                        if "MISMATCH" in status_curr:
                            warnings.append(f"{c.name}: Currency mismatch")
                            success = False

                        if c.figi:
                            audit_log.append(f"  FIGI:     {c.figi}")

                        # Historical Price Verification Points
                        if c.validation_points:
                            audit_log.append("  Historical Verification:")
                            for vp in c.validation_points:
                                verified_count = 0
                                vp_log = [f"    - {vp.date} (Target: {vp.price}):"]

                                providers_to_check = []
                                if c.tickers:
                                    if c.tickers.yahoo:
                                        providers_to_check.append(
                                            (ProviderName.YAHOO, c.tickers.yahoo)
                                        )
                                    if c.tickers.ft:
                                        providers_to_check.append((ProviderName.FT, c.tickers.ft))
                                    if c.tickers.google:
                                        providers_to_check.append(
                                            (ProviderName.GOOGLE, c.tickers.google)
                                        )

                                if not providers_to_check:
                                    vp_log.append("      [SKIPPED: No ticker found]")
                                    audit_log.extend(vp_log)
                                    continue

                                for p_name, t_val in providers_to_check:
                                    v_price = (
                                        Price(vp.price)
                                        if isinstance(vp.price, (float, int))
                                        else vp.price
                                    )
                                    try:
                                        if verify_ticker(t_val, vp.date, v_price, provider=p_name):
                                            vp_log.append(
                                                f"      * {p_name.upper()}: [OK: Range Match]"
                                            )
                                            verified_count += 1
                                        else:
                                            vp_log.append(f"      * {p_name.upper()}: [FAILED]")
                                    except PriceVerificationError as e:
                                        vp_log.append(f"      * {p_name.upper()}: [FAILED: {e}]")

                                if verified_count == 0:
                                    success = False
                                    audit_log.extend(vp_log)
                                    warnings.append(
                                        f"{c.name}: Price verification failed on {vp.date}"
                                    )
                                else:
                                    audit_log.extend(vp_log)

                        else:
                            audit_log.append(
                                "  Historical Verification: [SKIPPED: No validation points]"
                            )

                    # Print summary one-liner
                    status_lbl = "OK" if success else "FAILED"
                    print(f"{c.name}({provider.value} {ticker}): {status_lbl}")
                    if not success or self.vv:
                        for line in audit_log:
                            print(line)

                else:
                    if not c.tickers or not (c.tickers.yahoo or c.tickers.ft or c.tickers.google):
                        print(f"{c.name}: [SKIPPED: No compatible ticker]")
                        continue

        if errors:
            for err in errors:
                logger.error(f"Validation Error: {err}")
            sys.exit(1)

        if warnings:
            for w in warnings:
                logger.warning(f"Validation Warning: {w}")

        logger.info("All checks passed.")


class add(CommonArgs):
    command: Literal["add"] = "add"
    token: CliPositionalArg[OptionalToken] = None
    name: NAME | None = Field(None, description="Canonical name of the commodity")
    isin: ISIN_HELP | None = Field(None, description="ISIN code")
    ticker: SYMBOL | None = Field(None, description="Ticker symbol")
    instrument_type: InstrumentType | None = Field(
        None, description="Categorization (e.g. ETF, Future)"
    )
    asset_class: AssetClass | None = Field(
        None, description="Asset class (Equity, Commodity, etc.)"
    )
    currency: CURR | None = Field(None, description="Primary currency")
    figi: str | None = Field(None, description="FIGI identifier")
    ibkr: int | None = Field(None, description="Interactive Brokers conid")
    country: str | None = Field(None, description="Country or region of origin")
    validation_date: DATE | None = Field(None, description="Date for initial verification")
    validation_price: PRICE | None = Field(None, description="Price for initial verification")
    fetch_meta: bool = Field(
        False, alias="fetch", description="Fetch missing metadata from online sources"
    )
    dry_run: bool = Field(False, description="Preview changes without writing to file")

    def cli_cmd(self) -> None:
        from .finder import search_isin
        from .registry import add_commodity

        # Resolve Target Path
        target_path = Path(self.registry_path[0]).expanduser()
        if target_path.is_dir():
            target_path = target_path / "manual.yaml"

        isin: Any = self.isin
        ticker = self.ticker
        if self.token:
            if _is_isin(self.token):
                if not isin:
                    isin = ISIN(self.token)
            else:
                if not ticker:
                    ticker = SYMBOL(self.token)

        name = self.name
        currency = self.currency

        reg = self.get_registry()
        existing_entry = reg.find_by_isin(isin, currency) if (isin and currency) else None

        if existing_entry and not name:
            logger.info(
                f"Using existing name '{existing_entry.name}' for instrument {isin}/{currency}"
            )
            name = existing_entry.name

        criteria = SecurityCriteria(
            isin=isin,
            symbol=ticker,
            currency=CurrencyCode(Currency(currency.upper())) if currency else None,
            target_price=Price(self.validation_price) if self.validation_price else None,
            target_date=(
                pd.to_datetime(self.validation_date).date() if self.validation_date else None
            ),
        )

        metadata = None
        if self.fetch_meta:
            logger.info("Searching for metadata...")
            results = search_isin(criteria)
            if results:
                metadata = results[0]
                logger.info(
                    f"Found candidate: {metadata.ticker.root} "
                    f"({metadata.provider.value}) - {metadata.name}"
                )
                if not criteria.symbol:
                    criteria.symbol = str(metadata.ticker)
                if not name:
                    name = metadata.name
                if not currency:
                    currency = metadata.currency.root if metadata.currency else None
                    criteria.currency = metadata.currency
            else:
                logger.warning("No online metadata found.")
                if not ticker and not isin:
                    logger.error("No ticker or ISIN provided and no online match found.")
                    sys.exit(1)

        missing = []
        if not isin and not ticker:
            missing.append("--isin or --ticker")
        if not self.instrument_type:
            missing.append("--instrument-type")
        if not self.asset_class:
            missing.append("--asset-class")
        if not currency:
            missing.append("--currency")

        if missing:
            logger.error(f"Missing required fields: {', '.join(missing)}")
            sys.exit(1)

        inst_type = InstrumentType(self.instrument_type)
        asset_class_val = AssetClass(self.asset_class)

        if not criteria.symbol and ticker:
            criteria.symbol = ticker

        try:
            commodity = add_commodity(
                criteria=criteria,
                metadata=metadata,
                target_path=target_path,
                instrument_type=inst_type,
                asset_class=asset_class_val,
                name=name,
                dry_run=self.dry_run,
                registry=reg,
                country=self.country,
                ibkr=self.ibkr,
            )
            print(f"Successfully processed {commodity.name}")
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)


class fetch(CommonArgs):
    command: Literal["fetch"] = "fetch"
    isin: ISIN_HELP | None = Field(None, description="ISIN to fetch")
    ticker: SYMBOL | None = Field(None, description="Ticker to fetch")
    price_info: bool = Field(False, alias="price", description="Also fetch the latest price")

    def cli_cmd(self) -> None:
        from .finder import fetch_price, resolve_security

        logger.info(f"Fetching details for ISIN={self.isin}, Ticker={self.ticker}")
        criteria = SecurityCriteria(isin=self.isin, symbol=self.ticker)
        res = resolve_security(criteria, verify=True)

        if not res:
            logger.warning("No results found.")
            return

        if self.format == "json":
            print(res.model_dump_json(indent=2))
        else:
            print(f"\nFound Details ({res.provider.value.upper()}):")
            print(f"  Ticker:   {str(res.ticker)}")
            print(f"  Name:     {res.name}")
            print(f"  Currency: {res.currency if res.currency else 'None'}")

            if self.price_info:
                p = fetch_price(res.ticker, provider=res.provider)
                if p:
                    print(f"  Price:    {p}")
                else:
                    print("  Price:    [Unavailable]")


class AppCLI(BaseSettings, GlobalArgs):
    """Commodity Registry CLI Application"""

    model_config = SettingsConfigDict(
        cli_parse_args=True,
        cli_kebab_case=True,
        cli_implicit_flags="toggle",
        cli_hide_none_type=True,
    )

    registry_path: list[PATHS] = Field(
        default=[PATHS("data/commodities/")],
        description="Comma-separated paths to your user registry files or directories",
    )
    bundled: bool = Field(True, description="Exclude the bundled database")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
            PatchedCliSettingsSource(settings_cls),
        )

    subcommand: CliSubCommand[resolve | fetch | lint | add]

    def get_registry(self):
        extra_paths = []
        for p_str in self.registry_path:
            p_obj = Path(p_str).expanduser()
            if p_obj.exists():
                extra_paths.append(p_obj)
        return get_registry(include_bundled=self.bundled, extra_paths=extra_paths)

    def cli_cmd(self) -> None:
        # Propagate v/vv and format flags from subcommand if root doesn't have them set
        v = self.v or getattr(self.subcommand, "v", False)
        vv = self.vv or getattr(self.subcommand, "vv", False)
        format_val = getattr(self.subcommand, "format", self.format) or self.format

        # Propagate registry_path and bundled to subcommand
        if hasattr(self.subcommand, "registry_path") and not self.subcommand.registry_path:
            self.subcommand.registry_path = self.registry_path
        if hasattr(self.subcommand, "bundled"):
            self.subcommand.bundled = self.bundled

        # Ensure the subcommand instance has the propagated values
        for attr, val in [("v", v), ("vv", vv), ("format", format_val)]:
            if hasattr(self.subcommand, attr):
                setattr(self.subcommand, attr, val)

        setup_logging(v, vv)

        # Explicitly run the correct subcommand to avoid leakage
        if self.subcommand is not None and hasattr(self.subcommand, "cli_cmd"):
            self.subcommand.cli_cmd()
        else:
            CliApp.run_subcommand(self)


def main():
    CliApp.run(AppCLI)


if __name__ == "__main__":
    main()
