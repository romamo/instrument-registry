import logging
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from pydantic_market_data.models import Price, PriceVerificationError
from treaty import Ctx, Exit, Flag, Out, ParseError

from ..interfaces import ProviderName
from ..models import Instrument
from . import common
from .treaty_app import RegistryScope, app, raise_missing_provider, require_live_providers

logger = logging.getLogger(__name__)
COMMAND_NAME = "instrument-reg lint --verify"


def _provider_pairs(instrument: Instrument) -> list[tuple[ProviderName, str]]:
    pairs: list[tuple[ProviderName, str]] = []
    if not instrument.tickers:
        return pairs
    if instrument.tickers.yahoo:
        pairs.append((ProviderName.YAHOO, instrument.tickers.yahoo))
    if instrument.tickers.ft:
        pairs.append((ProviderName.FT, instrument.tickers.ft))
    return pairs


@dataclass(frozen=True, slots=True)
class LintArgs(RegistryScope):
    path: Path | None = Flag(
        default=None, description="Lint only this registry file or directory, without bundled data"
    )
    verify: bool = Flag(default=False, description="Also check each instrument against providers")
    only: str | None = Flag(default=None, description="Verify only the instrument with this symbol")

    def __post_init__(self) -> None:
        if self.only is not None and not self.verify:
            raise ParseError("--only selects what --verify checks; pass --verify too")


class VerifyStatus(StrEnum):
    OK = "ok"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class Verification:
    """One instrument checked against its primary provider"""

    symbol: str
    provider: ProviderName | None
    ticker: str | None
    status: VerifyStatus
    details: list[str] = Out(ordered=True, external=True)


@dataclass(frozen=True, slots=True, kw_only=True)
class LintReport:
    target: str
    instrument_count: int
    checked: list[str] = Out(ordered=True)
    error_count: int
    warning_count: int
    errors: list[str] = Out(ordered=True)
    warnings: list[str] = Out(ordered=True)
    verified: bool
    verifications: list[Verification] = Out(ordered=True)


def _verify(instrument: Instrument, warnings: list[str]) -> Verification:
    """Compare one instrument with live provider data, appending what differs to warnings"""
    from ..finder import fetch_metadata, verify_ticker

    pairs = _provider_pairs(instrument)
    if not pairs:
        return Verification(
            instrument.symbol, None, None, VerifyStatus.SKIPPED, ["No compatible ticker"]
        )
    provider, ticker = pairs[0]
    details: list[str] = []
    logger.debug("Fetching live metadata for %s via %s...", ticker, provider)
    try:
        ext_data = fetch_metadata(ticker, provider=provider)
    except ImportError:
        raise_missing_provider(provider, COMMAND_NAME)

    if not ext_data:
        warnings.append(f"{instrument.symbol}: No external metadata found")
        details.append(f"No external data found for {ticker}")
        return Verification(instrument.symbol, provider, ticker, VerifyStatus.FAILED, details)

    success = True
    ext_isin = ext_data.isin
    if instrument.isin and ext_isin:
        if str(instrument.isin).upper() != ext_isin.upper():
            details.append(f"ISIN: {instrument.isin} [MISMATCH: {ext_isin}]")
            warnings.append(
                f"{instrument.symbol}: ISIN mismatch (Registry: {instrument.isin}, "
                f"Provider: {ext_isin})"
            )
            success = False
        else:
            details.append(f"ISIN: {instrument.isin} [OK]")
    else:
        details.append(f"ISIN: {instrument.isin or 'N/A'} (Provider: {ext_isin or 'N/A'})")

    ext_symbol = ext_data.symbol
    if ext_symbol:
        if ticker.upper() != str(ext_symbol).upper():
            details.append(f"Ticker: {ticker} [MISMATCH: {ext_symbol}]")
            warnings.append(
                f"{instrument.symbol}: Ticker mismatch (Registry: {ticker}, Provider: {ext_symbol})"
            )
            success = False
        else:
            details.append(f"Ticker: {ticker} [OK]")
    else:
        details.append(f"Ticker: {ticker} (Provider: N/A)")

    ext_curr = str(ext_data.currency) if ext_data.currency else None
    if ext_curr and ext_curr.upper() == str(instrument.currency).upper():
        details.append(f"Currency: {instrument.currency} [OK]")
    else:
        details.append(f"Currency: {instrument.currency} [MISMATCH: {ext_curr}]")
        warnings.append(f"{instrument.symbol}: Currency mismatch")
        success = False

    if instrument.figi:
        details.append(f"FIGI: {instrument.figi}")

    if not instrument.validation_points:
        details.append("Historical Verification: [SKIPPED: No validation points]")
    for point in instrument.validation_points or []:
        verified_count = 0
        for provider_name, ticker_value in pairs:
            price = Price(point.price) if isinstance(point.price, (float, int)) else point.price
            label = f"{point.date} (Target: {point.price}) {provider_name.upper()}"
            try:
                if verify_ticker(ticker_value, point.date, price, provider=provider_name):
                    details.append(f"{label}: [OK: Range Match]")
                    verified_count += 1
                else:
                    details.append(f"{label}: [FAILED]")
            except ImportError:
                raise_missing_provider(provider_name, COMMAND_NAME)
            except PriceVerificationError as exc:
                details.append(f"{label}: [FAILED: {exc}]")
        if verified_count == 0:
            success = False
            warnings.append(f"{instrument.symbol}: Price verification failed on {point.date}")

    status = VerifyStatus.OK if success else VerifyStatus.FAILED
    return Verification(instrument.symbol, provider, ticker, status, details)


@app.command(
    "lint",
    description="Validate registry files and optionally verify live provider data",
    danger_level="safe",
    has_network_io=True,
    timeout=900,
    exit_codes=["NOT_FOUND", "MISSING_PROVIDER", "LINT_FAILED"],
    examples=[
        ("Lint the bundled and user registries", "instrument-reg lint"),
        ("Lint one file", "instrument-reg lint --path ~/registry/manual.yaml"),
        ("Verify one instrument against providers", "instrument-reg lint --verify --only AAPL"),
    ],
)
def lint(args: LintArgs, ctx: Ctx) -> LintReport:
    if args.path is not None:
        reg = common.get_registry(include_bundled=False, extra_paths=[args.path.expanduser()])
        target_desc = str(args.path)
    else:
        reg = common.open_registry(args.registry_paths(), bundled=not args.no_bundled)
        target_desc = "registry"

    instruments = reg.get_all()
    errors = list(reg.load_errors)
    warnings: list[str] = []

    seen_isinc: dict[tuple[str, str], str] = {}
    for instrument in instruments:
        instrument_ok = True
        if instrument.isin:
            key = (str(instrument.isin).upper(), str(instrument.currency).upper())
            if key in seen_isinc:
                errors.append(
                    f"Duplicate ISIN {instrument.isin} with currency {instrument.currency} in "
                    f"{instrument.symbol} and {seen_isinc[key]}"
                )
                instrument_ok = False
            seen_isinc[key] = instrument.symbol
        status = "OK" if instrument_ok else "FAILED"
        ctx.debug("instrument checked", symbol=instrument.symbol, status=status)

    verifications: list[Verification] = []
    if args.verify:
        targets = instruments
        if args.only is not None:
            targets = [instrument for instrument in targets if instrument.symbol == args.only]
            if not targets:
                raise Exit.NOT_FOUND(
                    f"Instrument '{args.only}' not found.",
                    context={"only": args.only},
                    suggestion="pass a symbol listed in data.checked of a plain lint run",
                )
        primary = next((pairs[0][0] for t in targets if (pairs := _provider_pairs(t))), None)
        require_live_providers(primary, COMMAND_NAME)
        for done, instrument in enumerate(targets, start=1):
            ctx.progress(f"verifying {instrument.symbol}", done=done, total=len(targets))
            verifications.append(_verify(instrument, warnings))

    report = LintReport(
        target=target_desc,
        instrument_count=len(instruments),
        checked=[instrument.symbol for instrument in instruments],
        error_count=len(errors),
        warning_count=len(warnings),
        errors=errors,
        warnings=warnings,
        verified=args.verify,
        verifications=verifications,
    )
    if errors:
        raise Exit.LINT_FAILED(
            f"{len(errors)} registry error(s) found.", context={"target": target_desc}, data=report
        )
    return report
