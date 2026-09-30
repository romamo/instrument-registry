import logging
import os
from pathlib import Path
from typing import Any

import yaml
from pydantic_market_data.models import Currency, SecurityQuery, Symbol

from .interfaces import ProviderName, SearchResult
from .models import AssetClass, Instrument, InstrumentFile, InstrumentType, _map_asset_class
from .resources import get_instrument_files

logger = logging.getLogger(__name__)


class StrictSafeLoader(yaml.SafeLoader):
    """YAML Loader that disallows duplicate keys."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
        seen_keys: set = set()
        for key_node, _value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in seen_keys:
                raise yaml.constructor.ConstructorError(
                    f"Duplicate key found in YAML: {key}", key_node.start_mark
                )
            seen_keys.add(key)
        return super().construct_mapping(node, deep=deep)


class InstrumentRegistry:
    def __init__(self, include_bundled: bool = True, extra_paths: list[Path] | None = None):
        self._instruments: list[Instrument] = []
        self._load_errors: list[str] = []
        self._by_isin: dict[str, list[Instrument]] = {}
        self._by_figi: dict[str, Instrument] = {}
        self._by_symbol: dict[str, list[Instrument]] = {}
        self._by_ticker: dict[str, Instrument] = {}  # Map "PROVIDER:TICKER" -> Instrument

        if include_bundled:
            self.load_bundled_data()

        if extra_paths:
            for p in extra_paths:
                self.load_path(p)

        self._rebuild_indices()

    def load_bundled_data(self):
        """Loads all instrument data from the bundled YAML files."""
        logger.debug("Loading bundled registry data...")
        for file_path in get_instrument_files():
            self._load_file(file_path)

    def reload(self, path: Path) -> None:
        """Load additional instrument data from path and rebuild lookup indices."""
        self.load_path(path)
        self._rebuild_indices()

    def load_path(self, path: Path):
        """Loads instrument data from a specific file or directory recursively."""
        logger.debug(f"Loading user registry path: {path}")
        if path.is_dir():
            for p in sorted(path.rglob("*")):
                if p.is_file() and p.suffix in (".yaml", ".yml"):
                    self._load_file(p)
        else:
            self._load_file(path)

    def _load_file(self, path: Path):
        if not path.exists():
            return
        logger.debug(f"Loading registry file: {path}")
        with open(path) as f:
            data = yaml.load(f, Loader=StrictSafeLoader)  # nosec B506

        if not data:
            return

        file_model = InstrumentFile(**data)
        self._instruments.extend(file_model.instruments)

    @property
    def load_errors(self) -> list[str]:
        return self._load_errors

    def _rebuild_indices(self):
        self._by_isin = {}
        for c in self._instruments:
            if c.isin:
                isin_key = str(c.isin).upper()
                if isin_key not in self._by_isin:
                    self._by_isin[isin_key] = []
                self._by_isin[isin_key].insert(0, c)  # User overrides first

        self._by_figi = {}
        self._by_symbol = {}
        for c in self._instruments:
            sym_key = c.symbol.upper()
            if sym_key not in self._by_symbol:
                self._by_symbol[sym_key] = []
            self._by_symbol[sym_key].insert(0, c)

        self._by_ticker = {}

        for c in self._instruments:
            if c.figi:
                self._by_figi[c.figi.upper()] = c

            if c.tickers:
                for provider, ticker in c.tickers.model_dump().items():
                    if ticker:
                        # Later items in self._instruments override earlier ones
                        self._by_ticker[f"{provider.upper()}:{str(ticker).upper()}"] = c

    def find_by_isin(self, isin: str, currency: Currency | None = None) -> Instrument | None:
        matches = self.find_candidates(SecurityQuery(isin=isin, currency=currency))
        return matches[0] if matches else None

    def find_candidates(self, criteria: SecurityQuery) -> list[Instrument]:
        """
        Match priority: FIGI (unique) → ISIN+currency → symbol (fallback only).

        A candidate must satisfy one of:
        - FIGI exact match (globally unique, returned immediately)
        - ISIN match AND currency match (when currency is provided)
        - ISIN match alone (when currency is not provided)
        - symbol/name match (only when neither ISIN nor FIGI is present)
        """
        # 1. FIGI — globally unique; short-circuit everything else
        if criteria.figi:
            figi_match = self._by_figi.get(str(criteria.figi).upper())
            if figi_match:
                return [figi_match]

        candidates: list[Instrument] = []

        # 2. ISIN + optional currency filter
        if criteria.isin:
            isin_matches = self._by_isin.get(str(criteria.isin).upper(), [])
            if criteria.currency:
                curr = str(criteria.currency).upper()
                isin_matches = [c for c in isin_matches if str(c.currency).upper() == curr]
            candidates.extend(isin_matches)

        # 3. Symbol — only when no strict identifier (ISIN/FIGI) was provided
        elif criteria.symbol:
            sym_matches = self._by_symbol.get(str(criteria.symbol).upper(), [])
            if criteria.currency:
                curr = str(criteria.currency).upper()
                sym_matches = [c for c in sym_matches if str(c.currency).upper() == curr]
            candidates.extend(sym_matches)

        # 4. Asset-class filter — only for symbol lookups; ISIN/FIGI are already unique identifiers
        if criteria.asset_class and candidates and not criteria.isin:
            target_ac = _map_asset_class(criteria.asset_class)
            if not target_ac:
                return []
            candidates = [c for c in candidates if _map_asset_class(c.asset_class) == target_ac]

        # Deduplicate by (symbol, asset_class)
        seen: set[tuple[str, object]] = set()
        unique: list[Instrument] = []
        for c in candidates:
            key = (c.symbol.upper(), c.asset_class)
            if key not in seen:
                unique.append(c)
                seen.add(key)
        return unique

    def find_by_ticker(self, provider: str, symbol: Symbol | str) -> Instrument | None:
        """
        Finds an instrument by provider-specific ticker.
        Example: find_by_ticker("yahoo", "4GLD.DE")
        """
        ticker_str = symbol.root if isinstance(symbol, Symbol) else symbol
        return self._by_ticker.get(f"{provider.upper()}:{ticker_str.upper()}")

    def get_all(self) -> list[Instrument]:
        return self._instruments


# Default registry if imported directly
def get_registry(
    include_bundled: bool = True, extra_paths: list[Path] | None = None
) -> InstrumentRegistry:
    if not extra_paths:
        env_path = os.getenv("INSTRUMENT_REGISTRY_PATH")
        if env_path:
            extra_paths = [Path(env_path).expanduser()]
        else:
            # Use platformdirs default
            import platformdirs

            default_dir = Path(platformdirs.user_data_dir("instrument-registry"))
            if default_dir.exists():
                extra_paths = [default_dir]

    return InstrumentRegistry(include_bundled=include_bundled, extra_paths=extra_paths)


def add_instrument(
    criteria: SecurityQuery,
    metadata: SearchResult | None,  # None if not found online
    target_path: Path,
    instrument_type: InstrumentType | None = None,
    asset_class: AssetClass | None = None,
    symbol: str | None = None,
    dry_run: bool = False,
    registry: InstrumentRegistry | None = None,
    country: str | None = None,
    ibkr: int | None = None,
) -> Instrument:
    """
    Adds a new instrument to the registry.

    Uses SecurityQuery.symbol (the raw token or security symbol).
    Extracts base ticker (before ':') for Beancount symbol.
    Stores provider-specific tickers only if found online.
    """
    if metadata:
        if instrument_type is None:
            instrument_type = metadata.instrument_type
        if asset_class is None:
            asset_class = metadata.asset_class

    # 1. Determine ticker
    if not criteria.symbol and not symbol:
        if not metadata or not metadata.symbol:
            raise ValueError("SecurityQuery.symbol or symbol is required if metadata fetch failed")
        criteria.symbol = str(metadata.symbol)

    # 2. Extract base ticker for instrument symbol
    if symbol:
        clean_symbol = symbol
    else:
        # Prefer the raw provider ticker (e.g. OpenFIGI's "CHIP" over Yahoo's "CHIP.PA")
        if metadata and metadata.ticker:
            token = str(metadata.ticker)
        elif metadata and metadata.symbol:
            token = str(metadata.symbol)
        else:
            token = str(criteria.symbol or "")
        if not token:
            raise ValueError("Could not determine ticker for symbol generation")

        # For CASH instruments, prefer use the name (e.g. "EUR") if it is a 3-letter code.
        is_fx = instrument_type == InstrumentType.CASH or asset_class == AssetClass.CASH
        if is_fx and metadata and metadata.name and len(str(metadata.name)) == 3:
            clean_symbol = str(metadata.name)
        else:
            clean_symbol = token.split(":")[-1]

    # 3. Collision check against entire registry
    if registry:
        existing = registry.find_candidates(SecurityQuery(symbol=clean_symbol))
        for match in existing:
            # If symbol matches but ISIN is different, it's a collision
            # unless one of them is missing ISIN (like CASH).
            # If both are same symbol, same asset class, same currency, we allow it (update).
            # If it's a symbol collision with a different asset class, we warn/error.
            if match.asset_class != asset_class:
                raise ValueError(
                    f"Symbol collision: '{clean_symbol}' already registered as "
                    f"{match.asset_class}. Refusing to add as {asset_class}."
                )

    # 4. Build tickers dict (prefer online metadata, fallback to criteria.symbol)
    tickers_dict: dict[str, Any] | None = None
    if metadata and metadata.symbol:
        tickers_dict = {metadata.provider.value: str(metadata.symbol)}
    elif criteria.symbol:
        symbol_str = str(criteria.symbol)
        # Fallback to yahoo for manual tickers unless it contains a provider prefix
        if ":" in symbol_str:
            parts = symbol_str.split(":", 1)
            tickers_dict = {parts[0].lower(): parts[1]}
        else:
            tickers_dict = {ProviderName.YAHOO.value: symbol_str}

    if ibkr:
        if not tickers_dict:
            tickers_dict = {}
        tickers_dict["ibkr"] = ibkr

    # 5. Create instrument
    if instrument_type is None:
        raise ValueError(
            "--instrument-type is required (could not be inferred from provider metadata)"
        )
    if asset_class is None:
        raise ValueError("--asset-class is required (could not be inferred from provider metadata)")

    from .models import Tickers, ValidationPoint

    comm_currency = (
        Currency(str(criteria.currency).upper())
        if criteria.currency
        else (metadata.currency if metadata and metadata.currency else Currency("USD"))
    )

    instrument = Instrument(
        symbol=clean_symbol,
        name=metadata.name if metadata else None,
        isin=criteria.isin,
        figi=metadata.figi if metadata else None,
        instrument_type=instrument_type,
        asset_class=asset_class,
        currency=comm_currency,
        tickers=Tickers(**tickers_dict) if tickers_dict else None,
        validation_points=(
            [ValidationPoint(date=pod.date, price=pod.price) for pod in criteria.price_on]
            if criteria.price_on
            else None
        ),
        country=country or (metadata.country if metadata else None),
        metadata=metadata.metadata if metadata else None,
    )

    # 5. Save to file
    _save_instrument_to_file(instrument, target_path, dry_run=dry_run)

    return instrument


def _save_instrument_to_file(instrument: Instrument, target_path: Path, dry_run: bool = False):
    """
    Saves an instrument to the specified YAML file.
    Handles duplicate checks (ISIN/Name) by reading existing file content first.
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would save to: {target_path}")
        print(
            yaml.dump(
                {"instruments": [instrument.model_dump(mode="json", exclude_none=True)]},
                sort_keys=False,
            )
        )
        return

    data = None

    if target_path.suffix == "" or target_path.is_dir():
        target_path.mkdir(parents=True, exist_ok=True)
        target_path = target_path / "manual.yaml"
    else:
        target_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing to check duplicates
    existing_instruments = []
    if target_path.exists():
        with open(target_path) as f:
            data = yaml.load(f, Loader=StrictSafeLoader)  # nosec B506
            if data:
                # Handle both list and dict formats
                if isinstance(data, list):
                    existing_instruments = data
                elif isinstance(data, dict) and "instruments" in data:
                    existing_instruments = data["instruments"]

    # Check for duplicates and update if needed
    for i, existing in enumerate(existing_instruments):
        match = False
        if isinstance(existing, dict):
            # Step 1: Strict match by ISIN + Currency
            if (
                existing.get("isin") == str(instrument.isin) if instrument.isin else False
            ) and existing.get("currency") == str(instrument.currency):
                logger.info(
                    f"Updating existing record for {instrument.symbol} "
                    f"({instrument.currency}) via ISIN match"
                )
                match = True
            # Step 2: Fallback to symbol match
            elif existing.get("symbol") == instrument.symbol:
                logger.info(f"Updating existing record for {instrument.symbol} via symbol match")
                match = True

            if match:
                # Update existing record with new data
                updated_data = instrument.model_dump(mode="json", exclude_none=True)
                existing_instruments[i] = updated_data
                _save_to_yaml(data, existing_instruments, target_path)
                return

    # Append new if no match found
    existing_instruments.append(instrument.model_dump(mode="json", exclude_none=True))
    _save_to_yaml(data, existing_instruments, target_path)

    logger.info(f"Auto-added {instrument.symbol} to {target_path}")


def _save_to_yaml(data: dict | None, instruments: list[dict], target_path: Path) -> None:
    if isinstance(data, dict) and "instruments" in data:
        data_to_dump = data
        data_to_dump["instruments"] = instruments
    else:
        data_to_dump = {"instruments": instruments}

    with open(target_path, "w") as f:
        yaml.dump(data_to_dump, f, sort_keys=False)
