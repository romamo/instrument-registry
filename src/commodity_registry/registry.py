import logging
import os
from pathlib import Path

import yaml
from pydantic_market_data.models import Currency, SecurityCriteria, Ticker

from .interfaces import SearchResult
from .models import AssetClass, Commodity, CommodityFile, InstrumentType
from .resources import get_commodity_files

logger = logging.getLogger(__name__)


class StrictSafeLoader(yaml.SafeLoader):
    """YAML Loader that disallows duplicate keys."""

    def construct_mapping(self, node, deep=False):
        seen_keys: set = set()
        for key_node, _value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in seen_keys:
                raise yaml.constructor.ConstructorError(
                    f"Duplicate key found in YAML: {key}", key_node.start_mark
                )
            seen_keys.add(key)
        return super().construct_mapping(node, deep=deep)


class CommodityRegistry:
    def __init__(self, include_bundled: bool = True, extra_paths: list[Path] | None = None):
        self._commodities: list[Commodity] = []
        self._load_errors: list[str] = []
        self._by_isin: dict[str, list[Commodity]] = {}
        self._by_figi: dict[str, Commodity] = {}
        self._by_name: dict[str, Commodity] = {}
        self._by_ticker: dict[str, Commodity] = {}  # Map "PROVIDER:TICKER" -> Commodity

        if include_bundled:
            self.load_bundled_data()

        if extra_paths:
            for p in extra_paths:
                self.load_path(p)

        self._rebuild_indices()

    def load_bundled_data(self):
        """Loads all commodity data from the bundled YAML files."""
        logger.debug("Loading bundled registry data...")
        for file_path in get_commodity_files():
            self._load_file(file_path)

    def load_path(self, path: Path):
        """Loads commodity data from a specific file or directory (recursively)."""
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

        file_model = CommodityFile(**data)
        self._commodities.extend(file_model.commodities)

    @property
    def load_errors(self) -> list[str]:
        return self._load_errors

    def _rebuild_indices(self):
        self._by_isin = {}
        for c in self._commodities:
            if c.isin:
                isin_key = str(c.isin).upper()
                if isin_key not in self._by_isin:
                    self._by_isin[isin_key] = []
                self._by_isin[isin_key].insert(0, c)  # User overrides first

        self._by_figi = {}
        self._by_name = {c.name.upper(): c for c in self._commodities}
        self._by_ticker = {}

        for c in self._commodities:
            if c.figi:
                self._by_figi[c.figi.upper()] = c

            if c.tickers:
                for provider, ticker in c.tickers.model_dump().items():
                    if ticker:
                        # Later items in self._commodities override earlier ones
                        self._by_ticker[f"{provider.upper()}:{str(ticker).upper()}"] = c

    def find_by_isin(self, isin: str, currency: Currency | None = None) -> Commodity | None:
        matches = self.find_candidates(SecurityCriteria(isin=isin, currency=currency))
        return matches[0] if matches else None

    def find_candidates(self, criteria: SecurityCriteria) -> list[Commodity]:
        """
        Finds all commodities matching the criteria using strict field lookups.
        Returns empty list if no match.
        """
        candidates = []

        # 1. Try ISIN (Strict)
        if criteria.isin:
            candidates.extend(self._by_isin.get(str(criteria.isin).upper(), []))

        # 2. Try Name/Symbol (Strict)
        if criteria.symbol:
            sym_upper = str(criteria.symbol).upper()
            if sym_upper in self._by_name:
                candidates.append(self._by_name[sym_upper])

        # 3. Try FIGI (Strict)
        # Note: FIGI lookup isn't explicitly in SecurityCriteria yet, but we have it in index.

        # Deduplicate (by name)
        seen = set()
        unique_candidates = []
        for c in candidates:
            if c.name not in seen:
                unique_candidates.append(c)
                seen.add(c.name)

        # 4. Filter by Currency if provided
        if criteria.currency:
            curr_str = str(criteria.currency)
            unique_candidates = [
                c for c in unique_candidates if str(c.currency).upper() == curr_str.upper()
            ]

        return unique_candidates

    def find_by_ticker(self, provider: str, ticker: Ticker | str) -> Commodity | None:
        """
        Finds a commodity by provider-specific ticker.
        Example: find_by_ticker("yahoo", "4GLD.DE")
        """
        ticker_str = ticker.root if isinstance(ticker, Ticker) else ticker
        return self._by_ticker.get(f"{provider.upper()}:{ticker_str.upper()}")

    def get_all(self) -> list[Commodity]:
        return self._commodities


# Default registry if imported directly
def get_registry(
    include_bundled: bool = True, extra_paths: list[Path] | None = None
) -> CommodityRegistry:
    if not extra_paths:
        env_path = os.getenv("COMMODITY_REGISTRY_PATH")
        if env_path:
            extra_paths = [Path(env_path).expanduser()]
        else:
            # Use platformdirs default
            import platformdirs

            default_dir = Path(platformdirs.user_data_dir("commodity-registry"))
            if default_dir.exists():
                extra_paths = [default_dir]

    return CommodityRegistry(include_bundled=include_bundled, extra_paths=extra_paths)


def add_commodity(
    criteria: SecurityCriteria,
    metadata: SearchResult | None,  # None if not found online
    target_path: Path,
    instrument_type: InstrumentType,
    asset_class: AssetClass,
    name: str | None = None,
    dry_run: bool = False,
) -> Commodity:
    """
    Adds a new commodity to the registry.

    Uses SecurityCriteria.symbol (the raw token or security symbol).
    Extracts base ticker (before ':') for Beancount name.
    Stores provider-specific tickers only if found online.
    """
    # 1. Determine ticker
    if not criteria.symbol and not name:
        if not metadata or not metadata.ticker:
            raise ValueError("SecurityCriteria.symbol or name is required if metadata fetch failed")
        criteria.symbol = str(metadata.ticker)

    # 2. Extract base ticker for Beancount name
    # Example: "ALUM.L" -> "ALUM", "4GLD:GER:EUR" -> "4GLD"
    if name:
        clean_name = name
    else:
        # Prefer online metadata ticker if available, otherwise fallback to criteria.symbol
        token = str(metadata.ticker) if metadata and metadata.ticker else str(criteria.symbol or "")
        if not token:
            # Should not happen given check in step 1, but for mypy
            raise ValueError("Could not determine ticker for name generation")

        # For CASH instruments, prefer use the name (e.g. "EUR") if it is a 3-letter code.
        is_fx = instrument_type == InstrumentType.CASH or asset_class == AssetClass.CASH
        if is_fx and metadata and metadata.name and len(str(metadata.name)) == 3:
            clean_name = str(metadata.name)
        else:
            # Simple extraction: "ALUM.L" -> "ALUM", "^GSPC" -> "^GSPC"
            # But let's be careful with providers. If it's "YAHOO:^GSPC", we want "^GSPC".
            clean_name = token.split(":")[-1]

    # 3. Build tickers dict (prefer online metadata, fallback to criteria.symbol)
    tickers_dict: dict[str, str] | None = None
    if metadata and metadata.ticker:
        tickers_dict = {metadata.provider.value: str(metadata.ticker)}
    elif criteria.symbol:
        symbol_str = str(criteria.symbol)
        # Fallback to yahoo for manual tickers unless it contains a provider prefix
        if ":" in symbol_str:
            parts = symbol_str.split(":", 1)
            tickers_dict = {parts[0].lower(): parts[1]}
        else:
            tickers_dict = {"yahoo": symbol_str}

    # 4. Create commodity
    from .models import Tickers, ValidationPoint

    comm_currency = (
        Currency(str(criteria.currency).upper())
        if criteria.currency
        else (metadata.currency if metadata and metadata.currency else Currency("USD"))
    )

    commodity = Commodity(
        name=clean_name,
        isin=criteria.isin,
        figi=None,
        instrument_type=instrument_type,
        asset_class=asset_class,
        currency=comm_currency,
        tickers=Tickers(**tickers_dict) if tickers_dict else None,
        validation_points=[ValidationPoint(date=criteria.target_date, price=criteria.target_price)]
        if criteria.target_date and criteria.target_price is not None
        else None,
    )

    # 5. Save to file
    _save_commodity_to_file(commodity, target_path, dry_run=dry_run)

    return commodity


def _save_commodity_to_file(commodity: Commodity, target_path: Path, dry_run: bool = False):
    """
    Saves a commodity to the specified YAML file.
    Handles duplicate checks (ISIN/Name) by reading existing file content first.
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would save to: {target_path}")
        print(
            yaml.dump(
                {"commodities": [commodity.model_dump(mode="json", exclude_none=True)]},
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
    existing_commodities = []
    if target_path.exists():
        with open(target_path) as f:
            data = yaml.load(f, Loader=StrictSafeLoader)  # nosec B506
            if data:
                # Handle both list and dict formats
                if isinstance(data, list):
                    existing_commodities = data
                elif isinstance(data, dict) and "commodities" in data:
                    existing_commodities = data["commodities"]

    # Check for duplicates and update if needed
    for i, existing in enumerate(existing_commodities):
        match = False
        if isinstance(existing, dict):
            # Step 1: Strict match by ISIN + Currency
            if (
                existing.get("isin") == str(commodity.isin) if commodity.isin else False
            ) and existing.get("currency") == str(commodity.currency):
                logger.info(
                    f"Updating existing record for {commodity.name} "
                    f"({commodity.currency}) via ISIN match"
                )
                match = True
            # Step 2: Fallback to Name match
            elif existing.get("name") == commodity.name:
                logger.info(f"Updating existing record for {commodity.name} via name match")
                match = True

            if match:
                # Update existing record with new data
                updated_data = commodity.model_dump(mode="json", exclude_none=True)
                existing_commodities[i] = updated_data
                _save_to_yaml(data, existing_commodities, target_path)
                return

    # Append new if no match found
    existing_commodities.append(commodity.model_dump(mode="json", exclude_none=True))
    _save_to_yaml(data, existing_commodities, target_path)

    logger.info(f"Auto-added {commodity.name} to {target_path}")


def _save_to_yaml(data: dict | None, commodities: list[dict], target_path: Path) -> None:
    if isinstance(data, dict) and "commodities" in data:
        data_to_dump = data
        data_to_dump["commodities"] = commodities
    else:
        data_to_dump = {"commodities": commodities}

    with open(target_path, "w") as f:
        yaml.dump(data_to_dump, f, sort_keys=False)
