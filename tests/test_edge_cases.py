import pytest
import yaml
from pydantic import ValidationError

from instrument_registry.models import AssetClass, Instrument, InstrumentType
from instrument_registry.registry import save_instrument


@pytest.fixture
def temp_registry_file(tmp_path):
    f = tmp_path / "manual.yaml"
    f.write_text("instruments: []")
    return f


def test_invalid_isin_validation():
    """Ensure invalid ISINs are rejected by the model."""
    with pytest.raises(ValidationError, match="Invalid ISIN format"):
        Instrument(
            symbol="INVALID",
            isin="SHORT",
            instrument_type=InstrumentType.ETF,
            asset_class=AssetClass.EQUITY_ETF,
            currency="USD",
        )


def test_currency_normalization():
    """Ensure currencies are handled by the model (normalization check)."""
    comm = Instrument(
        symbol="TEST",
        isin="US0378331005",
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="usd",  # lowercase
    )
    assert str(comm.currency) == "USD"


def test_idempotent_addition(temp_registry_file):
    """Adding the exact same commodity multiple times should not create duplicates."""
    comm = Instrument(
        symbol="AAPL",
        isin="US0378331005",
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="USD",
    )

    # Add twice
    save_instrument(comm, temp_registry_file)
    save_instrument(comm, temp_registry_file)

    with open(temp_registry_file) as f:
        data = yaml.safe_load(f)

    assert len(data["instruments"]) == 1
    assert data["instruments"][0]["symbol"] == "AAPL"


def test_instrument_priority_isin_currency(temp_registry_file):
    """
    If ISIN + Currency matches, it should update that record even if the name
    in the new object is different (derived), unless we specifically handle it.
    Actually, save_instrument uses the new object's name if it matches ISIN/Currency.
    The CLI layer is responsible for preserving the name.
    """
    # 1. Add initial
    comm1 = Instrument(
        symbol="CUSTOM_NAME",
        isin="US0378331005",
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="USD",
    )
    save_instrument(comm1, temp_registry_file)

    # 2. Add with different symbol but same ISIN/Currency
    comm2 = Instrument(
        symbol="AAPL",  # Derived symbol
        isin="US0378331005",
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="USD",
    )
    save_instrument(comm2, temp_registry_file)

    with open(temp_registry_file) as f:
        data = yaml.safe_load(f)

    assert len(data["instruments"]) == 1
    # In the registry layer, it updates the record.
    # The CLI layer is where we reuse the existing symbol.
    assert data["instruments"][0]["symbol"] == "AAPL"


def test_same_symbol_different_isin_replaces_existing(temp_registry_file):
    """
    Two instruments with the same symbol but different ISINs: the second write
    replaces the first because save_instrument matches on symbol.
    """
    comm1 = Instrument(
        symbol="SHARED",
        isin="US0378331005",
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="USD",
    )
    save_instrument(comm1, temp_registry_file)

    comm2 = Instrument(
        symbol="SHARED",
        isin="US5949181045",
        instrument_type=InstrumentType.STOCK,
        asset_class=AssetClass.STOCK,
        currency="USD",
    )
    save_instrument(comm2, temp_registry_file)

    with open(temp_registry_file) as f:
        data = yaml.safe_load(f)

    assert len(data["instruments"]) == 1
    assert data["instruments"][0]["isin"] == "US5949181045"


def test_dual_listing_coexistence(temp_registry_file):
    """Verify that same ISIN with different currencies can coexist."""
    comm_eur = Instrument(
        symbol="GDX_EUR",
        isin="IE00BQQP9F84",
        instrument_type=InstrumentType.ETF,
        asset_class=AssetClass.EQUITY_ETF,
        currency="EUR",
    )
    comm_gbp = Instrument(
        symbol="GDX_GBP",
        isin="IE00BQQP9F84",
        instrument_type=InstrumentType.ETF,
        asset_class=AssetClass.EQUITY_ETF,
        currency="GBP",
    )

    save_instrument(comm_eur, temp_registry_file)
    save_instrument(comm_gbp, temp_registry_file)

    with open(temp_registry_file) as f:
        data = yaml.safe_load(f)

    assert len(data["instruments"]) == 2
    currencies = [c["currency"] for c in data["instruments"]]
    assert "EUR" in currencies
    assert "GBP" in currencies
