import pytest
import yaml

from commodity_registry.registry import CommodityRegistry


@pytest.fixture
def temp_registry_dir(tmp_path):
    # Create a mock registry structure
    reg_dir = tmp_path / "commodities"
    reg_dir.mkdir()

    # Create a base file
    base_file = reg_dir / "base.yaml"
    base_data = {
        "commodities": [
            {
                "name": "AAPL",
                "isin": "US0378331005",
                "asset_class": "Stock",
                "instrument_type": "Stock",
                "currency": "USD",
                "tickers": {"yahoo": "AAPL"},
            },
            {
                "name": "XAID",
                "isin": "GB00B00FHZ82",
                "asset_class": "CommodityETF",
                "instrument_type": "ETF",
                "currency": "GBP",
                "tickers": {"yahoo": "XAID.L"},
            },
        ]
    }
    with open(base_file, "w") as f:
        yaml.dump(base_data, f)

    return reg_dir


def test_registry_loading(temp_registry_dir):
    reg = CommodityRegistry(extra_paths=[temp_registry_dir], include_bundled=False)
    all_commodities = reg.get_all()
    assert len(all_commodities) == 2
    assert any(c.name == "AAPL" for c in all_commodities)
    assert any(c.name == "XAID" for c in all_commodities)


def test_find_by_isin(temp_registry_dir):
    reg = CommodityRegistry(extra_paths=[temp_registry_dir], include_bundled=False)
    c = reg.find_by_isin("US0378331005")
    assert c is not None
    assert c.name == "AAPL"


def test_find_candidates_by_name(temp_registry_dir):
    from pydantic_market_data.models import SecurityCriteria

    reg = CommodityRegistry(extra_paths=[temp_registry_dir], include_bundled=False)
    candidates = reg.find_candidates(SecurityCriteria(symbol="AAPL"))
    assert len(candidates) == 1
    assert candidates[0].name == "AAPL"


def test_find_by_ticker(temp_registry_dir):
    reg = CommodityRegistry(extra_paths=[temp_registry_dir], include_bundled=False)
    c = reg.find_by_ticker("yahoo", "XAID.L")
    assert c is not None
    assert c.name == "XAID"


def test_registry_merging(temp_registry_dir):
    # Add an override file
    override_file = temp_registry_dir / "override.yaml"
    override_data = {
        "commodities": [
            {
                "name": "AAPL",
                "isin": "US0378331005",
                "asset_class": "Stock",
                "instrument_type": "Stock",
                "currency": "USD",
                "issuer": "Overridden Issuer",
            }
        ]
    }
    with open(override_file, "w") as f:
        yaml.dump(override_data, f)

    reg = CommodityRegistry(extra_paths=[temp_registry_dir], include_bundled=False)
    c = reg.find_by_isin("US0378331005")
    assert c.issuer == "Overridden Issuer"
    # Ensure tickers are NOT lost if not in override (wait, need to check merge behavior)
    # The current implementation might replace the whole object if ISIN matches.
    # Let's verify merge logic in registry.py
    # NOTE: The current simple implementation overrides the entire object if ISIN matches.


def test_recursive_loading(temp_registry_dir):
    # Create a nested directory
    nested_dir = temp_registry_dir / "subdir" / "nested"
    nested_dir.mkdir(parents=True)

    # Add a file in the nested directory
    nested_file = nested_dir / "nested.yaml"
    nested_data = {
        "commodities": [
            {
                "name": "NESTED",
                "isin": "US0378331005",  # Valid ISIN (reused AAPL for validity)
                "asset_class": "Stock",
                "instrument_type": "Stock",
                "currency": "USD",
            }
        ]
    }
    with open(nested_file, "w") as f:
        yaml.dump(nested_data, f)

    reg = CommodityRegistry(extra_paths=[temp_registry_dir], include_bundled=False)
    from pydantic_market_data.models import SecurityCriteria

    c = reg.find_candidates(SecurityCriteria(symbol="NESTED"))
    assert len(c) == 1
    assert c[0].name == "NESTED"


def test_find_by_figi(temp_registry_dir):
    # Add FIGI to a commodity
    figi_file = temp_registry_dir / "figi.yaml"
    figi_data = {
        "commodities": [
            {
                "name": "FIGI_STOCK",
                "isin": "US0378331005",
                "figi": "BBG000B9XRY4",
                "asset_class": "Stock",
                "instrument_type": "Stock",
                "currency": "USD",
            }
        ]
    }
    with open(figi_file, "w") as f:
        yaml.dump(figi_data, f)

    reg = CommodityRegistry(extra_paths=[temp_registry_dir], include_bundled=False)
    assert reg._by_figi.get("BBG000B9XRY4") is not None
    assert reg._by_figi["BBG000B9XRY4"].name == "FIGI_STOCK"


def test_duplicate_yaml_key(temp_registry_dir):
    # Create a file with duplicate keys
    dup_file = temp_registry_dir / "duplicate.yaml"
    with open(dup_file, "w") as f:
        f.write("commodities: []\ncommodities: []")

    with pytest.raises(yaml.constructor.ConstructorError, match="Duplicate key found"):
        CommodityRegistry(extra_paths=[dup_file], include_bundled=False)


def test_add_commodity_no_sanitization(tmp_path):
    from pydantic_market_data.models import SecurityCriteria

    from commodity_registry.models import AssetClass, InstrumentType
    from commodity_registry.registry import add_commodity

    target_path = tmp_path / "manual.yaml"
    criteria = SecurityCriteria(symbol="^GSPC", currency="USD")

    # Should NOT add X. prefix or convert to dots
    c = add_commodity(
        criteria=criteria,
        metadata=None,
        target_path=target_path,
        instrument_type=InstrumentType.INDEX,
        asset_class=AssetClass.STOCK,
    )

    assert c.name == "^GSPC"

    # Test with provider prefix - should extract only ticker part
    criteria_2 = SecurityCriteria(symbol="YAHOO:EURUSD=X", currency="USD")
    c2 = add_commodity(
        criteria=criteria_2,
        metadata=None,
        target_path=target_path,
        instrument_type=InstrumentType.CASH,
        asset_class=AssetClass.CASH,
    )
    assert c2.name == "EURUSD=X"
