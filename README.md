# Instrument Registry

Resolve and maintain canonical financial instrument records.
`instrument-reg` maps ISINs, provider symbols, names, and other identifiers to a
consistent local registry for Beancount and other financial workflows.

## Why Use It

- resolve an instrument from a flexible query like an ISIN, symbol, name, conid, or FX pair
- keep a local instrument registry with explicit, predictable write paths
- validate bundled and user-defined instrument data
- fetch provider-backed details without giving up local registry precedence

## Concepts

- **Canonical Name**: The unique identifier used in Beancount ledgers (e.g., `AAPL`, `CSPX`).
- **ISIN**: International Securities Identification Number (e.g., `US0378331005`). Primary identifier for many instruments.
- **FIGI**: Financial Instrument Global Identifier.
- **Query**: User input to `resolve` or `add`, such as an ISIN, provider symbol, name, conid, or FX pair.
- **Provider Symbol**: Provider-specific market symbol (e.g., `AAPL` on Yahoo, `CSPX.L` on Yahoo).

## Installation

Requires Python 3.14 or newer.

```bash
uv tool install instrument-registry
```

Install optional live-data providers when you want external resolution and verification:

```bash
uv tool install 'instrument-registry[providers]'
```

## Configuration

The registry loads data from two sources:
1.  **Bundled Data**: Built-in instruments (ETFs, Stocks).
2.  **User Data**: Custom YAML files located at `INSTRUMENT_REGISTRY_PATH`.

Set the environment variable to point to your custom registry:

```bash
export INSTRUMENT_REGISTRY_PATH=~/path/to/my/registry
```

The registry recursively scans this directory for `.yaml` and `.yml` files.

Read precedence for CLI commands is:
- user-defined registry paths first
- bundled registry second

Write behavior for `add` is strict:
- `instrument-reg add` writes only when `INSTRUMENT_REGISTRY_PATH` is set or `--registry-path` is passed
- `--registry-path` is a command option, so pass it after the subcommand, for example `instrument-reg add --registry-path ~/registry ...`
- if neither is set, the command fails instead of guessing a write location

Network lookup caching uses the platform cache directory by default. For sandboxed
development or CI runs, you can override it with `INSTRUMENT_REGISTRY_CACHE_DIR`:

```bash
export INSTRUMENT_REGISTRY_CACHE_DIR=.cache/instrument-registry
```

## Usage

### Resolve a Query
Resolve a query from local registries first, then external providers if needed.
Queries can be ISINs, provider symbols, names, IBKR conids, or common FX pairs.

```bash
instrument-reg resolve US0378331005
# Output: Resolved: Apple Inc. -> AAPL (yahoo)
```

### Automatic Currency Resolution
The resolver can derive common FX provider symbols when they are not already in your registry:
- `resolve EUR` -> `EURUSD=X`
- `resolve EUR/JPY` -> `EURJPY=X`
- `resolve USD/JPY` -> `JPY=X`

This eliminates the need to manually define standard Forex pairs in your registry files.

With price verification (checks if price matches historical data):
```bash
instrument-reg resolve US0378331005 --date 2024-01-01 --price 185.00
```

### Add an Instrument
Add a new instrument to your local registry.

`add` requires an explicit write target. Set `INSTRUMENT_REGISTRY_PATH` or pass `--registry-path`.

```bash
# Auto-fetch metadata — instrument type and asset class are inferred from the provider
instrument-reg add --registry-path ~/registry US0378331005 --fetch

# Manually specify all details (instrument-type and asset-class are required without --fetch)
INSTRUMENT_REGISTRY_PATH=~/registry instrument-reg add \
  --name AAPL \
  --isin US0378331005 \
  --symbol AAPL \
  --instrument-type Stock \
  --asset-class Stock \
  --currency USD
```

### Linting
Validate registry files and check for duplicates.

```bash
instrument-reg lint
```

Verify against live market data (checks if tickers are valid):

```bash
instrument-reg lint --verify
```

### Fetch Metadata
Fetch details using local registries first, then provider lookup when needed.

```bash
instrument-reg fetch --isin US0378331005
instrument-reg fetch --symbol AAPL --price
```

`fetch` runs on [treaty](https://github.com/romamo/treaty): piped output is a JSON envelope (`{"ok", "data", "error", "warnings", "meta"}`) with the result under `data`, and a terminal gets plain text. Failures have their own exit codes: `2` bad arguments, `5` no match, `79` a live-data provider is not installed. `instrument-reg manifest` and `instrument-reg fetch --schema` describe the command for agents.

### Command Summary

- `resolve`: resolve a query from local registries first, then external providers
- `add`: add or update an instrument in an explicit user registry path
- `fetch`: inspect provider details using local registry precedence first
- `lint`: validate registry files and optionally verify live provider data


## Programmatic Usage

You can use the registry in your Python scripts:

```python
from pydantic_market_data.models import SecurityCriteria

from instrument_registry.registry import get_registry

# Initialize registry (loads bundled data + user data from env var)
reg = get_registry()

# 1. Resolve by ISIN
instrument = reg.find_by_isin("US0378331005")
if instrument:
    print(f"Name: {instrument.name}, Currency: {instrument.currency}")

# 2. Search by identifier fields
criteria = SecurityCriteria(symbol="AAPL")
candidates = reg.find_candidates(criteria)
for c in candidates:
    print(f"Found: {c.name} ({c.isin})")

# 3. Look up by provider symbol
# Note: Provider-symbol lookup requires the provider name
instrument = reg.find_by_ticker("yahoo", "AAPL")
```


## Data Format (YAML)

```yaml
instruments:
  - name: AAPL
    isin: US0378331005
    instrument_type: Stock
    asset_class: Stock
    currency: USD
    tickers:
      yahoo: AAPL
    validation_points:
      - date: "2024-01-01"
        price: 185.64
```

## Contributing

- **Bundled Data**: Submit a PR to add common instruments to `src/instrument_registry/data/instruments/`.
- **Providers**: Implement new data sources in `src/instrument_registry/finder.py` by adding a class that implements the `DataProvider` protocol.
