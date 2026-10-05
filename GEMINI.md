# Instrument Registry: Project Context

A Python-based registry for canonical financial instrument records. This project maps ISINs, provider symbols, names, and other identifiers for use in financial tools like Beancount.

## Project Overview

- **Core Purpose**: Resolve financial symbols and ISINs to a canonical registry, providing metadata (currency, asset class) and price verification.
- **Main Technologies**:
  - **Language**: Python 3.10+
  - **Data Modeling**: [Pydantic v2](https://docs.pydantic.dev/latest/) for robust validation and settings.
  - **Data Storage**: YAML files (Strict loading, no duplicate keys).
  - **Market Data**: Integrates with `py-yfinance` and `py-ftmarkets`.
  - **Caching**: [diskcache](https://grantjenks.com/docs/diskcache/) for metadata and price persistence.
  - **CLI Framework**: [treaty](https://github.com/romamo/treaty) with command modules under `src/instrument_registry/cli/`.

## Architecture & Concepts

- **Models (`models.py`)**: Defines `Instrument`, `Tickers`, `ValidationPoint`, and enums for `InstrumentType` and `AssetClass`.
- **Registry (`registry.py`)**: Manages bundled and user-provided instrument data. Supports lookups by ISIN, Name, FIGI, and Ticker.
- **Finder (`finder.py`)**: Handles external data fetching, metadata resolution, and price verification.
- **Interfaces (`interfaces.py`)**: Uses Python `Protocols` for `DataProvider` and `InstrumentLookup` to ensure modularity.
- **Resolution Strategy**:
  1.  **Local Registry**: Check bundled and user YAML files.
  2.  **Programmatic FX**: Resolve currency pairs (e.g., `EURUSD=X`) without explicit registry entries.
  3.  **Online Search**: Query providers (Yahoo, FT) for unknown symbols/ISINs.

## Commands

### Environment Setup
The project uses `uv` for dependency management.
```bash
uv sync --all-extras
source .venv/bin/activate
```

### Building & Running
- **Install Tool**: `uv tool install .`
- **CLI Usage**: `instrument-reg --help`
- **Resolve Query**: `instrument-reg resolve [QUERY]`
- **Add Instrument**: `instrument-reg add --registry-path ~/registry [ISIN] --fetch`

### Testing & Quality
- **Run Tests**: `pytest`
- **Linting**: `ruff check .`
- **Type Checking**: `mypy .`
- **Security Audit**: `bandit -r src/`

## Development Conventions

- **Data Integrity**: All instrument data must adhere to the Pydantic models in `models.py`.
- **User Data**: User-specific registries are loaded from the path specified in `INSTRUMENT_REGISTRY_PATH`.
- **CLI Write Target**: `--registry-path` is currently documented and supported as an option after the subcommand, e.g. `instrument-reg add --registry-path ~/registry ...`.
- **Bundled Data**: Permanent common instruments are stored in `src/instrument_registry/data/instruments/`.
- **Testing Strategy**: New features must include a smoke test or unit test in the `tests/` directory.
- **Optional Dependencies**: Market data providers (Yahoo, FT) are optional; use `try...except ImportError` when adding new providers in `finder.py`.
- **Caching**: External lookups should be cached using the `@cache.memoize` decorator in `finder.py`.
