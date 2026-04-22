# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.6] - 2026-04-22

### Added
- **`resolve` from stdin**: The `resolve` command now accepts a piped JSON record when no positional `query` argument is given. Callers can pipe `{"isin":"...","symbol":"...","currency":"...","asset_class":null,...}` directly and the command maps all fields into `SecurityCriteria`. CLI flags (`--currency`, `--asset-class`, etc.) take precedence over piped JSON values.

## [0.2.5] - 2026-04-22

### Changed
- **Type inference**: Extracted a shared `_infer_types()` helper in `finder.py`, eliminating the duplicated if/elif chain that mapped raw asset-class strings to `InstrumentType`/`AssetClass` enums.
- **`add_instrument` signature**: `instrument_type` and `asset_class` are now optional; values are resolved from provider `SearchResult` metadata before falling back to a clear `ValueError` if still unresolved.
- **CLI `add` command**: `--instrument-type` and `--asset-class` are no longer required upfront; they may be omitted when `--fetch` is used and the provider supplies type information.
- **Dependency**: Bumped `agentyper` to 0.1.12.

## [0.2.4] - 2026-04-17

### Fixed
- **Base-package CLI tests**: Updated fetch and verify CLI tests to mock provider availability explicitly, so release CI passes consistently when optional live-data providers are not installed.

## [0.2.3] - 2026-04-17

### Fixed
- **Publish CI**: Made the provider optimization test skip cleanly when optional `py-yfinance` dependencies are not installed, so base-package release workflows can validate and publish successfully.

## [0.2.2] - 2026-04-17

### Changed
- **Package Rename**: Renamed the distribution and Python package from `commodity-registry` / `commodity_registry` to `instrument-registry` / `instrument_registry`, including bundled data paths and import locations.
- **CLI Behavior**: Reworked the CLI entrypoint and command structure around `instrument-reg`, with clearer handling for explicit registry write targets and verbosity flags.
- **Documentation**: Rewrote the README around instrument-focused terminology, installation, configuration, and command examples.

### Fixed
- **CLI Coverage**: Expanded automated tests for dispatch, verbosity, output formats, and registry path handling to lock in the new command behavior.

## [0.2.1] - 2026-04-14

### Changed
- **Finder Logic Improvements**: Refined the security resolution engine with intelligent asset class mapping and result deduplication, improving accuracy for multi-provider lookups.

### Fixed
- **Repository Hygiene**: Cleaned up accidental files and updated `.gitignore` for a cleaner source distribution.

## [0.2.0] - 2026-03-27

### Added
- **Relaxed Instrument Constraints**: Decoupled the registry from strict Beancount naming requirements. Any valid financial symbol (including `^GSPC`, `EURUSD=X`) can now be used as an instrument name without automatic sanitization, increasing flexibility for non-Beancount use cases.

## [0.1.12] - 2026-03-27

### Changed
- **Provider Resilience**: Updated `search_isin` to gracefully handle provider failures. If one provider (e.g. Yahoo Finance) fails or times out, the system now logs a warning and continues with other available providers instead of crashing.

## [0.1.11] - 2026-03-27

### Fixed
- **FX Metadata**: Improved identification of FX instruments as `CASH` asset class.
- **Name Generation**: Refined name generation for FX pairs to be more Beancount-friendly (avoiding `EURUSD.X` style).

## [0.1.10] - 2026-03-27

### Fixed
- **Beancount Compatibility**: Updated `res.name` to consistently use the generated Beancount-style name.

## [0.1.9] - 2026-03-26

### Changed
- **Type Safety**: Enforced strict typing and Namespace Pattern across the codebase for better DX and Mypy compatibility.
- formatting: Applied unified Ruff formatting.

## [0.1.8] - 2026-03-26

### Fixed
- Registry: Fixed `AttributeError` in `find_candidates` when certain fields were missing.
- Release Process: Strengthened automated release scripts and quality gates.

## [0.1.7] - 2026-02-15

### Added
- **Unified Security Resolution**: Introduced `resolve_security` in `finder.py` as a single entry point for resolving any instrument (Stocks, ETFs, Currencies) across Registry, FX, and Online sources.
- **Strict Field Lookup**: Refactored `InstrumentRegistry` to use `SecurityCriteria` for targeted searching by ISIN, Symbol, or FIGI, improving accuracy over generic string matching.

### Changed
- CLI Harmonization: All CLI commands (`resolve`, `fetch`, `add`) now build and use `SecurityCriteria` for consistent data modeling.
- ISIN Heuristic: Improved ISIN detection in the CLI by requiring a minimum length of 12 characters, preventing misidentification of standard FX symbols.

## [0.1.6] - 2026-02-15

### Added
- Programmatic Currency Resolution: Integrated smart lookup logic in `finder.py` to automatically resolve standard currencies to Yahoo tickers (e.g. `EUR` -> `EURUSD=X`).
- **Live Verification**: Programmatic currency hits are now verified against the live provider to ensure "True Truth" resolution.
- Currency Pair Support: Added parsing for composite pair strings like `EURUSD`, `EUR/JPY`, and `EUR-USD`.
- CLI Price Fetch: Added `--price` flag to the `fetch` command to retrieve the latest market price.
- Recursive directory scanning: `load_path` now recursively finds all `.yaml`/`.yml` files.
- Dynamic provider discovery: Dynamically detects available providers (`py-yfinance`, `py-ftmarkets`).
- Caching: Integrated `diskcache` for 24-hour metadata caching.

### Changed
- `CLI`: Updated help text and logic to use dynamic provider list.
- `README.md`: Completely rewritten with Concepts, Configuration, and Programmatic Usage sections.
- `registry.py`: Duplicate handling logic improved.

### Removed
- Unused `test_invalid.beancount` file.

## [0.1.0] - 2026-02-09

### Added

- Initial release of `instrument-registry`.
- Comprehensive CLI for instrument data management.
- Support for ISIN, Ticker, and Name mapping.
- Integration with Yahoo Finance and FT Markets (optional).
- Standardized OSS package structure.
- GitHub Actions for CI and Trusted Publishing.
