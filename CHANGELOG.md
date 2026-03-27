# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-03-27

### Added
- **Relaxed Commodity Constraints**: Decoupled the registry from strict Beancount naming requirements. Any valid financial symbol (including `^GSPC`, `EURUSD=X`) can now be used as a commodity name without automatic sanitization, increasing flexibility for non-Beancount use cases.

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
- **Strict Field Lookup**: Refactored `CommodityRegistry` to use `SecurityCriteria` for targeted searching by ISIN, Symbol, or FIGI, improving accuracy over generic string matching.

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

- Initial release of `commodity-registry`.
- Comprehensive CLI for instrument data management.
- Support for ISIN, Ticker, and Name mapping.
- Integration with Yahoo Finance and FT Markets (optional).
- Standardized OSS package structure.
- GitHub Actions for CI and Trusted Publishing.
