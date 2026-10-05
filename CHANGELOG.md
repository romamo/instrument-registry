# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.3] - 2026-10-05

### Changed
- **Dependencies**: `providers` extra requires `py-openfigi2>=0.2.1`, which runs on treaty instead of agentyper, so `agentyper` no longer installs at all

## [0.4.2] - 2026-10-03

### Changed
- **Dependencies**: `providers` extra requires `py-yfinance>=0.4.3`, which accepts `treaty>=1.0.0rc12,<1.1`, so it installs alongside `py-openfigi2` 0.1.5 and this package's own `treaty>=1.0.0rc30`

## [0.4.1] - 2026-10-03

### Fixed
- First published release of the 0.4.0 changes below: the 0.4.0 publish stopped at a test that read the developer's own user registry, so 0.4.0 never reached PyPI

## [0.4.0] - 2026-10-03

### Changed
- **Every command runs on treaty**, so all of them answer with a JSON envelope when piped (result under `data`), take `-v`/`-vv` after the command, and accept a repeated `--registry-path`
- **`resolve`** (breaking for callers that parse its output or pipe into it):
  - Takes exactly one QUERY; piped records go to the new `resolve-batch`
  - The result is always an instrument record with an `effect` (`created` for a saved discovery, `noop` for a registry hit, `would_*` under `--dry-run`); a provider-only match, such as a currency, has the same shape with empty registry fields
  - `--no-save` is now `--dry-run`, and `--provider` (never implemented) is gone
  - `--date` and `--price` must be passed together; a malformed ISIN or date exits `2` before any lookup
  - No match exits `5` (`NOT_FOUND`) instead of `1`, and missing live-data providers exit `79` (`MISSING_PROVIDER`)
  - `--report-price` puts the price on every result, not only on provider-only matches
- **`add`**:
  - Returns the saved record with `effect` `created` or `updated`; `--dry-run` returns `would_create` or `would_update` instead of printing YAML
  - Without `--fetch`, `--currency`, `--instrument-type`, and `--asset-class` are required up front (exit `2`); a QUERY, `--isin`, or `--symbol` is always required
  - No write target exits `4` (`PRECONDITION`), a symbol already registered under another asset class exits `6` (`CONFLICT`), and `--fetch` with no provider match adds a `METADATA_NOT_FOUND` warning
  - `--figi` (never stored) is gone
- **`lint`**:
  - Registry errors exit `80` (`LINT_FAILED`) instead of `1`, with the full report still in `data`
  - `--verify` results are in `data.verifications` (status and check lines per instrument) instead of printed text; `-vv` per-instrument lines go to stderr
  - `--only` requires `--verify`, and an unknown `--only` symbol exits `5` (`NOT_FOUND`)
- `registry.add_instrument` is split into `build_instrument` and `save_instrument`, which returns `SaveEffect.CREATED` or `UPDATED`; `_save_instrument_to_file` is renamed `save_instrument` and no longer prints on a dry run. A symbol collision raises `SymbolCollision`, a `ValueError`

### Added
- `resolve-batch` reads query records from stdin or `--input-file` (JSON objects, arrays, or upstream envelopes) and returns one result per record; any failed record exits `3` (`PARTIAL_FAILURE`), so only those need a retry

### Dependencies
- Removed `agentyper`
- `treaty>=1.0.0rc30,<1.1` (was `>=1.0.0rc5`). Its audit log setting `INSTRUMENT_REG_AUDIT_LOG` takes `0`, `1`, or an absolute path; `off` is refused

## [0.3.0] - 2026-09-30

### Changed
- **Python 3.14 or newer is required** (was 3.10). The CLI is moving to [treaty](https://github.com/romamo/treaty), which requires 3.14
- **`fetch` runs on treaty** (breaking for callers that parse its output):
  - Piped output is a JSON envelope; the result moves from the top level to `data`, and `metadata` is `{}` instead of `null` when empty
  - A missing match exits `5` (`NOT_FOUND`) instead of logging a warning and exiting `0`
  - A missing live-data provider exits `79` (`MISSING_PROVIDER`) instead of `1`
  - At least one of `--isin`, `--figi`, `--symbol` is required, and a malformed identifier exits `2` before any lookup
  - `--price` reports a `PRICE_UNAVAILABLE` warning when no price comes back, instead of silently leaving `price` empty
  - `-v`/`-vv` go after the command (`instrument-reg fetch -v ...`), and `--registry-path` may be repeated
- `instrument-reg manifest` prints the treaty manifest for the commands migrated so far
- `InstrumentType`, `AssetClass`, and `ProviderName` are `StrEnum`s: `str()` and f-strings give the value (`"Stock"`) instead of `"AssetClass.STOCK"`

### Dependencies
- Added `treaty>=1.0.0rc5`

## [0.2.14] - 2026-09-30

### Fixed
- **Price lookup skips a provider that returns an invalid ISIN match**: when a provider's `resolve` result fails `pydantic-market-data` validation (e.g. `py-yfinance` passing a raw `quoteType` such as `"ETF"` as the asset class), the provider is skipped and the next one is tried instead of the lookup crashing
- **ISIN lookups ignore the asset-class filter**: an ISIN already identifies one instrument, so a registry match is no longer dropped because the requested asset class maps differently
- **Crypto detection**: any provider security type containing `CRYPTO` now maps to `Crypto`, not only `CRYPTOCURRENCY`
- **`resolve` price verification** uses the already-parsed `price_on` date instead of re-parsing `--date`

### Internal
- `resolve` output selection simplified

## [0.2.13] - 2026-09-30

### Fixed
- **`resolve` reads response envelopes on stdin**: piped input may now be `{"ok": ..., "data": ...}` envelopes (one per line, pretty-printed, or streamed with a terminal `data: null` line) as well as bare JSON objects and arrays. An envelope contributes its `data`; one with `ok: false` fails the run with the upstream error code and message instead of being read as a query. `other-cli --format json | instrument-registry resolve` now works without `--format jsonl`
- **Stdin errors name the line** where a bad value starts, including after pretty-printed values

## [0.2.12] - 2026-05-10

### Changed
- **CLI output unified via `typer.output()`**: All commands (`add`, `fetch`, `lint`, `resolve`) now emit results through `typer.output()` directly, removing the `emit_structured()` helper and consolidating format dispatch.
- **`exit_with_error` simplified**: Delegates fully to `typer.exit_error()` regardless of format — no duplicate logger call.
- **`resolve` error handling**: Internal `ResolutionFailed` exception replaces direct `exit_with_error()` calls inside `_resolve_criteria`, enabling clean propagation and consistent error output via a single handler at the command level.
- **`current_format` fix**: Removed the `explicit` guard so format is respected even when not explicitly set on the context.

### Dependencies
- `agentyper` bumped to `>=0.1.13`

### Internal
- pytest `testpaths` and `norecursedirs` configured in `pyproject.toml`

## [0.2.11] - 2026-05-09

### Changed
- **`Instrument.name` renamed to `Instrument.symbol`**: The canonical identifier field is now `symbol`; `name` is a new optional field for display names. All internal registry indices updated accordingly (`_by_symbol`, deduplication key).
- **Asset class mapping expanded**: `_ASSET_CLASS_MAP` is now a module-level constant (no lazy init) and adds `FIXED_INCOME → FixedIncomeETF`, `INDEX → Stock`, `COMMODITY → Commodity`, and `FX → Cash` mappings.
- **`resolve` asset-class coercion**: New `_coerce_asset_class` helper maps local registry asset-class strings (e.g. `"stock"`, `"equityetf"`) to `pmdp.AssetClass` values for provider queries.
- **`price_on` list support**: Pipe-mode `price_on` field now accepts both a single dict and a list (compatibility with pmdp ≥ 0.4.1 which emits a list).
- **`resolve` symbol fallback**: When building a `SearchResult` from a registry component, falls back to `res_comp.symbol` (not `res_comp.name`) and uses `res_comp.name or res_comp.symbol` for display.

### Dependencies
- `pydantic-market-data` bumped to `>=0.4.0`

## [0.2.10] - 2026-05-08

### Added
- **Synthetic CASH result for same-currency pair**: `resolve_currency("EUR", target_currency=Currency("EUR"))` now returns a synthetic `SearchResult` with `asset_class=CASH` and `instrument_type=CASH` instead of `None`, enabling callers to treat same-currency no-ops uniformly.

### Changed
- **`SearchResult.provider` is now optional**: `provider: ProviderName | None = None` — allows results built without a known provider (e.g. synthetic CASH) to be constructed without a dummy value.
- **Provider display in `resolve` output**: Provider label is only printed when a provider is present, preventing a crash on provider-less results.
- **`fetch_price` provider fallback**: `resolve_security` now falls back to `ProviderName.YAHOO` when `res.provider` is `None`, avoiding a type error on price fetch.

## [0.2.9] - 2026-05-08

### Added
- **`resolve --figi`**: The `resolve` command now accepts `--figi` to look up a security by its FIGI identifier via OpenFIGI. Passed through JSONL pipe records as well.
- **`resolve --report-price`**: Made `--report-price` a proper named CLI flag (was a bare `bool` default).
- **`resolve --no-save`**: Renamed `--dry-run` to `--no-save` for clearer intent.
- **OpenFIGI resolution in finder**: New `_resolve_via_openfigi` function resolves ISIN/FIGI to ticker with FIGI → ISIN priority fallback.
- **`resolve` structured output priority**: Registry hits now emit the registry `Instrument` record; new discoveries emit the newly persisted `Instrument`. Falls back to `SearchResult` when no instrument is available.
- **`registry.reload()`**: New method on `InstrumentRegistry` to load additional data from a path and rebuild indices without recreating the instance.
- **ETP type mapping**: `"ETP"` asset class now correctly maps to `InstrumentType.ETF`.
- **`_infer_types` `security_type` parameter**: Type inference now accepts an optional `security_type` (more specific than `asset_class`) for better classification from OpenFIGI metadata.

### Changed
- **Registry `find_candidates` lookup order**: FIGI (globally unique, short-circuit) → ISIN+currency filter → symbol/name (only when no strict identifier). Currency filter now applied at ISIN and symbol stages.
- **JSONL pipe `price_on` passthrough**: `price_on` object in piped JSONL records is now parsed and forwarded to the resolver.

### Removed
- **Bundled data files**: Removed `data/instruments/manual.yaml`, `currencies.yaml`, and `etfs.yaml` from source tree (data is managed externally).

### Dependencies
- `pydantic-market-data` bumped to `>=0.3.2`
- `py-yfinance` bumped to `>=0.1.16`
- `py-ftmarkets` bumped to `>=0.5.0`

## [0.2.8] - 2026-04-23

### Added
- **`fetch --figi`**: The `fetch` command now accepts `--figi` to resolve a security by its FIGI identifier. FT Markets is the only provider that supports FIGI; the option resolves via `FTDataSource.resolve` which already prioritises FIGI over ISIN/symbol.
- **Registry FIGI lookup**: `InstrumentRegistry.find_candidates` now queries the `_by_figi` index when `SecurityQuery.figi` is set. The index was already built at load time but never queried.

### Changed
- **`SecurityCriteria` → `SecurityQuery`**: Migrated to the renamed model introduced in `pydantic-market-data` 0.3.0. All internal APIs, Protocol definitions, CLI commands, and tests updated.
- **`price_on` field**: Replaced the flat `target_date` / `target_price` fields (removed in `pydantic-market-data` 0.3.0) with the combined `price_on: PriceOnDate` model throughout the CLI and finder.

### Dependencies
- `pydantic-market-data` bumped to `>=0.3.0`
- `agentyper` pin relaxed from `==0.1.12` to `>=0.1.12`
- `py-yfinance` bumped to `>=0.1.15`
- `py-ftmarkets` bumped to `>=0.4.0`

## [0.2.7] - 2026-04-23

### Added
- **`resolve` JSONL stdin**: The `resolve` command now accepts multiple records piped as JSONL (one JSON object per line). Single JSON objects, JSON arrays, and JSONL streams are all detected automatically. Each record is resolved independently; blank lines are skipped and any malformed line exits immediately with a clear error.

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
