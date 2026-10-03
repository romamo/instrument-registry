from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from ..interfaces import ProviderName
from ..registry import InstrumentRegistry, get_registry

REGISTRY_PATH_ENV_VAR = "INSTRUMENT_REGISTRY_PATH"


def split_registry_paths(registry_path: Sequence[str]) -> list[str]:
    values: list[str] = []
    for item in registry_path:
        values.extend(part.strip() for part in item.split(",") if part.strip())
    return values


def open_registry(registry_paths: list[str], *, bundled: bool) -> InstrumentRegistry:
    extra_paths = [p for p in (Path(s).expanduser() for s in registry_paths) if p.exists()]
    return get_registry(include_bundled=bundled, extra_paths=extra_paths or None)


def write_target(registry_paths: list[str], env: Mapping[str, str]) -> Path | None:
    """The file or directory a write goes to: the first --registry-path, else the env var"""
    if registry_paths:
        return Path(registry_paths[0]).expanduser()
    env_path = env.get(REGISTRY_PATH_ENV_VAR)
    if env_path:
        return Path(env_path).expanduser()
    return None


def provider_install_message(provider: ProviderName | None, command_name: str) -> str:
    if provider == ProviderName.YAHOO:
        requirement = "the yahoo provider (`py-yfinance`)"
    elif provider == ProviderName.FT:
        requirement = "the ft provider (`py-ftmarkets`)"
    else:
        requirement = "optional live-data providers"

    return (
        f"`{command_name}` requires {requirement}. "
        "Install them with: uv tool install 'instrument-registry[providers]'"
    )


def is_isin(value: str) -> bool:
    """Return True if value looks like an ISIN."""
    upper = value.upper()
    return len(value) == 12 and upper[:2].isalpha() and upper.isalnum()


def is_ibkr_conid(value: str) -> bool:
    """Return True if value is a numeric string (IBKR conid)."""
    return value.isdigit()
