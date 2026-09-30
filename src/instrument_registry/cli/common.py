from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import agentyper as typer

from ..registry import InstrumentRegistry, get_registry

logger = logging.getLogger(__name__)
REGISTRY_PATH_ENV_VAR = "INSTRUMENT_REGISTRY_PATH"
REGISTRY_PATH_OPTION = typer.Option(
    None,
    "--registry-path",
    help="User registry file or directory to read from or write to",
)
NO_BUNDLED_OPTION = typer.Option(
    False,
    "--no-bundled",
    is_flag=True,
    help="Exclude bundled registry data for this command",
)


@dataclass
class CLIState:
    verbosity: int = 0
    registry_paths: list[str] = field(default_factory=list)
    bundled: bool = True

    @property
    def verbose(self) -> bool:
        return self.verbosity >= 1

    @property
    def debug(self) -> bool:
        return self.verbosity >= 2


STATE = CLIState()


def setup_logging(verbosity: int) -> None:
    """Set up logging from verbosity count (0=WARNING, 1=INFO, 2+=DEBUG)."""
    if verbosity >= 2:
        level = logging.DEBUG
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    elif verbosity >= 1:
        level = logging.INFO
        fmt = "[%(levelname)s] %(name)s: %(message)s"
    else:
        level = logging.WARNING
        fmt = "%(message)s"

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
        force=True,
    )

    if verbosity < 2:
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("py_ibkr").setLevel(logging.WARNING)


def current_format() -> str:
    explicit = explicit_output_format()
    if explicit:
        return explicit
    try:
        ctx = typer.get_current_context()
    except Exception:
        ctx = None
    if ctx is not None:
        format_ = getattr(ctx, "format_", None)
        if isinstance(format_, str) and format_:
            return format_
    return "table"


def explicit_output_format(args: list[str] | None = None) -> str | None:
    argv = list(sys.argv[1:] if args is None else args)
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--format":
            if i + 1 < len(argv):
                return argv[i + 1]
            return None
        if arg.startswith("--format="):
            return arg.split("=", 1)[1]
        i += 1
    return None


def explicit_verbosity(args: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if args is None else args)
    verbosity = 0
    for arg in argv:
        if arg == "--debug":
            verbosity = max(verbosity, 2)
        elif arg == "--verbose":
            verbosity += 1
        elif arg.startswith("-") and not arg.startswith("--") and set(arg[1:]) == {"v"}:
            verbosity += len(arg) - 1
    return verbosity


def emit_json_event(data: Any) -> bool:
    if current_format() != "json":
        return False
    print(json.dumps(data), flush=True)
    return True


def exit_with_error(message: str, code: int = 1, error_type: str = "ValidationError") -> None:
    typer.exit_error(message, code=code, error_type=error_type, format_=current_format())


def split_registry_paths(registry_path: list[str] | None) -> list[str]:
    values: list[str] = []
    for item in registry_path or []:
        values.extend(part.strip() for part in item.split(",") if part.strip())
    return values


def configure_state(*, verbosity: int, registry_path: str | None, bundled: bool) -> None:
    STATE.verbosity = verbosity
    STATE.registry_paths = split_registry_paths([registry_path] if registry_path else None)
    STATE.bundled = bundled
    setup_logging(verbosity)


def configure_registry_scope(
    *,
    ctx: typer.Context,
    registry_path: str | None,
    no_bundled: bool,
) -> None:
    del ctx
    STATE.verbosity = explicit_verbosity()
    STATE.registry_paths = split_registry_paths([registry_path] if registry_path else None)
    STATE.bundled = not no_bundled


def primary_registry_path() -> Path | None:
    if not STATE.registry_paths:
        return None
    return Path(STATE.registry_paths[0]).expanduser()


def open_registry(registry_paths: list[str], *, bundled: bool) -> InstrumentRegistry:
    extra_paths = [p for p in (Path(s).expanduser() for s in registry_paths) if p.exists()]
    return get_registry(include_bundled=bundled, extra_paths=extra_paths or None)


def registry() -> Any:
    return open_registry(STATE.registry_paths, bundled=STATE.bundled)


def require_write_target() -> Path:
    path = primary_registry_path()
    if path is not None:
        return path

    env_path = os.getenv(REGISTRY_PATH_ENV_VAR)
    if env_path:
        return Path(env_path).expanduser()

    raise ValueError(
        "No registry write path configured. Set INSTRUMENT_REGISTRY_PATH or pass --registry-path."
    )


def provider_install_message(provider: Any = None, command_name: str | None = None) -> str:
    provider_name = getattr(provider, "value", provider)
    if provider_name == "yahoo":
        requirement = "the yahoo provider (`py-yfinance`)"
    elif provider_name == "ft":
        requirement = "the ft provider (`py-ftmarkets`)"
    else:
        requirement = "optional live-data providers"

    prefix = f"`{command_name}` requires " if command_name else "This operation requires "
    return (
        f"{prefix}{requirement}. "
        "Install them with: uv tool install 'instrument-registry[providers]'"
    )


def exit_missing_provider(provider: Any = None, command_name: str | None = None) -> None:
    exit_with_error(
        provider_install_message(provider=provider, command_name=command_name),
        error_type="MissingProviderError",
    )


def require_live_providers(command_name: str, provider: Any = None) -> None:
    from ..finder import get_available_providers

    if get_available_providers():
        return

    exit_missing_provider(provider=provider, command_name=command_name)


def is_isin(value: str) -> bool:
    """Return True if value looks like an ISIN."""
    upper = value.upper()
    return len(value) == 12 and upper[:2].isalpha() and upper.isalnum()


def is_ibkr_conid(value: str) -> bool:
    """Return True if value is a numeric string (IBKR conid)."""
    return value.isdigit()
