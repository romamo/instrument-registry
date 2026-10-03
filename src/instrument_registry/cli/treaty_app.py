"""The treaty app every ``instrument-reg`` command registers on."""

import datetime
import importlib.metadata
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn, Self

from treaty import App, Ctx, Exit, Flag

from ..interfaces import ProviderName
from ..registry import InstrumentRegistry
from . import common

app = App(
    "instrument-reg",
    version=importlib.metadata.version("instrument-registry"),
    description="Resolve and maintain canonical financial instrument records",
)

app.scalar(
    datetime.date,
    parse=datetime.date.fromisoformat,
    pattern=r"\d{4}-\d{2}-\d{2}",
    serialize=datetime.date.isoformat,
)

app.exit_code(
    "MISSING_PROVIDER",
    79,
    description="A live-data provider the command needs is not installed; nothing was changed",
    retryable=False,
    side_effects="none",
)
app.exit_code(
    "LINT_FAILED",
    80,
    description="A registry file failed validation; data holds the full lint report",
    retryable=False,
    side_effects="none",
    suggestion="fix the entries listed in data.errors, then lint again",
)


def raise_missing_provider(provider: ProviderName | None, command_name: str) -> NoReturn:
    raise Exit.MISSING_PROVIDER(
        common.provider_install_message(provider, command_name),
        context={"provider": provider.value if provider else None},
        suggestion="uv tool install 'instrument-registry[providers]'",
    )


def require_live_providers(provider: ProviderName | None, command_name: str) -> None:
    from ..finder import get_available_providers

    if not get_available_providers():
        raise_missing_provider(provider, command_name)


@dataclass(frozen=True, slots=True, kw_only=True)
class RegistryScope:
    registry_path: tuple[str, ...] = Flag(
        default=(),
        description=(
            "User registry file or directory to read; repeat or comma-separate for several. "
            "The first one is also where writes go"
        ),
    )
    no_bundled: bool = Flag(default=False, description="Exclude bundled registry data")

    def registry_paths(self) -> list[str]:
        return common.split_registry_paths(self.registry_path)


@dataclass(frozen=True, slots=True)
class Registry:
    lookup: InstrumentRegistry

    @classmethod
    def acquire(cls, args: RegistryScope, ctx: Ctx) -> Self:
        return cls(common.open_registry(args.registry_paths(), bundled=not args.no_bundled))


@dataclass(frozen=True, slots=True)
class WriteTarget:
    """Where new records go: the first --registry-path, else INSTRUMENT_REGISTRY_PATH"""

    path: Path | None

    @classmethod
    def acquire(cls, args: RegistryScope, ctx: Ctx) -> Self:
        return cls(common.write_target(args.registry_paths(), ctx.env))

    def require(self) -> Path:
        if self.path is None:
            raise Exit.PRECONDITION(
                "No registry write path configured.",
                context={"env_var": common.REGISTRY_PATH_ENV_VAR},
                suggestion=f"set {common.REGISTRY_PATH_ENV_VAR} or pass --registry-path",
            )
        return self.path
