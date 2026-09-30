"""The treaty app: commands migrated off agentyper, routed here by ``cli.main``."""

import importlib.metadata
from dataclasses import dataclass
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

app.exit_code(
    "MISSING_PROVIDER",
    79,
    description="A live-data provider the command needs is not installed; nothing was changed",
    retryable=False,
    side_effects="none",
)


def raise_missing_provider(provider: ProviderName | None, command_name: str) -> NoReturn:
    raise Exit.MISSING_PROVIDER(
        common.provider_install_message(provider=provider, command_name=command_name),
        context={"provider": provider.value if provider else None},
        suggestion="uv tool install 'instrument-registry[providers]'",
    )


def require_live_providers(provider: ProviderName, command_name: str) -> None:
    from ..finder import get_available_providers

    if not get_available_providers():
        raise_missing_provider(provider, command_name)


@dataclass(frozen=True, slots=True, kw_only=True)
class RegistryScope:
    registry_path: tuple[str, ...] = Flag(
        default=(),
        description="User registry file or directory to read; repeat or comma-separate for several",
    )
    no_bundled: bool = Flag(default=False, description="Exclude bundled registry data")


@dataclass(frozen=True, slots=True)
class Registry:
    lookup: InstrumentRegistry

    @classmethod
    def acquire(cls, args: RegistryScope, ctx: Ctx) -> Self:
        paths = common.split_registry_paths(list(args.registry_path))
        return cls(common.open_registry(paths, bundled=not args.no_bundled))
