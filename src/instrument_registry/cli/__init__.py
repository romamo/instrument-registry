from __future__ import annotations

import importlib.metadata
import sys

import agentyper as typer

from . import common
from . import fetch as _fetch  # noqa: F401  # registers `fetch` on the treaty app
from .add import command as add_command
from .lint import command as lint_command
from .resolve import command as resolve_command
from .treaty_app import app as treaty_app

app = typer.Agentyper(
    name="instrument-reg",
    version=importlib.metadata.version("instrument-registry"),
    help=(
        "Instrument Registry CLI Application. "
        "`fetch` and `manifest` run on treaty: see `instrument-reg fetch --help`"
    ),
)


@app.callback()
def root(
    ctx: typer.Context,
) -> None:
    del ctx
    verbosity = common.explicit_verbosity()
    common.configure_state(
        verbosity=verbosity,
        registry_path=None,
        bundled=True,
    )


app.command(name="resolve")(resolve_command)
app.command(name="lint")(lint_command)
app.command(name="add")(add_command)

AppCLI = app

# Re-export for compatibility with existing tests and callers.
get_registry = common.get_registry
setup_logging = common.setup_logging


# Commands already on treaty; the rest still run on agentyper until migrated
TREATY_COMMANDS = frozenset({"fetch", "manifest"})


def main(args: list[str] | None = None) -> None:
    old_argv = sys.argv
    argv = list(old_argv[1:] if args is None else args)
    sys.argv = ["instrument-reg", *argv]
    try:
        if argv[:1] and argv[0] in TREATY_COMMANDS:
            sys.exit(treaty_app.run(argv))
        app(args=args)
    finally:
        sys.argv = old_argv


if __name__ == "__main__":
    main()
