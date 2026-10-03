import sys

# Each module registers its commands on the treaty app
from . import add as _add  # noqa: F401
from . import fetch as _fetch  # noqa: F401
from . import lint as _lint  # noqa: F401
from . import resolve as _resolve  # noqa: F401
from .treaty_app import app

AppCLI = app


def main(args: list[str] | None = None) -> None:
    if args is None:
        app.main()
    sys.exit(app.run(args))


if __name__ == "__main__":
    main()
