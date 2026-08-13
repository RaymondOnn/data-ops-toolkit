# Decision: Thin Entry Point.
# Registration of commands and sub-apps is encapsulated in src/cli/__init__.py.
# This file serves only as the bootstrapping layer for the Typer application.
from libs.utils.exceptions import install_exception_hooks

from src.cli import app

if __name__ == "__main__":
    install_exception_hooks()
    app()
