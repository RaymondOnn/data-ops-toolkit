# Decision: Thin Entry Point.
# Registration of commands and sub-apps is encapsulated in src/cli/__init__.py.
# This file serves only as the bootstrapping layer for the Typer application.
from apps.ingestion.src.cli import app
from libs.utils.exceptions import install_exception_hooks

if __name__ == "__main__":
    install_exception_hooks()
    app()
