"""
CLI Package Initializer.

Decision: Self-Configuring Package (ADR 015).
By moving registration and module discovery here, we ensure that any
module importing 'app' receives a fully wired Typer instance, preventing
circular dependencies between implementation modules and the entry point.
"""

from .commands import daemon, execution, maintenance  # noqa: F401
from .groups.doctor import doctor_app
from .groups.test.test import test_app
from .state import app, apply_global_options

# 1. Register Global Options Callback
app.callback()(apply_global_options)

# 2. Register Sub-apps (Namespaced commands)
app.add_typer(doctor_app, name="doctor")
app.add_typer(test_app, name="test")

# 3. Trigger Command Discovery
# Implementation modules are imported above using redundant aliases to satisfy
# static analysis while ensuring their @app.command decorators are executed.

__all__ = ["app"]
