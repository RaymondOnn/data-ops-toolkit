import os
import re
from contextlib import suppress
from typing import Any

from libs.utils.dict import flatten_dict
from simpleeval import simple_eval


class SafeDict(dict):
    """Dictionary subclass that preserves unpopulated template keys (e.g. {missing_key})."""

    def __missing__(self, key: str) -> str:
        return f"{{{key}}}"


ENV_PATTERN = re.compile(r"\$?\$\{([^:-]+)(?::-([^}]*))?\}|\$([a-zA-Z_][a-zA-Z0-9_]*)")
NAMED_TEMPLATE_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_\.]*)\}")
EXPR_PATTERN = re.compile(r"\$\{\{\s*(.*?)\s*\}\}")  # Matches ${{ expression }}


class TemplateEngine:
    """Generic string templating and expression evaluation engine.

    Supports variable interpolation, missing key preservation, environment variable
    expansion, simpleeval expression evaluation, and recursive structure rendering.

    Features & Order of Operations:
        1. Expression Evaluation (${{ expr }}): Evaluates dynamic expressions using simpleeval.
           Returns primitive types directly (bool, int, list, etc.) if matched fully.
        2. Named Variable Substitution ({key} or {nested.key}): Interpolates variables from the context.
           Unmatched template tags remain untouched (e.g., '{missing_key}').
        3. Environment Variables (${VAR} or ${VAR:-default}): Expands environment variables.
        4. Recursive Processing: Recursively renders strings inside dictionaries, lists, and primitives.

    Usage Examples:
        >>> engine = TemplateEngine({"job_id": "job_101", "meta": {"env": "prod"}})

        # 1. Variable Interpolation & Dot Notation
        >>> engine.render_string("Processing {job_id} in {meta.env}")
        'Processing job_101 in prod'

        # 2. Missing Key Fallback (Preserved)
        >>> engine.render_string("Hello {user_name}, job: {job_id}")
        'Hello {user_name}, job: job_101'

        # 3. Environment Variables
        >>> os.environ["API_KEY"] = "secret_abc"
        >>> engine.render_string("Key: ${API_KEY}, Host: ${DB_HOST:-localhost}")
        'Key: secret_abc, Host: localhost'

        # 4. Dynamic Expression Evaluation (${{ expr }})
        >>> engine.render_string("${{ meta.env == 'prod' and 1 > 0 }}")
        True

        # 5. Recursive Data Structure Resolution
        >>> config = {
        ...     "query": "SELECT * FROM {job_id}",
        ...     "is_prod": "${{ meta.env == 'prod' }}",
        ... }
        >>> engine.render(config)
        {'query': 'SELECT * FROM job_101', 'is_prod': True}
    """

    def __init__(self, context: dict[str, Any] | None = None):
        """Initialize engine with a base context dictionary.

        Args:
            context: Dictionary of context variables for interpolation and evaluation.
        """
        self.context: dict[str, Any] = context or {}

    def update_context(self, new_vars: dict[str, Any]) -> None:
        """Merge additional context variables into the current context map."""
        self.context.update(new_vars)

    def _get_safe_context(self) -> SafeDict:
        """Flattens nested dictionaries for dot-notation lookups and wraps in SafeDict."""
        flat = flatten_dict(self.context)
        safe = SafeDict(flat)
        for k, v in self.context.items():
            if k not in safe:
                safe[k] = v
        return safe

    def render_string(self, value: str) -> Any:
        """Renders a single template string.

        Args:
            value: The string to template.

        Returns:
            The rendered string, or the evaluated type if value is an expression.
        """
        if not value or not isinstance(value, str):
            return value

        # 1. Evaluate Dynamic Expressions: ${{ manifest.count > 0 }}
        if match := EXPR_PATTERN.fullmatch(value.strip()):
            # Fall through to string interpolation if expression fails
            with suppress(Exception):
                return simple_eval(match.group(1), names=self.context)

        mutated = value

        # 2. Resolve {named.templates} while preserving missing keys via SafeDict
        if NAMED_TEMPLATE_PATTERN.search(mutated):
            clean_template = mutated.replace("@format ", "")
            safe_ctx = self._get_safe_context()

            def _replacer(m: re.Match) -> str:
                key = m.group(1).strip()
                if key in safe_ctx:
                    val = safe_ctx[key]
                    return str(val) if val is not None else ""
                return safe_ctx[key]  # Triggers SafeDict.__missing__() -> f"{{{key}}}"

            mutated = NAMED_TEMPLATE_PATTERN.sub(_replacer, clean_template)

        # 3. Resolve Environment Variables: ${VAR:-default}
        if "$" in mutated:

            def _env_replacer(m: re.Match) -> str:
                var_name = m.group(1) or m.group(3)
                default = m.group(2)
                return os.getenv(var_name, default if default is not None else "")

            mutated = ENV_PATTERN.sub(_env_replacer, mutated)

        return mutated

    def render(self, target: Any) -> Any:
        """Recursively resolves templates across dicts, lists, and primitives.

        Args:
            target: Any data structure (dict, list, str, or primitive).

        Returns:
            Rendered data structure with all string placeholders resolved.
        """
        if isinstance(target, dict):
            return {k: self.render(v) for k, v in target.items()}
        if isinstance(target, list):
            return [self.render(v) for v in target]
        if isinstance(target, str):
            return self.render_string(target)
        return target
