.PHONY: clean-pyc clean-test clean-all help

help:
	@echo "Usage: make <target>"
	@echo "  clean-pyc    - Remove python cache files"
	@echo "  clean-test   - Remove test and mypy caches"
	@echo "  clean-all    - Remove all caches and temporary files"

clean-pyc:
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name "*.pyc" -delete

clean-test:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache

clean-all: clean-pyc clean-test
	@echo "All temporary caches cleared."

typecheck:
	ty  # Or 'pyright' if ty isn't aliased in your CI

run:
	uv run -m ingestion