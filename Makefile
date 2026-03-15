# NetSim Development Makefile

.PHONY: help venv clean-venv dev install check check-ci lint format test qt build clean check-dist publish-test publish info hooks check-python

.DEFAULT_GOAL := help

# --------------------------------------------------------------------------
# Python interpreter detection
# --------------------------------------------------------------------------
VENV_BIN := $(PWD)/venv/bin

PY_BEST := $(shell for v in 3.14 3.13 3.12 3.11; do command -v python$$v >/dev/null 2>&1 && { echo python$$v; exit 0; }; done; command -v python3 2>/dev/null || command -v python 2>/dev/null)

PY_PATH := $(shell command -v python3 2>/dev/null || command -v python 2>/dev/null)

PYTHON ?= $(if $(wildcard $(VENV_BIN)/python),$(VENV_BIN)/python,$(if $(PY_PATH),$(PY_PATH),$(if $(PY_BEST),$(PY_BEST),python3)))

PIP := $(PYTHON) -m pip
PYTEST := $(PYTHON) -m pytest
RUFF := $(PYTHON) -m ruff
PRECOMMIT := $(PYTHON) -m pre_commit

# --------------------------------------------------------------------------
# Help
# --------------------------------------------------------------------------
help:
	@echo "NetSim Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make venv          - Create virtual environment (./venv)"
	@echo "  make dev           - Full dev setup (venv + deps + pre-commit hooks)"
	@echo "  make install       - Install package only (no dev deps)"
	@echo "  make clean-venv    - Remove virtual environment"
	@echo ""
	@echo "Code Quality & Testing:"
	@echo "  make check         - Pre-commit (auto-fix) + tests + lint"
	@echo "  make check-ci      - Lint + tests (non-mutating, CI entrypoint)"
	@echo "  make lint          - Linting only (ruff + pyright)"
	@echo "  make format        - Auto-format with ruff"
	@echo "  make test          - Run tests with coverage"
	@echo "  make qt            - Quick tests (no coverage, no benchmarks)"
	@echo ""
	@echo "Build & Publish:"
	@echo "  make build         - Build distribution packages"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make check-dist    - Check packages with twine"
	@echo "  make publish-test  - Publish to Test PyPI"
	@echo "  make publish       - Publish to PyPI"
	@echo ""
	@echo "Utilities:"
	@echo "  make info          - Show project info"
	@echo "  make hooks         - Run pre-commit on all files"
	@echo "  make check-python  - Verify venv Python version"

# --------------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------------
dev:
	@echo "Setting up development environment..."
	@if [ ! -x "$(VENV_BIN)/python" ]; then \
		if [ -z "$(PY_BEST)" ]; then \
			echo "Error: No Python interpreter found"; \
			exit 1; \
		fi; \
		echo "Creating virtual environment with $(PY_BEST) ..."; \
		$(PY_BEST) -m venv venv || { echo "Failed to create venv"; exit 1; }; \
		$(VENV_BIN)/python -m pip install -U pip setuptools wheel; \
	fi
	@echo "Installing dev dependencies..."
	@$(VENV_BIN)/python -m pip install -e '.[dev]'
	@echo "Installing pre-commit hooks..."
	@$(VENV_BIN)/python -m pre_commit install --install-hooks
	@echo "Dev environment ready. Activate with: source venv/bin/activate"
	@$(MAKE) check-python

venv:
	@if [ -z "$(PY_BEST)" ]; then \
		echo "Error: No Python interpreter found"; \
		exit 1; \
	fi
	@echo "Creating virtual environment with $(PY_BEST) ..."
	@$(PY_BEST) -m venv venv
	@$(VENV_BIN)/python -m pip install -U pip setuptools wheel
	@echo "venv ready. Activate with: source venv/bin/activate"

clean-venv:
	@rm -rf venv/

install:
	@$(PIP) install -e .

# --------------------------------------------------------------------------
# Code Quality & Testing
# --------------------------------------------------------------------------
check:
	@echo "Running complete checks..."
	@$(PRECOMMIT) run --all-files || true
	@$(PRECOMMIT) run --all-files
	@$(MAKE) test
	@$(MAKE) lint

check-ci:
	@echo "Running CI checks (lint + tests)..."
	@$(MAKE) lint
	@$(MAKE) test

lint:
	@$(RUFF) format --check .
	@$(RUFF) check .
	@$(PYTHON) -m pyright

format:
	@$(RUFF) format .

test:
	@$(PYTEST)

qt:
	@$(PYTEST) --no-cov -m "not slow and not benchmark"

hooks:
	@$(PRECOMMIT) run --all-files

# --------------------------------------------------------------------------
# Build & Publish
# --------------------------------------------------------------------------
build:
	@$(PYTHON) -m build

clean:
	@rm -rf build/ dist/ *.egg-info/ netsim.egg-info/

check-dist:
	@$(PYTHON) -m twine check dist/*

publish-test:
	@$(PYTHON) -m twine upload --repository testpypi dist/*

publish:
	@$(PYTHON) -m twine upload dist/*

# --------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------
info:
	@echo "Python:  $$($(PYTHON) --version 2>&1)"
	@echo "pip:     $$($(PIP) --version 2>&1 | head -1)"
	@echo "ruff:    $$($(RUFF) --version 2>&1 || echo 'not installed')"
	@echo "pyright: $$($(PYTHON) -m pyright --version 2>&1 || echo 'not installed')"
	@echo "Git:     $$(git describe --tags --always 2>/dev/null || echo 'no tags')"

check-python:
	@VENV_PY=$$($(VENV_BIN)/python --version 2>&1 | awk '{print $$2}' | cut -d. -f1,2); \
	BEST_PY=$$($(PY_BEST) --version 2>&1 | awk '{print $$2}' | cut -d. -f1,2); \
	if [ "$$VENV_PY" != "$$BEST_PY" ]; then \
		echo "Warning: venv uses Python $$VENV_PY but $$BEST_PY is available"; \
		echo "Run: make clean-venv && make dev"; \
	fi
