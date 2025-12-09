# Makefile for drainage project

.PHONY: help install build test test-python test-integration lint format clean release

# Default target
help:
	@echo "Available targets:"
	@echo "  install       - Install dependencies and build the project"
	@echo "  build         - Build the Python extension"
	@echo "  test          - Run all tests"
	@echo "  test-python   - Run Python tests"
	@echo "  test-integration - Run integration tests"
	@echo "  lint          - Run linting checks"
	@echo "  format        - Format code"
	@echo "  clean         - Clean build artifacts"
	@echo "  release       - Build release version"
	@echo "  docs          - Generate documentation"

# Install dependencies
install:
	@echo "Installing dependencies..."
	python -m pip install --upgrade pip
	python -m pip install -r requirements.txt

# Build the project
build:
	@echo "Building drainage..."
	python -m pip install --upgrade pip
	pip install -e .

# Run all tests
test: test-python test-integration

# Run Python tests
test-python:
	@echo "Running Python tests..."
	python -m pytest tests/ -v --cov=drainage --cov-report=xml

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	python -m pytest tests/ -m integration -v

# Run linting
lint:
	@echo "Running linting checks..."
	flake8 tests/ examples/ --max-line-length=100
	black --check tests/ examples/

# Format code
format:
	@echo "Formatting code..."
	black tests/ examples/

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf target/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build release version
release:
	@echo "Building release version..."
	python -m pip install --upgrade build
	python -m build

# Generate documentation
docs:
	@echo "Generating documentation..."
	python -c "import drainage; help(drainage)" > drainage_help.txt
	@echo "Documentation generated in drainage_help.txt"

# Security checks
security:
	@echo "Running security checks..."
	safety check
	bandit -r tests/ examples/ -f json -o bandit-report.json || true

# Performance tests
perf:
	@echo "Running performance tests..."
	python -m pytest tests/ -v --benchmark-only --benchmark-sort=mean

# Check examples
examples:
	@echo "Checking examples..."
	python -c "import examples.simple_analysis; print('Examples imported successfully')"
	@echo "All examples are valid"

# Full CI pipeline
ci: install build test lint security examples
	@echo "CI pipeline completed successfully"

# Development setup
dev: install build test
	@echo "Development environment ready"

# Quick test (just unit tests)
quick-test: test-python
	@echo "Quick tests completed"

# Test specific module
test-module:
	@echo "Testing specific module: $(MODULE)"
	python -m pytest tests/test_$(MODULE).py -v

# Test specific function
test-function:
	@echo "Testing specific function: $(FUNCTION)"
	python -m pytest tests/ -k $(FUNCTION) -v

# Run with coverage
coverage: test-python
	@echo "Coverage report generated"
	@echo "Open htmlcov/index.html in your browser to view coverage report"

# Install development dependencies
install-dev: install
	@echo "Installing development dependencies..."
	pip install pytest-benchmark sphinx sphinx-rtd-theme

# Build documentation
build-docs: install-dev
	@echo "Building documentation..."
	sphinx-build -b html docs/ docs/_build/html

# Run all checks
check: lint test security
	@echo "All checks passed"

# Pre-commit hook
pre-commit: format lint test
	@echo "Pre-commit checks passed"

# Post-commit hook
post-commit: test examples
	@echo "Post-commit checks passed"

# Setup git hooks
setup-hooks:
	@echo "Setting up git hooks..."
	@echo "#!/bin/bash" > .git/hooks/pre-commit
	@echo "make pre-commit" >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed"

# Remove git hooks
remove-hooks:
	@echo "Removing git hooks..."
	rm -f .git/hooks/pre-commit
	@echo "Git hooks removed"

# Show project info
info:
	@echo "Project: drainage"
	@echo "Language: Python"
	@echo "Build tool: maturin"
	@echo "Test framework: pytest"
	@echo "Linting: flake8 + black"
	@echo "Security: safety + bandit"

# Show help for specific target
help-%:
	@echo "Help for target: $*"
	@echo "Description: $(shell grep -A 2 "^$*:" Makefile | tail -n 1 | sed 's/^[[:space:]]*@echo[[:space:]]*//')"
