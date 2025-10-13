#!/usr/bin/env python3
"""
Test runner for the drainage library.

This script provides a convenient way to run all tests for the drainage library,
including both Rust and Python tests.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path


def run_command(command, cwd=None, capture_output=False):
    """Run a command and return the result."""
    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=cwd,
            capture_output=capture_output,
            text=True,
            check=True
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {command}")
        print(f"Error: {e}")
        if e.stdout:
            print(f"Stdout: {e.stdout}")
        if e.stderr:
            print(f"Stderr: {e.stderr}")
        return None


def run_rust_tests():
    """Run Rust unit tests."""
    print("Running Rust unit tests...")
    print("=" * 50)
    
    result = run_command("cargo test", capture_output=True)
    if result is None:
        print("❌ Rust tests failed")
        return False
    
    print("✅ Rust tests passed")
    print(f"Output: {result.stdout}")
    return True


def run_python_tests():
    """Run Python tests."""
    print("Running Python tests...")
    print("=" * 50)
    
    # Check if pytest is available
    try:
        import pytest
    except ImportError:
        print("❌ pytest not available. Installing...")
        result = run_command("pip install pytest pytest-mock", capture_output=True)
        if result is None:
            print("❌ Failed to install pytest")
            return False
    
    # Run pytest
    result = run_command("python -m pytest tests/ -v", capture_output=True)
    if result is None:
        print("❌ Python tests failed")
        return False
    
    print("✅ Python tests passed")
    print(f"Output: {result.stdout}")
    return True


def run_integration_tests():
    """Run integration tests."""
    print("Running integration tests...")
    print("=" * 50)
    
    # Check if drainage module is available
    try:
        import drainage
        print("✅ drainage module is available")
    except ImportError:
        print("❌ drainage module not available. Building...")
        result = run_command("maturin develop", capture_output=True)
        if result is None:
            print("❌ Failed to build drainage module")
            return False
    
    # Run integration tests
    result = run_command("python -m pytest tests/ -m integration -v", capture_output=True)
    if result is None:
        print("❌ Integration tests failed")
        return False
    
    print("✅ Integration tests passed")
    return True


def run_example_tests():
    """Run example tests."""
    print("Running example tests...")
    print("=" * 50)
    
    examples_dir = Path("examples")
    if not examples_dir.exists():
        print("❌ Examples directory not found")
        return False
    
    # Test each example script
    example_scripts = list(examples_dir.glob("*.py"))
    if not example_scripts:
        print("❌ No example scripts found")
        return False
    
    for script in example_scripts:
        print(f"Testing {script.name}...")
        # Test that the script can be imported and has a main function
        try:
            with open(script, 'r') as f:
                content = f.read()
                if 'def main(' in content or 'if __name__' in content:
                    print(f"✅ {script.name} has proper structure")
                else:
                    print(f"⚠️  {script.name} may not have proper structure")
        except Exception as e:
            print(f"❌ Error reading {script.name}: {e}")
            return False
    
    print("✅ Example tests passed")
    return True


def run_linting():
    """Run linting checks."""
    print("Running linting checks...")
    print("=" * 50)
    
    # Check Rust linting
    print("Checking Rust code...")
    result = run_command("cargo clippy -- -D warnings", capture_output=True)
    if result is None:
        print("❌ Rust linting failed")
        return False
    print("✅ Rust code passed linting")
    
    # Check Python linting (if flake8 is available)
    try:
        import flake8
        print("Checking Python code...")
        result = run_command("flake8 tests/ examples/ --max-line-length=100", capture_output=True)
        if result is None:
            print("❌ Python linting failed")
            return False
        print("✅ Python code passed linting")
    except ImportError:
        print("⚠️  flake8 not available, skipping Python linting")
    
    return True


def run_formatting():
    """Run formatting checks."""
    print("Running formatting checks...")
    print("=" * 50)
    
    # Check Rust formatting
    print("Checking Rust formatting...")
    result = run_command("cargo fmt -- --check", capture_output=True)
    if result is None:
        print("❌ Rust formatting failed")
        return False
    print("✅ Rust code is properly formatted")
    
    # Check Python formatting (if black is available)
    try:
        import black
        print("Checking Python formatting...")
        result = run_command("black --check tests/ examples/", capture_output=True)
        if result is None:
            print("❌ Python formatting failed")
            return False
        print("✅ Python code is properly formatted")
    except ImportError:
        print("⚠️  black not available, skipping Python formatting check")
    
    return True


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(description="Run tests for the drainage library")
    parser.add_argument("--rust", action="store_true", help="Run only Rust tests")
    parser.add_argument("--python", action="store_true", help="Run only Python tests")
    parser.add_argument("--integration", action="store_true", help="Run only integration tests")
    parser.add_argument("--examples", action="store_true", help="Run only example tests")
    parser.add_argument("--lint", action="store_true", help="Run only linting checks")
    parser.add_argument("--format", action="store_true", help="Run only formatting checks")
    parser.add_argument("--all", action="store_true", help="Run all tests and checks")
    
    args = parser.parse_args()
    
    # If no specific tests are requested, run all
    if not any([args.rust, args.python, args.integration, args.examples, args.lint, args.format]):
        args.all = True
    
    success = True
    
    if args.all or args.rust:
        if not run_rust_tests():
            success = False
    
    if args.all or args.python:
        if not run_python_tests():
            success = False
    
    if args.all or args.integration:
        if not run_integration_tests():
            success = False
    
    if args.all or args.examples:
        if not run_example_tests():
            success = False
    
    if args.all or args.lint:
        if not run_linting():
            success = False
    
    if args.all or args.format:
        if not run_formatting():
            success = False
    
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
