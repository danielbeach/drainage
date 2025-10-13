# Drainage Tests

This directory contains comprehensive tests for the drainage library, including unit tests, integration tests, and example tests.

## Test Structure

```
tests/
├── __init__.py              # Test package initialization
├── conftest.py              # Pytest configuration and fixtures
├── test_drainage.py         # Main test suite for drainage module
└── README.md               # This file
```

## Running Tests

### Prerequisites

Make sure you have the following installed:

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Rust dependencies (if not already installed)
cargo install maturin
```

### Quick Start

```bash
# Run all tests
make test

# Or run tests directly
python -m pytest tests/ -v
```

### Individual Test Suites

```bash
# Run only Rust tests
make test-rust
# or
cargo test

# Run only Python tests
make test-python
# or
python -m pytest tests/ -v

# Run integration tests
make test-integration
# or
python -m pytest tests/ -m integration -v
```

### Test Categories

Tests are organized into several categories:

- **Unit Tests**: Test individual functions and methods
- **Integration Tests**: Test the complete workflow
- **Mock Tests**: Test with mocked dependencies
- **Example Tests**: Test the example scripts

### Test Markers

Tests are marked with pytest markers for easy filtering:

```bash
# Run only unit tests
python -m pytest tests/ -m unit -v

# Run only integration tests
python -m pytest tests/ -m integration -v

# Run only mock tests
python -m pytest tests/ -m mock -v

# Run only real service tests
python -m pytest tests/ -m real -v
```

## Test Configuration

### Pytest Configuration

The `pytest.ini` file contains the test configuration:

```ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
    --disable-warnings
    --color=yes
    --durations=10
```

### Fixtures

The `conftest.py` file provides common fixtures for testing:

- `drainage_module`: The drainage module
- `mock_health_report`: Mock health report for testing
- `mock_delta_lake_objects`: Mock Delta Lake objects
- `mock_iceberg_objects`: Mock Iceberg objects
- `valid_s3_paths`: Valid S3 paths for testing
- `invalid_s3_paths`: Invalid S3 paths for testing
- And many more...

## Test Coverage

To run tests with coverage:

```bash
# Run with coverage
make coverage
# or
python -m pytest tests/ --cov=drainage --cov-report=html

# View coverage report
open htmlcov/index.html
```

## Writing Tests

### Test Naming Convention

- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

### Example Test

```python
def test_analyze_delta_lake_parameters():
    """Test analyze_delta_lake function parameters."""
    with patch('drainage.analyze_delta_lake') as mock_analyze:
        mock_report = MagicMock()
        mock_analyze.return_value = mock_report
        
        result = drainage.analyze_delta_lake(
            s3_path="s3://test-bucket/test-table/",
            aws_region="us-west-2"
        )
        
        mock_analyze.assert_called_once_with(
            s3_path="s3://test-bucket/test-table/",
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_region="us-west-2"
        )
        assert result == mock_report
```

### Mocking

Use the provided fixtures for common mocks:

```python
def test_with_mock_report(mock_health_report):
    """Test with mock health report."""
    assert mock_health_report.table_path == "s3://test-bucket/test-table/"
    assert mock_health_report.health_score == 0.85
```

### Testing Async Functions

For testing async functions, use pytest-asyncio:

```python
@pytest.mark.asyncio
async def test_async_function():
    """Test async function."""
    result = await some_async_function()
    assert result is not None
```

## Continuous Integration

Tests are automatically run on:

- Push to main/develop branches
- Pull requests
- Manual workflow dispatch

The CI pipeline includes:

1. **Rust Tests**: Unit tests for Rust code
2. **Python Tests**: Unit tests for Python bindings
3. **Integration Tests**: End-to-end workflow tests
4. **Linting**: Code quality checks
5. **Security**: Security vulnerability scans
6. **Performance**: Performance benchmarks
7. **Documentation**: Documentation generation

## Debugging Tests

### Running Specific Tests

```bash
# Run specific test file
python -m pytest tests/test_drainage.py -v

# Run specific test function
python -m pytest tests/test_drainage.py::TestDrainageModule::test_analyze_delta_lake_parameters -v

# Run tests matching pattern
python -m pytest tests/ -k "delta_lake" -v
```

### Debug Mode

```bash
# Run with debug output
python -m pytest tests/ -v -s

# Run with pdb on failure
python -m pytest tests/ --pdb

# Run with pdb on first failure
python -m pytest tests/ --pdb -x
```

### Verbose Output

```bash
# Very verbose output
python -m pytest tests/ -vv

# Show local variables on failure
python -m pytest tests/ -l
```

## Test Data

Test data is provided through fixtures in `conftest.py`. For custom test data:

1. Create a fixture in `conftest.py`
2. Use the fixture in your test
3. Keep test data minimal and focused

## Best Practices

1. **Test Isolation**: Each test should be independent
2. **Mock External Dependencies**: Don't make real AWS calls in tests
3. **Clear Test Names**: Test names should describe what they test
4. **One Assertion Per Test**: Keep tests focused on one behavior
5. **Use Fixtures**: Reuse common test data through fixtures
6. **Test Edge Cases**: Include boundary conditions and error cases
7. **Documentation**: Add docstrings to test functions

## Troubleshooting

### Common Issues

1. **Import Errors**: Make sure the drainage module is built
2. **Missing Dependencies**: Install all requirements
3. **Permission Errors**: Check file permissions
4. **Timeout Errors**: Increase timeout for slow tests

### Getting Help

- Check the test output for error messages
- Use `-v` flag for verbose output
- Use `--pdb` for debugging
- Check the CI logs for detailed error information

## Contributing

When adding new tests:

1. Follow the naming convention
2. Add appropriate markers
3. Use existing fixtures when possible
4. Add docstrings
5. Test both success and failure cases
6. Update this README if needed
