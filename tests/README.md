# Osprey Tests

This directory contains integration and end-to-end tests for Osprey.

## Structure

- `integration/` - Integration tests that test multiple components working together
  - Server startup/shutdown
  - Client-server communication
  - Persistence and recovery
  - Concurrent operations

## Running Tests

### Run all tests (unit + integration)
```bash
make test-all
```

### Run only unit tests
```bash
make test
```

### Run only integration tests
```bash
make test-integration
```

### Run tests with coverage
```bash
make test-coverage
```

## Writing Tests

- Unit tests should be placed alongside the source files (e.g., `store_test.go` next to `store.go`)
- Integration tests should be placed in the `tests/integration` directory
- End-to-end tests (if added) should go in `tests/e2e`
- Benchmark tests can go in `tests/benchmark`

## Test Utilities

Common test utilities and helpers can be added to `tests/testutil/` if needed.