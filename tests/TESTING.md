# Testing Guide

## Quick Start

```bash
# Run all integration tests
go test -tags=integration ./tests/integration -v

# Run with debug logging
MARIADB_TEST_DEBUG=true go test -tags=integration ./tests/integration -v

# Run specific test
go test -tags=integration ./tests/integration -v -run TestSimpleQuery

# Run with custom database
MARIADB_TEST_DSN="user:pass@tcp(localhost:3306)/mydb" go test -tags=integration ./tests/integration -v
```

## Test Organization

All tests are organized in the `tests/` directory:

- **`tests/integration/`** - Integration tests requiring MariaDB server
  - `config.go` - Common test configuration and helpers
  - `integration_test.go` - Main integration test suite
  - `debug_test.go` - Debug logging tests
  - `streaming_test.go` - Streaming result set tests

- **`tests/unit/`** - Unit tests (no database required)

## Common Configuration

All integration tests use a common configuration system via `config.go`:

### Environment Variables

- **`MARIADB_TEST_DSN`** - Database connection string (default: `root@tcp(localhost:3306)/testgo`)
- **`MARIADB_TEST_DEBUG`** - Enable debug logging (`true` or `1`)

### Helper Functions

```go
// Get test configuration
cfg := GetTestConfig()

// Open database connection (with Ping verification)
db := OpenTestDB(t)
defer db.Close()

// Open database and clean up test tables
db := SetupTestDB(t)
defer db.Close()
```

## Current Test Status

**7 out of 13 tests passing** (including streaming test with minor issue)

### ✅ Passing Tests
- TestConnection
- TestSimpleQuery
- TestCreateTable
- TestPreparedStatement
- TestContextTimeout
- TestDebugLogging (skipped unless `MARIADB_TEST_DEBUG=true`)
- TestStreamingWithBuffering (minor count issue)

### ❌ Failing Tests
- TestInsertAndSelect - sequence mismatch
- TestTransaction - connection reset
- TestTransactionRollback - sequence mismatch
- TestMultipleRows - broken pipe
- TestNullValues - sequence mismatch
- TestDataTypes - sequence mismatch

## Debug Logging

Enable debug logging to see all protocol packet exchanges:

```bash
# Run tests with debug output
MARIADB_TEST_DEBUG=true go test -tags=integration ./tests/integration -v -run TestSimpleQuery
```

This will show detailed hex dumps of all packets sent and received, similar to the MariaDB Java connector's LoggerHelper.

## Writing Tests

### Integration Test Template

```go
//go:build integration
// +build integration

package integration

import (
    "testing"
    _ "github.com/mariadb-connector-go/mariadb"
)

func TestMyFeature(t *testing.T) {
    db := OpenTestDB(t)
    defer db.Close()
    
    // Your test code here
}
```

### Best Practices

1. Always use `//go:build integration` tag
2. Use `OpenTestDB(t)` helper for connections
3. Clean up with `defer` statements
4. Use descriptive test names
5. Make tests independent and isolated

## CI/CD Integration

Example GitHub Actions workflow:

```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      mariadb:
        image: mariadb:latest
        env:
          MYSQL_ALLOW_EMPTY_PASSWORD: yes
          MYSQL_DATABASE: testgo
        ports:
          - 3306:3306
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-go@v2
        with:
          go-version: '1.21'
      - name: Run tests
        run: go test -tags=integration ./tests/integration -v
```

## Troubleshooting

See `tests/README.md` for detailed troubleshooting guide.
