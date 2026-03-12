# Tests

## Structure

```
tests/
├── unit/          Unit tests — no database required
├── integration/   Integration tests — require a live MariaDB/MySQL server
└── benchmark/     Driver comparison benchmarks (see benchmark/README.md)
```

## Unit tests

No external dependencies.

```bash
go test ./tests/unit/
```

## Integration tests

### Requirements

- MariaDB or MySQL server
- A database the user can create/drop tables in (default: `testgo`)

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `MARIADB_TEST_DSN` | `root@tcp(localhost:3306)/testgo` | Full DSN for the test connection |
| `MARIADB_TEST_DEBUG` | *(unset)* | Set to `true` or `1` to enable packet-level debug logging |

### Running

```bash
# All integration tests
go test -tags integration ./tests/integration/

# Verbose output
go test -tags integration -v ./tests/integration/

# Single test
go test -tags integration -v -run TestPreparedStatement ./tests/integration/

# Custom server
MARIADB_TEST_DSN="app:secret@tcp(192.168.1.10:3306)/ci" \
  go test -tags integration -v ./tests/integration/

# Debug packet traces
MARIADB_TEST_DEBUG=true go test -tags integration -v -run TestSimpleQuery ./tests/integration/
```

### Writing a new integration test

```go
//go:build integration

package integration

import (
    "testing"
    _ "github.com/mariadb-connector-go/mariadb"
)

func TestMyFeature(t *testing.T) {
    db := OpenTestDB(t)
    defer db.Close()

    // ...
}
```

Use `SetupTestDB(t)` instead of `OpenTestDB(t)` when the test needs a clean slate (drops common test tables before returning the connection).

## Starting a local MariaDB with Docker

```bash
docker run --rm -d \
  --name mariadb-test \
  -e MYSQL_ALLOW_EMPTY_PASSWORD=yes \
  -e MYSQL_DATABASE=testgo \
  -p 3306:3306 \
  mariadb:latest

# Wait for the server to be ready, then run all test suites
go test ./tests/unit/
go test -tags integration ./tests/integration/
go test -tags benchmark -bench=. -benchmem ./tests/benchmark/
```
