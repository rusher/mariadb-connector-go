# MariaDB Go Connector

A production-ready MariaDB/MySQL database driver for Go implementing the `database/sql` interface.

[![License](https://img.shields.io/badge/License-LGPL%202.1-blue.svg)](LICENSE)

## Features

- **Full database/sql compatibility**: Implements Go's standard `database/sql/driver` interface
- **MariaDB & MySQL support**: Works with both MariaDB 10.x and MySQL 5.7+/8.0+
- **Multiple authentication methods**: 
  - mysql_native_password (SHA1-based)
  - caching_sha2_password (SHA256-based, MySQL 8.0 default)
  - ed25519 (MariaDB-specific)
  - GSSAPI/Kerberos
  - PAM authentication
- **Secure connections**: TLS/SSL support with client certificate authentication
- **Prepared statements**: Both text and binary protocol support
- **Transactions**: Full transaction support with savepoints
- **Connection pooling**: Leverages Go's built-in connection pooling
- **Context support**: Proper context handling for cancellation and timeouts

## Installation

```bash
go get github.com/mariadb-connector-go/mariadb
```

## Quick Start

```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    
    _ "github.com/mariadb-connector-go/mariadb"
)

func main() {
    // Open connection
    db, err := sql.Open("mariadb", "user:password@tcp(localhost:3306)/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Ping to verify connection
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }
    
    // Execute query
    rows, err := db.Query("SELECT id, name FROM users WHERE age > ?", 18)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Iterate results
    for rows.Next() {
        var id int
        var name string
        if err := rows.Scan(&id, &name); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("ID: %d, Name: %s\n", id, name)
    }
}
```

## Connection String Format

```
[username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
```

### Examples

```go
// TCP connection
"user:password@tcp(localhost:3306)/mydb"

// TCP with custom port
"user:password@tcp(127.0.0.1:3307)/mydb"

// Unix socket
"user:password@unix(/var/run/mysqld/mysqld.sock)/mydb"

// With TLS
"user:password@tcp(localhost:3306)/mydb?tls=true"

// With custom TLS config
"user:password@tcp(localhost:3306)/mydb?tls=custom&ca=/path/to/ca.pem"
```

### Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timeout` | duration | 10s | Connection timeout |
| `readTimeout` | duration | 0 | I/O read timeout |
| `writeTimeout` | duration | 0 | I/O write timeout |
| `tls` | bool/string | false | Enable TLS (true, false, skip-verify, preferred, custom) |
| `ca` | string | - | Path to CA certificate file |
| `cert` | string | - | Path to client certificate file |
| `key` | string | - | Path to client private key file |
| `charset` | string | utf8mb4 | Character set |
| `collation` | string | - | Collation |

## Usage Examples

### Prepared Statements

```go
stmt, err := db.Prepare("INSERT INTO users (name, email) VALUES (?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

result, err := stmt.Exec("John Doe", "john@example.com")
if err != nil {
    log.Fatal(err)
}

id, _ := result.LastInsertId()
fmt.Printf("Inserted user with ID: %d\n", id)
```

### Transactions

```go
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

_, err = tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = ?", 1)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

_, err = tx.Exec("UPDATE accounts SET balance = balance + 100 WHERE id = ?", 2)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

### Context Support

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "SELECT * FROM large_table")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()
```

## Development

### Building

```bash
go build ./...
```

### Running Tests

```bash
# Unit tests
go test ./...

# Integration tests (requires MariaDB/MySQL instance)
go test -tags=integration ./tests/integration

# Benchmarks
go test -bench=. ./tests/benchmark
```

### Testing with Docker

```bash
# Start MariaDB
docker run -d --name mariadb-test -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mariadb:latest

# Run tests
go test -tags=integration ./tests/integration

# Cleanup
docker stop mariadb-test && docker rm mariadb-test
```

## Architecture

The driver follows an idiomatic Go structure:

```
github.com/mariadb-connector-go/mariadb/
├── driver.go              # database/sql/driver implementation
├── connector.go           # Connector implementation
├── connection.go          # Connection implementation
├── statement.go           # Statement implementation
├── rows.go                # Rows implementation
├── transaction.go         # Transaction implementation
├── dsn.go                 # DSN parsing
├── errors.go              # Error types
├── internal/
│   ├── protocol/          # MySQL/MariaDB wire protocol
│   ├── auth/              # Authentication plugins
│   ├── codec/             # Data type encoding/decoding
│   └── buffer/            # Buffer management
├── examples/              # Usage examples
└── tests/                 # Test suites
```

## Compatibility

- **Go**: 1.19 or later
- **MariaDB**: 10.x
- **MySQL**: 5.7, 8.0+

## License

This library is distributed under the [LGPL-2.1-or-later](LICENSE) license.

## Support

- **Issues**: [GitHub Issues](https://github.com/mariadb-connector-go/mariadb/issues)
